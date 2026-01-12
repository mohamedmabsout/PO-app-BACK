# backend/app/routers/acceptances.py

from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, status
from fastapi.responses import StreamingResponse,FileResponse
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import or_
import pandas as pd
import io
import os
import shutil
from typing import List, Optional
from ..dependencies import get_db
from ..schemas import BulkValidationPayload,GenerateACTPayload,ServiceAcceptance
from .. import crud
from ..auth import get_current_user  # Import your authentication dependency
from .. import models  # To specify the user model type
from fastapi import BackgroundTasks # Import this
from ..utils.pdf_generator import generate_act_pdf
from datetime import datetime
router = APIRouter(
    prefix="/api/acceptances",
    tags=["Acceptances"],
    # This ensures all routes in this file require an authenticated user
    dependencies=[Depends(get_current_user)],
)

@router.get("/acts/all", response_model=List[ServiceAcceptance]) 
def list_all_acceptances(
    search: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """
    Returns a list of all generated Acceptance Certificates (ACTs),
    ordered by newest first. Supports search.
    """
    query = db.query(models.ServiceAcceptance).options(
        joinedload(models.ServiceAcceptance.bc),
        joinedload(models.ServiceAcceptance.creator),
        joinedload(models.ServiceAcceptance.items) # To count items
    )

    if search:
        search_term = f"%{search}%"
        # Search by ACT Number OR BC Number
        query = query.join(models.BonDeCommande).filter(
            or_(
                models.ServiceAcceptance.act_number.ilike(search_term),
                models.BonDeCommande.bc_number.ilike(search_term)
            )
        )

    return query.order_by(models.ServiceAcceptance.created_at.desc()).all()

@router.post("/upload", status_code=status.HTTP_200_OK)
def upload_and_process_acceptances(
    background_tasks: BackgroundTasks,  # <-- Add this parameter
    file: UploadFile = File(..., description="The Acceptance Excel file"),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file type. Please upload an Excel file.",
        )

    # 1. Create History Record (PROCESSING)
    history_record = crud.create_upload_history_record(
        db=db,
        filename=file.filename,
        status="PROCESSING",
        user_id=current_user.id,
    )

    try:
        # 2. Save file to disk
        temp_dir = "temp_uploads"
        os.makedirs(temp_dir, exist_ok=True)
        temp_file_path = f"{temp_dir}/{history_record.id}_{file.filename}"

        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # 3. Dispatch Background Task
        # Pass the file path, history ID, and user ID to the worker
        background_tasks.add_task(
            crud.process_acceptance_file_background,
            temp_file_path,
            history_record.id,
            current_user.id,
        )

        # 4. Return Immediate Success
        return {
            "message": "Acceptance file uploaded. Processing started in background.",
            "filename": file.filename,
            "history_id": history_record.id,
        }

    except Exception as e:
        # If saving to disk fails before dispatching, update history to failed immediately
        history_record.status = "FAILED"
        history_record.error_message = f"Upload failed: {str(e)}"
        db.commit()
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")

@router.get("/bc/{id}/acts", status_code=status.HTTP_200_OK)
def get_acts_for_bc(
    id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    return db.query(models.ServiceAcceptance).filter(models.ServiceAcceptance.bc_id == id).all()

# routers/acceptance.py

@router.get("/bc/{bc_id}/status-report")
def download_bc_acceptance_status(
    bc_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """
    Generates an Excel report showing the QC/PM validation status for all items in a BC.
    """
    # Fetch BC with items
    bc = crud.get_bc_by_id(db, bc_id)
    if not bc:
        raise HTTPException(status_code=404, detail="BC not found")
        
    # Generate DataFrame
    data = []
    for item in bc.items:
        row = {
            "PO Ref": item.merged_po.po_no,
            "Item Description": item.merged_po.item_description,
            "Qty": item.quantity_sbc,
            "QC Status": item.qc_validation_status,
            "PM Status": item.pm_validation_status,
            "Global Status": item.global_status,
            "Rejections": item.rejection_count,
            "Postponed Until": item.postponed_until.strftime('%Y-%m-%d') if item.postponed_until else ""
        }
        data.append(row)
        
    df = pd.DataFrame(data)
    
    # Export to Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Acceptance Status')
    
    output.seek(0)
    filename = f"Acceptance_Status_{bc.bc_number}.xlsx"
    headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
    
    return StreamingResponse(output, headers=headers)

@router.post("/validate-items")
def validate_items_bulk(
    payload: BulkValidationPayload,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    success_count = 0
    errors = []

    for item_id in payload.item_ids:
        try:
            crud.validate_bc_item(
                db=db,
                item_id=item_id,
                current_user=current_user,
                action=payload.action,
                comment=payload.comment,
                background_tasks=background_tasks,   # ✅
            )
            success_count += 1
        except ValueError as e:
            errors.append(f"Item {item_id}: {str(e)}")
        except Exception:
            errors.append(f"Item {item_id}: Unexpected error")

    if not success_count and errors:
        raise HTTPException(status_code=400, detail=errors[0])

    return {"message": f"Successfully processed {success_count} items.", "errors": errors}

@router.post("/bc/{bc_id}/generate-act")
def generate_act_endpoint(
    bc_id: int,
    payload: GenerateACTPayload, # { item_ids: [1, 2, 3] }
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    # Check if user is PD
    try:
        act = crud.generate_act_record(db, bc_id, current_user.id, payload.item_ids)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal Server Error")

    # 2. Generate the PDF (We need to build this utility)
    # We pass the full ACT object (which has relationships to items, BC, etc.)
    pdf_buffer = generate_act_pdf(act) 

    # 3. Return the file
    filename = f"{act.act_number}.pdf"
    headers = {
        'Content-Disposition': f'attachment; filename="{filename}"'
    }
    
    return StreamingResponse(pdf_buffer, media_type='application/pdf', headers=headers)
@router.get("/bc/{bc_id}/acts", response_model=List[ServiceAcceptance])
def list_acts(bc_id: int, db: Session = Depends(get_db)):
    return db.query(models.ServiceAcceptance).filter(models.ServiceAcceptance.bc_id == bc_id).all()

# 2. Download Endpoint
@router.get("/act/{act_id}/download")
def download_act_pdf(
    act_id: int, 
    db: Session = Depends(get_db)
):
    # 1. Fetch the Data
    act = db.query(models.ServiceAcceptance).options(
        joinedload(models.ServiceAcceptance.items).joinedload(models.BCItem.merged_po),
        joinedload(models.ServiceAcceptance.bc),
        joinedload(models.ServiceAcceptance.creator)
    ).filter(models.ServiceAcceptance.id == act_id).first()
    
    if not act:
        raise HTTPException(status_code=404, detail="ACT record not found")

    # 2. Generate PDF in Memory (Stateless)
    try:
        pdf_buffer = generate_act_pdf(act)
    except Exception as e:
        print(f"PDF Gen Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate PDF")

    # 3. Stream Response
    filename = f"{act.act_number}.pdf"
    headers = {
        'Content-Disposition': f'attachment; filename="{filename}"'
    }
    
    return StreamingResponse(
        pdf_buffer, 
        media_type='application/pdf', 
        headers=headers
    )

# Generation Endpoint
@router.get("/acts/export-list", status_code=status.HTTP_200_OK)
def export_acts_list(
    search: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    # Reuse the list logic to respect filters
    query = db.query(models.ServiceAcceptance).options(
        joinedload(models.ServiceAcceptance.bc).joinedload(models.BonDeCommande.sbc),
        joinedload(models.ServiceAcceptance.bc).joinedload(models.BonDeCommande.internal_project),
        joinedload(models.ServiceAcceptance.creator),
        joinedload(models.ServiceAcceptance.items).joinedload(models.BCItem.merged_po)
    )

    if search:
        search_term = f"%{search}%"
        query = query.join(models.BonDeCommande).filter(
            or_(
                models.ServiceAcceptance.act_number.ilike(search_term),
                models.BonDeCommande.bc_number.ilike(search_term)
            )
        )
    
    acts = query.order_by(models.ServiceAcceptance.created_at.desc()).all()

    # Flatten Data
    data = []
    for act in acts:
        header_info = {
            "ACT Number": act.act_number,
            "Date Generated": act.created_at.strftime('%d/%m/%Y'),
            "Generated By": f"{act.creator.first_name} {act.creator.last_name}" if act.creator else "",
            "BC Number": act.bc.bc_number,
            "Project": act.bc.internal_project.name if act.bc.internal_project else "",
            "Sub-Contractor": act.bc.sbc.short_name if act.bc.sbc else "",
            "ACT Total HT": act.total_amount_ht,
            "ACT Total TTC": act.total_amount_ttc
        }

        # One row per item in the ACT
        for item in act.items:
            row = header_info.copy()
            row.update({
                "PO Ref": item.merged_po.po_no,
                "Site Code": item.merged_po.site_code,
                "Description": item.merged_po.item_description,
                "Category": item.merged_po.category,
                "Quantity": item.quantity_sbc,
                "Unit Price": item.unit_price_sbc,
                "Line Amount": item.line_amount_sbc
            })
            data.append(row)

    df = pd.DataFrame(data)
    
    # Generate Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='All Acceptances', index=False)
    
    output.seek(0)
    filename = f"Acceptances_List_{datetime.now().strftime('%Y%m%d')}.xlsx"
    headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
    return StreamingResponse(output, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers=headers)


# --- 2. EXPORT SINGLE ACT DETAILS (2 Sheets) ---
@router.get("/act/{act_id}/export-details", status_code=status.HTTP_200_OK)
def export_act_details(
    act_id: int,
    db: Session = Depends(get_db)
):
    act = db.query(models.ServiceAcceptance).options(
        joinedload(models.ServiceAcceptance.items).joinedload(models.BCItem.merged_po),
        joinedload(models.ServiceAcceptance.items).joinedload(models.BCItem.rejection_history).joinedload(models.ItemRejectionHistory.rejected_by)
    ).filter(models.ServiceAcceptance.id == act_id).first()

    if not act:
        raise HTTPException(status_code=404, detail="ACT not found")

    # --- Sheet 1: Acceptance Details ---
    accepted_data = []
    for item in act.items:
        accepted_data.append({
            "PO Ref": item.merged_po.po_no,
            "Site Code": item.merged_po.site_code,
            "Description": item.merged_po.item_description,
            "Quantity": item.quantity_sbc,
            "Unit Price": item.unit_price_sbc,
            "Total Amount": item.line_amount_sbc,
            "Tax Rate": item.applied_tax_rate
        })
    df_accepted = pd.DataFrame(accepted_data)

    # --- Sheet 2: Rejection History ---
    rejection_data = []
    for item in act.items:
        for history in item.rejection_history:
            rejection_data.append({
                "PO Ref": item.merged_po.po_no,
                "Item Description": item.merged_po.item_description,
                "Rejected By": f"{history.rejected_by.first_name} {history.rejected_by.last_name}" if history.rejected_by else "Unknown",
                "Rejection Date": history.rejected_at.strftime('%d/%m/%Y %H:%M'),
                "Reason / Comment": history.comment
            })
    df_rejected = pd.DataFrame(rejection_data)

    # Generate Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_accepted.to_excel(writer, sheet_name='Accepted Items', index=False)
        
        if not df_rejected.empty:
            df_rejected.to_excel(writer, sheet_name='Rejection History', index=False)
        else:
            # Create an empty sheet with a note if no rejections
            pd.DataFrame({'Note': ['No rejections for these items']}).to_excel(writer, sheet_name='Rejection History', index=False)

    output.seek(0)
    filename = f"ACT_Details_{act.act_number}.xlsx"
    headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
    return StreamingResponse(output, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers=headers)
