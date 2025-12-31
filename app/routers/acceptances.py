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
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """
    Validates multiple items at once using the existing logic.
    """
    success_count = 0
    errors = []

    for item_id in payload.item_ids:
        try:
            # Reuse your existing, correct logic
            crud.validate_bc_item(db, item_id, current_user, payload.action, payload.comment)
            success_count += 1
        except ValueError as e:
            errors.append(f"Item {item_id}: {str(e)}")
        except Exception as e:
            errors.append(f"Item {item_id}: Unexpected error")

    if not success_count and errors:
         # If everything failed, return error
         raise HTTPException(status_code=400, detail=errors[0])
    
    return {
        "message": f"Successfully processed {success_count} items.",
        "errors": errors
    }
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
