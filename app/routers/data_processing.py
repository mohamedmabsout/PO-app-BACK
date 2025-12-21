# in app/routers/data_processing.py
from typing import List
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import logging
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from sqlalchemy.orm import Session, query
from typing import Optional
from fastapi import Query, status
from ..dependencies import get_current_user, get_db
from .. import crud, models, auth, schemas
from datetime import datetime, date
from xlsxwriter.utility import xl_col_to_name
from ..utils.pdf_generator import generate_bc_pdf # Import the function
from fastapi.responses import FileResponse
from fastapi.responses import StreamingResponse # <-- Import this
    
from backend.app.utils import pdf_generator
 
router = APIRouter(prefix="/api/data", tags=["data_processing"])
logger = logging.getLogger(__name__)


@router.post("/import/purchase-orders")
async def import_purchase_orders(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file type.")

    history_record = crud.create_upload_history_record(
        db=db, filename=file.filename, status="PROCESSING", user_id=current_user.id
    )

    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))

        # --- FIX 1: Delegate ALL logic to a new, smart CRUD function ---
        num_raw_records = crud.create_raw_purchase_orders_from_dataframe(
            db=db, df=df, user_id=current_user.id
        )

        # --- FIX 2: Immediately trigger the merge process ---
        num_merged_records = crud.process_and_merge_pos(db=db)

        # Update history on success
        history_record.status = "SUCCESS"
        history_record.total_rows = num_raw_records
        db.commit()

        return {
            "message": f"{num_raw_records} POs saved and {num_merged_records} records merged.",
            "raw_records_saved": num_raw_records,
            "merged_records_processed": num_merged_records,
        }

    except Exception as e:
        db.rollback()
        history_record.status = "FAILURE"
        history_record.error_message = str(e)
        db.commit()
        logger.error(f"Error during PO import: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/import/history", response_model=List[schemas.UploadHistory])
def read_upload_history(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    return crud.get_upload_history(db=db)


@router.get("/staging/purchase-orders", response_model=List[schemas.RawPurchaseOrder])
def get_staging_purchase_orders(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    """
    Retrieves all records from the purchase_orders staging table.
    """
    # We can add pagination later if needed (skip, limit)
    return (
        db.query(models.RawPurchaseOrder)
        .order_by(models.RawPurchaseOrder.id.desc())
        .all()
    )


@router.get(
    "/merged-pos", response_model=schemas.PaginatedMergedPO
)  # Use a paginated schema
def get_merged_pos_preview(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
    # Add filter parameters as Query dependencies
    internal_project_id: Optional[int] = Query(None),
    customer_project_id: Optional[int] = Query(None),
    site_code: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None, description="Format: YYYY-MM-DD"),
    end_date: Optional[date] = Query(None, description="Format: YYYY-MM-DD"),
    search: Optional[str] = Query(None),
    # Add pagination parameters
    page: int = Query(1, gt=0),
    per_page: int = Query(20, gt=0),
):
    """
    Retrieves a paginated and filtered list of records from the MergedPO table.
    """
    # 1. Get the base filtered query from our new CRUD function
    query = crud.get_filtered_merged_pos(
        db,
        internal_project_id=internal_project_id,
        customer_project_id=customer_project_id,
        site_code=site_code,
        start_date=start_date,
        end_date=end_date,
        search=search,
    )

    # 2. Get the total count of items that match the filters (for pagination)
    total_items = query.count()

    # 3. Apply pagination to the query
    items = query.offset((page - 1) * per_page).limit(per_page).all()

    # 4. Return the data in a structured pagination format
    return {
        "items": items,
        "total_items": total_items,
        "page": page,
        "per_page": per_page,
        "total_pages": (total_items + per_page - 1) // per_page,
    }

@router.get("/export-merged-pos", status_code=status.HTTP_200_OK)
def export_merged_pos_report(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
    internal_project_id: Optional[int] = Query(None),
    customer_project_id: Optional[int] = Query(None),
    site_code: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None, description="Format: YYYY-MM-DD"),
    end_date: Optional[date] = Query(None, description="Format: YYYY-MM-DD"),
    search: Optional[str] = Query(None),
):
    """
    Generates an Excel report with colored headers and robust data highlighting.
    """
    try:
        # 1. Fetch Data
        export_df = crud.get_export_dataframe(
            db,
            internal_project_id=internal_project_id,
            customer_project_id=customer_project_id,
            site_code=site_code,
            start_date=start_date,
            end_date=end_date,
            search=search,
        )

        if export_df.empty:
            raise HTTPException(
                status_code=404, detail="No data found for the selected filters."
            )
        all_columns_in_order = [
            # Your newly requested columns first
            "PO ID","Internal Project", "PM", "Unit Price", "Requested Qty", "Internal Check", "Payment Term",
            # The rest of the original columns
             "Customer Project", "Site Code", "PO No.", 
            "PO Line No.", "Item Description", "Category", "Publish Date", 
            "Line Amount",
            # AC/PAC and Remaining columns
            "Total AC (80%)", "Accepted AC Amount", "Date AC OK",
            "Total PAC (20%)", "Accepted PAC Amount", "Date PAC OK",
            "Remaining Amount"
        ]
        
        # Reorder the dataframe to match the desired output
        export_df = export_df[all_columns_in_order]

        # 2. Format Dates
        date_columns = ["Publish Date", "Date AC OK", "Date PAC OK"]
        for col in date_columns:
            if col in export_df.columns:
                # Convert to datetime, then format. Fill NaT/None with an empty string.
                export_df[col] = pd.to_datetime(export_df[col], errors='coerce').dt.strftime("%Y-%m-%d").fillna("")
        
        # 3. Setup Excel Writer
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            export_df.to_excel(writer, sheet_name="Export Data", startrow=1, header=False, index=False)

            workbook = writer.book
            worksheet = writer.sheets["Export Data"]
            
            # --- DEFINING FORMATS (No change) ---
            # ... (fmt_header_std, fmt_header_ac, etc. are the same)
            header_base = {'bold': True, 'text_wrap': True, 'valign': 'vcenter', 'align': 'center', 'border': 1}
            fmt_header_std = workbook.add_format({**header_base, 'fg_color': '#EEEEEE'})
            fmt_header_ac = workbook.add_format({**header_base, 'fg_color': '#93c47d'})
            fmt_header_pac = workbook.add_format({**header_base, 'fg_color': '#6d9eeb'})
            fmt_header_red = workbook.add_format({**header_base, 'fg_color': '#e06666'})
            fmt_bg_ac = workbook.add_format({"bg_color": "#D9EAD3"})
            fmt_bg_pac = workbook.add_format({"bg_color": "#CFE2F3"})
            fmt_bg_red = workbook.add_format({"bg_color": "#F4CCCC"})

            headers = export_df.columns.tolist()

            # --- APPLY COLUMN WIDTHS AND FORMATTING ---
            for col_idx, col_name in enumerate(headers):
                # Set a default width
                col_width = 20
                
                # --- FIX 2: APPLY FULL-COLUMN BACKGROUND COLORS ---
                if "AC" in col_name and "PAC" not in col_name:
                    worksheet.set_column(col_idx, col_idx, col_width, fmt_bg_ac)
                elif "PAC" in col_name:
                    worksheet.set_column(col_idx, col_idx, col_width, fmt_bg_pac)
                elif "Remaining Amount" in col_name:
                    worksheet.set_column(col_idx, col_idx, col_width, fmt_bg_red)
                else:
                    worksheet.set_column(col_idx, col_idx, col_width)

            # --- WRITE HEADERS ON TOP ---
            for col_idx, col_name in enumerate(headers):
                if "AC" in col_name and "PAC" not in col_name:
                    style = fmt_header_ac
                elif "PAC" in col_name:
                    style = fmt_header_pac
                elif "Remaining Amount" in col_name:
                    style = fmt_header_red
                else:
                    style = fmt_header_std
                
                worksheet.write(0, col_idx, col_name, style)

        # ... (StreamingResponse part is the same) ...
        output.seek(0)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        filename = f"PO_Export_{timestamp}.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.sheet",
            headers=headers,
        )

    except Exception as e:
        print(f"Error during export: {e}")
        raise HTTPException(status_code=500, detail="Could not generate the Excel report.")


@router.get("/remaining-to-accept")
def get_remaining_pos(
    page: int = 1,
    size: int = 20,
    filter_stage: str = "ALL",
    search: Optional[str] = None,
    internal_project_id: Optional[int] = None,
    customer_project_id: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    data = crud.get_remaining_to_accept_paginated(
                db, page, size, filter_stage, 
                search, internal_project_id, customer_project_id
    )
    # Note: Stats are usually global, calculating them with filters might be expensive 
    # but let's keep the global stats for the cards at the top
    stats = crud.get_remaining_stats(db) 
    
    return {
        "data": data,
        "stats": stats
    }
@router.get("/bc/export", status_code=status.HTTP_200_OK)
def export_bcs(
    search: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    try:
        # 1. Get DataFrame
        df = crud.get_bcs_export_dataframe(db, search)
        
        if df.empty:
             raise HTTPException(status_code=404, detail="No data found to export.")

        # 2. Format Dates
        for col in df.select_dtypes(include=['datetime64']).columns:
            df[col] = pd.to_datetime(df[col]).dt.strftime('%d/%m/%Y %H:%M').fillna('')

        # 3. Create Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='BC Details', index=False)
            
            # Optional: Add simple coloring or formatting here if desired
            workbook = writer.book
            worksheet = writer.sheets['BC Details']
            header_format = workbook.add_format({'bold': True, 'bg_color': '#f0f0f0', 'border': 1})
            
            # Apply header format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                # Auto-adjust column width (approximate)
                worksheet.set_column(col_num, col_num, 20)

        output.seek(0)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        headers = {'Content-Disposition': f'attachment; filename="BC_Export_{timestamp}.xlsx"'}
        return StreamingResponse(output, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers=headers)

    except Exception as e:
        print(f"Export Error: {e}")
        raise HTTPException(status_code=500, detail="Export failed")

@router.get("/bc-candidates", response_model=List[schemas.MergedPO]) # Use MergedPO schema
def get_bc_candidates(
    project_id: int,
    site_codes: Optional[str] = Query(None, description="Comma separated codes"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: Session = Depends(get_db)
):
    """
    Returns list of POs eligible for BC creation based on filters.
    """
    code_list = []
    if site_codes:
        # Split by newline (Excel paste) or comma
        code_list = site_codes.replace('\n', ',').split(',')
        
    return crud.get_eligible_pos_for_bc(
        db, project_id, code_list, start_date, end_date
    )
@router.post("/create-bc", response_model=schemas.BCResponse)
def generate_bc(
    bc_data: schemas.BCCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    try:
        return crud.create_bon_de_commande(db, bc_data, current_user.id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Failed to generate BC")
@router.get("/bc/list/{status}", response_model=List[schemas.BCResponse])
def list_bcs(status: str, db: Session = Depends(get_db)):
    # Map string to Enum
    status_enum = models.BCStatus(status) 
    return crud.get_bcs_by_status(db, status_enum)
@router.get("/bc/all", response_model=List[schemas.BCResponse]) # Use your schema
def read_all_bcs(
    search: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    return crud.get_all_bcs(db, current_user, search=search)
@router.post("/bc/{bc_id}/approve-l1")
def approve_l1(bc_id: int, db: Session = Depends(get_db), current_user: models.User = Depends(auth.get_current_user)):
    # Check if PD
    bc = crud.approve_bc_l1(db, bc_id, current_user.id)
    
    # FIND ADMINS
    admins = db.query(models.User).filter(models.User.role == "Admin").all()
    
    for admin in admins:
        crud.create_notification(
            db, 
            recipient_id=admin.id,
            type=models.NotificationType.TODO,
            title="Final Approval Required",
            message=f"BC {bc.bc_number} validated L1. Pending final approval.",
            link=f"/configuration/bc/detail/{bc.id}"
        )
    db.commit()
    return bc

@router.post("/bc/{bc_id}/approve-l2")
def approve_l2(bc_id: int, db: Session = Depends(get_db), current_user: models.User = Depends(auth.get_current_user)):
    # Check if Admin
    bc = crud.approve_bc_l2(db, bc_id, current_user.id)
    
    # NOTIFY CREATOR
    crud.create_notification(
        db, 
        recipient_id=bc.creator_id,
        type=models.NotificationType.APP,
        title="BC Approved",
        message=f"Your BC {bc.bc_number} has been fully approved.",
        link=f"/configuration/bc/detail/{bc.id}"
    )
    db.commit()
    return bc

@router.get("/bc/{bc_id}/pdf")
def get_bc_pdf(
    bc_id: int, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_active_user)
):
    bc = crud.get_bc_by_id(db, bc_id)
    if not bc:
        raise HTTPException(status_code=404, detail="Bon de Commande not found")
    
    # 1. Generate PDF into memory buffer
    pdf_buffer = pdf_generator.generate_bc_pdf(bc)
    
    # 2. Return as a stream
    filename = f"BC_{bc.bc_number}.pdf"
    
    return StreamingResponse(
        pdf_buffer, 
        media_type="application/pdf", 
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

@router.post("/import/assign-projects-only")
async def assign_projects_only(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    # if current_user.role != auth.UserRole.ADMIN:
    #     raise HTTPException(status_code=403, detail="Admin only")

    try:
        contents = await file.read()
        stats = crud.bulk_assign_projects_only(db, contents)
        return {"message": "Project assignment complete", "stats": stats}
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/bc/{bc_id}/reject")
def reject_bon_de_commande(
    bc_id: int, 
    rejection_data: schemas.BCRejectionRequest,
    db: Session = Depends(get_db), 
    current_user: models.User = Depends(auth.get_current_user)
):
    return crud.reject_bc(
        db, bc_id=bc_id, reason=rejection_data.reason, rejector_id=current_user.id
    )

@router.post("/bc/{bc_id}/submit")
def submit_bon_de_commande(
    bc_id: int, 
    db: Session = Depends(get_db), 
    current_user: models.User = Depends(get_current_user)
):
    bc = crud.submit_bc(db, bc_id)
    
    # FIND PROJECT DIRECTORS
    pds = db.query(models.User).filter(models.User.role == "PD").all() # Or "Project Director"
    
    for pd in pds:
        crud.create_notification(
            db, 
            recipient_id=pd.id,
            type=models.NotificationType.TODO,
            title="Approval Required",
            message=f"BC {bc.bc_number} submitted by {current_user.first_name} requires L1 validation.",
            link=f"/configuration/bc/detail/{bc.id}"
        )
    db.commit()
    return bc

@router.get("/bc/{bc_id}", response_model=schemas.BCResponse)
def get_bc_details(
    bc_id: int,
    db: Session = Depends(get_db)
):
    bc = crud.get_bc_by_id(db, bc_id)
    if not bc:
        raise HTTPException(status_code=404, detail="BC not found")
    return bc

