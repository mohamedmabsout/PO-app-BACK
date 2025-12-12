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

        # 2. Format Dates
        for col in export_df.select_dtypes(include=["datetime64"]).columns:
            export_df[col] = (
                pd.to_datetime(export_df[col]).dt.strftime("%d/%m/%Y").fillna("")
            )

        # 3. Setup Excel Writer
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            export_df.to_excel(writer, sheet_name="Merged PO Data", startrow=1, header=False, index=False)

            workbook = writer.book
            worksheet = writer.sheets["Merged PO Data"]
            (max_row, max_col) = export_df.shape

            # --- DEFINING FORMATS ---
            header_base = {
                'bold': True, 'text_wrap': True, 'valign': 'vcenter', 'align': 'center', 'border': 1
            }
            
            fmt_header_std = workbook.add_format({**header_base, 'fg_color': '#EEEEEE'}) # Gray
            fmt_header_ac = workbook.add_format({**header_base, 'fg_color': '#93c47d'})  # Green
            fmt_header_pac = workbook.add_format({**header_base, 'fg_color': '#6d9eeb'}) # Blue
            
            # NEW: Red Header for Remaining
            fmt_header_red = workbook.add_format({**header_base, 'fg_color': '#e06666'}) # Darker Red for Header

            # Data Formats
            fmt_data_ac = workbook.add_format({"bg_color": "#D9EAD3"})  # Light Green
            fmt_data_pac = workbook.add_format({"bg_color": "#CFE2F3"})  # Light Blue
            
            # NEW: Red Data Background for Remaining
            fmt_data_red = workbook.add_format({"bg_color": "#F4CCCC"}) # Light Red

            # --- WRITING HEADERS MANUALLY ---
            headers = export_df.columns.tolist()
            
            for col_idx, column_name in enumerate(headers):
                # 1. Determine Header Style
                if "Remaining" in column_name:
                    style = fmt_header_red
                elif "AC" in column_name and "PAC" not in column_name:
                    style = fmt_header_ac
                elif "PAC" in column_name:
                    style = fmt_header_pac
                else:
                    style = fmt_header_std
                
                # 2. Write Header
                worksheet.write(0, col_idx, column_name, style)
                worksheet.set_column(col_idx, col_idx, 20) 

            # --- CONDITIONAL FORMATTING (DATA) ---
            try:
                # 1. AC Formatting
                ac_amt_idx = headers.index("Accepted AC Amount")
                ac_date_idx = headers.index("Date AC OK")
                
                for idx in [ac_amt_idx, ac_date_idx]:
                    col_letter = xl_col_to_name(idx)
                    worksheet.conditional_format(f"{col_letter}2:{col_letter}{max_row + 1}", {
                        'type': 'cell', 'criteria': '!=', 'value': '""', 'format': fmt_data_ac
                    })

                # 2. PAC Formatting
                pac_amt_idx = headers.index("Accepted PAC Amount")
                pac_date_idx = headers.index("Date PAC OK")
                
                for idx in [pac_amt_idx, pac_date_idx]:
                    col_letter = xl_col_to_name(idx)
                    worksheet.conditional_format(f"{col_letter}2:{col_letter}{max_row + 1}", {
                        'type': 'cell', 'criteria': '!=', 'value': '""', 'format': fmt_data_pac
                    })

                # 3. NEW: Remaining Amount Formatting (Light Red)
                # We apply this to the "Remaining Amount" column
                rem_idx = headers.index("Remaining Amount")
                col_letter = xl_col_to_name(rem_idx)
                
                # Option A: Always color it light red to highlight it's a debt/gap
                # Option B: Only color if > 0. Let's do > 0 (meaning there is still work to do)
                worksheet.conditional_format(f"{col_letter}2:{col_letter}{max_row + 1}", {
                     'type': 'cell', 
                     'criteria': '>', 
                     'value': 0, 
                     'format': fmt_data_red
                })

            except ValueError:
                print("Warning: Could not find specific AC/PAC columns for formatting.")

        output.seek(0)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        filename = f"Merged_PO_Report_{timestamp}.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        print(f"Error during export: {e}")
        raise HTTPException(
            status_code=500, detail="Could not generate the Excel report."
        )
# backend/app/routers/data_processing.py

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
    return crud.approve_bc_l1(db, bc_id, current_user.id)

@router.post("/bc/{bc_id}/approve-l2")
def approve_l2(bc_id: int, db: Session = Depends(get_db), current_user: models.User = Depends(auth.get_current_user)):
    # Check if Admin
    return crud.approve_bc_l2(db, bc_id, current_user.id)

@router.get("/bc/{bc_id}/pdf")
def download_bc_pdf(bc_id: int, db: Session = Depends(get_db)):
    bc = db.query(models.BonDeCommande).get(bc_id)
    if not bc or bc.status != models.BCStatus.APPROVED:
        raise HTTPException(status_code=400, detail="BC not approved yet")
    
    pdf_path = generate_bc_pdf(bc) # Returns path to generated file
    return FileResponse(pdf_path, filename=f"{bc.bc_number}.pdf", media_type='application/pdf')
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
    return crud.submit_bc(db, bc_id=bc_id)
@router.get("/bc/{bc_id}", response_model=schemas.BCResponse)
def get_bc_details(
    bc_id: int,
    db: Session = Depends(get_db)
):
    bc = crud.get_bc_by_id(db, bc_id)
    if not bc:
        raise HTTPException(status_code=404, detail="BC not found")
    return bc
@router.post("/bc/{bc_id}/submit")
def submit_bc_endpoint(bc_id: int, db: Session = Depends(get_db), current_user: models.User = Depends(auth.get_current_user)):
    # Optional: Check if current_user is the creator before allowing submit
    return crud.submit_bc(db, bc_id)
