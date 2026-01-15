# in app/routers/data_processing.py
from typing import List
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import logging
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from sqlalchemy.orm import Session, query, joinedload
from sqlalchemy import or_
from typing import Optional
from fastapi import Query, status

from app import database
from app.core.security import is_admin, is_pd, is_pd_or_admin, is_pm
from ..dependencies import get_current_user, get_db
from .. import crud, models, auth, schemas
from datetime import datetime, date
from xlsxwriter.utility import xl_col_to_name
from ..utils.pdf_generator import generate_bc_pdf  # Import the function
from fastapi.responses import FileResponse
from fastapi.responses import StreamingResponse  # <-- Import this
from fastapi import BackgroundTasks # Import this
import shutil
import os
from ..utils import pdf_generator
import traceback

router = APIRouter(prefix="/api/data", tags=["data_processing"])
logger = logging.getLogger(__name__)


@router.post("/import/purchase-orders")
async def import_purchase_orders(
    background_tasks: BackgroundTasks,  # <-- Add this parameter
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file type.")

    # 1. Create the History Record immediately as "PROCESSING"
    history_record = crud.create_upload_history_record(
        db=db,
        filename=file.filename,
        status="PROCESSING",  # <-- Shows spinner in frontend
        user_id=current_user.id,
    )

    # 2. Save the file to a temporary location on the server
    # We can't pass the 'file' object to background task because it closes
    temp_dir = "temp_uploads"
    os.makedirs(temp_dir, exist_ok=True)
    temp_file_path = f"{temp_dir}/{history_record.id}_{file.filename}"

    with open(temp_file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # 3. Add the task to the background queue
    background_tasks.add_task(
        crud.process_po_file_background,
        temp_file_path,
        history_record.id,
        current_user.id,
    )

    # 4. Respond IMMEDIATELY
    return {
        "message": "File uploaded. Processing started in background.",
        "history_id": history_record.id,
    }


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
            "PO ID",
            "Internal Project",
            "PM",
            "Unit Price",
            "Requested Qty",
            "Internal Check",
            "Payment Term",
            # The rest of the original columns
            "Customer Project",
            "Site Code",
            "PO No.",
            "PO Line No.",
            "Item Description",
            "Category",
            "Publish Date",
            "Line Amount",
            # AC/PAC and Remaining columns
            "Total AC (80%)",
            "Accepted AC Amount",
            "Date AC OK",
            "Total PAC (20%)",
            "Accepted PAC Amount",
            "Date PAC OK",
            "Remaining Amount",
            "Real Backlog"
        ]

        # Reorder the dataframe to match the desired output
        export_df = export_df[all_columns_in_order]

        # 2. Format Dates
        date_columns = ["Publish Date", "Date AC OK", "Date PAC OK"]
        for col in date_columns:
            if col in export_df.columns:
                # Convert to datetime, then format. Fill NaT/None with an empty string.
                export_df[col] = (
                    pd.to_datetime(export_df[col], errors="coerce")
                    .dt.strftime("%Y-%m-%d")
                    .fillna("")
                )

        # 3. Setup Excel Writer
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            export_df.to_excel(
                writer, sheet_name="Export Data", startrow=1, header=False, index=False
            )

            workbook = writer.book
            worksheet = writer.sheets["Export Data"]

            # --- DEFINING FORMATS (No change) ---
            # ... (fmt_header_std, fmt_header_ac, etc. are the same)
            header_base = {
                "bold": True,
                "text_wrap": True,
                "valign": "vcenter",
                "align": "center",
                "border": 1,
            }
            fmt_header_std = workbook.add_format({**header_base, "fg_color": "#EEEEEE"})
            fmt_header_ac = workbook.add_format({**header_base, "fg_color": "#93c47d"})
            fmt_header_pac = workbook.add_format({**header_base, "fg_color": "#6d9eeb"})
            fmt_header_red = workbook.add_format({**header_base, "fg_color": "#e06666"})
            fmt_header_violet = workbook.add_format({**header_base, "fg_color": "#c76e9b"})
            fmt_bg_ac = workbook.add_format({"bg_color": "#D9EAD3"})
            fmt_bg_pac = workbook.add_format({"bg_color": "#CFE2F3"})
            fmt_bg_red = workbook.add_format({"bg_color": "#F4CCCC"})
            fmt_bg_violet = workbook.add_format({"bg_color": "#E0ADF0"})
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
                elif "Real Backlog" in col_name:
                    # Maybe color it differently? Or same as Remaining?
                    worksheet.set_column(col_idx, col_idx, col_width, fmt_bg_violet)
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
                elif "Real Backlog" in col_name:
                    style = fmt_header_violet   
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
        raise HTTPException(
            status_code=500, detail="Could not generate the Excel report."
        )


@router.get("/remaining-to-accept")
def get_remaining_pos(
    page: int = 1,
    size: int = 20,
    filter_stage: str = "ALL",
    search: Optional[str] = None,
    internal_project_id: Optional[int] = None,
    customer_project_id: Optional[int] = None,
    project_manager_id: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    data = crud.get_remaining_to_accept_paginated(
        db,
        page,
        size,
        filter_stage,
        search,
        internal_project_id,
        customer_project_id,
        project_manager_id,
        user=current_user,
    )
    # Note: Stats are usually global, calculating them with filters might be expensive
    # but let's keep the global stats for the cards at the top
    stats = crud.get_remaining_stats(db, user=current_user)

    return {"data": data, "stats": stats}


@router.get("/bc/export", status_code=status.HTTP_200_OK)
def export_bcs(
    search: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    try:
        # 1) Get DataFrame
        df = crud.get_bcs_export_dataframe(db, search)

        if df is None or df.empty:
            raise HTTPException(status_code=404, detail="No data found to export.")

        # 2) Format dates (robuste: convertit ce qui ressemble à une date)
        for col in df.columns:
            if "date" in col.lower() or col.lower().endswith("_at"):
                try:
                    s = pd.to_datetime(df[col], errors="coerce")
                    if s.notna().any():
                        df[col] = s.dt.strftime("%d/%m/%Y %H:%M").fillna("")
                except Exception:
                    # si conversion impossible, on laisse la colonne telle quelle
                    pass

        # 3) Create Excel (ENGINE OPENPYXL)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="BC Details", index=False)

            ws = writer.sheets["BC Details"]
            # Auto width simple
            for i, col_name in enumerate(df.columns, start=1):
                max_len = max(len(str(col_name)), int(df[col_name].astype(str).map(len).max() if len(df) else 0))
                ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = min(max_len + 2, 40)

        output.seek(0)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        headers = {
            "Content-Disposition": f'attachment; filename="BC_Export_{timestamp}.xlsx"'
        }

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    except HTTPException:
        raise
    except Exception as e:
        print("Export Error:", repr(e))
        traceback.print_exc()
        raise  


@router.get(
    "/bc-candidates", response_model=List[schemas.MergedPO]
)  # Use MergedPO schema
def get_bc_candidates(
    project_id: int,
    site_codes: Optional[str] = Query(None, description="Comma separated codes"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: Session = Depends(get_db),
):
    """
    Returns list of POs eligible for BC creation based on filters.
    """
    code_list = []
    if site_codes:
        # Split by newline (Excel paste) or comma
        code_list = site_codes.replace("\n", ",").split(",")

    return crud.get_eligible_pos_for_bc(db, project_id, code_list, start_date, end_date)


@router.post("/create-bc", response_model=schemas.BCResponse)
def generate_bc(
    bc_data: schemas.BCCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    try:
        return crud.create_bon_de_commande(db, bc_data, current_user.id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Failed to generate BC")


# data_router.py

@router.get("/bc/list/{status}", response_model=List[schemas.BCResponse])
def list_bcs(
    status: str, 
    search: Optional[str] = Query(None), # Add this
    db: Session = Depends(get_db)
):
    # Map string to Enum
    try:
        status_enum = models.BCStatus(status)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid status")

    # Pass the search term to your existing CRUD function
    return crud.get_bcs_by_status(db, status_enum, search_term=search)

@router.get("/bc/all", response_model=List[schemas.BCResponse])  # Use your schema
def read_all_bcs(
    background_tasks: BackgroundTasks ,  # <-- Add this parameter
    search: Optional[str] = None,
    status_filter: Optional[str] = None, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    try:
        crud.check_rejections_and_notify(db, background_tasks)
    except Exception as e:
        print(f"Error in background check: {e}")

    return crud.get_all_bcs(db, current_user, search=search, status_filter=status_filter)  # <-- Pass status_filter


@router.post("/bc/{bc_id}/approve-l1")
def approve_l1(
    bc_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
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
            link=f"/configuration/bc/detail/{bc.id}",
        )
    db.commit()
    return bc


@router.post("/bc/{bc_id}/approve-l2")
def approve_l2(
    bc_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    # Check if Admin
    bc = crud.approve_bc_l2(db, bc_id, current_user.id)

    # NOTIFY CREATOR
    crud.create_notification(
        db,
        recipient_id=bc.creator_id,
        type=models.NotificationType.APP,
        title="BC Approved",
        message=f"Your BC {bc.bc_number} has been fully approved.",
        link=f"/configuration/bc/detail/{bc.id}",
    )
    db.commit()
    return bc


@router.get("/bc/{bc_id}/pdf")
def get_bc_pdf(
    bc_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
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
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/import/assign-projects-only")
async def assign_projects_only(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
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
    current_user: models.User = Depends(auth.get_current_user),
):
    return crud.reject_bc(
        db, bc_id=bc_id, reason=rejection_data.reason, rejector_id=current_user.id
    )


@router.post("/bc/{bc_id}/submit")
def submit_bon_de_commande(
    bc_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    bc = crud.submit_bc(db, bc_id)

    # FIND PROJECT DIRECTORS
    pds = (
        db.query(models.User).filter(models.User.role == "PD").all()
    )  # Or "Project Director"

    for pd in pds:
        crud.create_notification(
            db,
            recipient_id=pd.id,
            type=models.NotificationType.TODO,
            title="Approval Required",
            message=f"BC {bc.bc_number} submitted by {current_user.first_name} requires L1 validation.",
            link=f"/configuration/bc/detail/{bc.id}",
        )
    db.commit()
    return bc


@router.get("/bc/{bc_id}", response_model=schemas.BCResponse)
def get_bc_details(bc_id: int, db: Session = Depends(get_db)):
    bc = crud.get_bc_by_id(db, bc_id)
    if not bc:
        raise HTTPException(status_code=404, detail="BC not found")
    return bc


@router.post("/bc/{bc_id}/cancel", status_code=status.HTTP_200_OK)
def cancel_bc_endpoint(
    bc_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    """Cancels a Bon de Commande if it's in DRAFT status."""
    try:
        # Pass the current user to the CRUD function for ownership check
        crud.cancel_bc(db, bc_id, current_user.id)
        return {"message": "BC cancelled successfully."}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
@router.post("/bc/item/{item_id}/validate")
def validate_item(
    item_id: int, 
    payload: schemas.ValidationPayload, # { action: "APPROVE"|"REJECT", comment: str }
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    return crud.validate_bc_item(db, item_id, current_user, payload.action, payload.comment)



@router.get("/stats")
def get_stats(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """Returns the 3 cards data: Balance, Pending, Spent"""
    return crud.get_caisse_stats(db, current_user)


@router.get("/transactions")
def list_transactions(
    page: int = 1,
    limit: int = 20,
    type: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    search: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """Returns the history table data"""
    return crud.get_transactions(
        db, current_user, page, limit, type, start_date, end_date, search
    )


@router.get("/requests/pending")
def list_pending_requests(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """Returns the list of requests for the dashboard table (Admin/PD only)"""
    # Optional: Enforce security
    if current_user.role not in [models.UserRole.ADMIN, models.UserRole.PD]:
        return []
        
    return crud.get_pending_requests(db)
@router.post("/request")
def create_fund_request(
    payload: schemas.FundRequestCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    return crud.create_fund_request(db, current_user.id, payload.items) 


@router.post("/request/{req_id}/review")
def review_fund_request(
    req_id: int, 
    payload: schemas.FundRequestReview,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    if current_user.role not in [models.UserRole.ADMIN]:
        raise HTTPException(403, "Only Admins can review requests")

    if payload.action == "REJECT":
        # Handle rejection (simple status update)
        req = db.query(models.FundRequest).get(req_id)
        req.status = models.FundRequestStatus.REJECTED
        db.commit()
        return {"message": "Request Rejected"}
    
    # Handle Approval
    # Convert list to dict for easier lookup {item_id: amount}
    approved_map = {str(i.item_id): i.approved_amount for i in payload.items}
    
    crud.approve_fund_request(db, req_id, current_user.id, approved_map)
    return {"message": "Request Approved"}

@router.get("/request/{req_id}", response_model=schemas.FundRequestDetail) # <--- USE THE NEW SCHEMA
def get_fund_request_details(
    req_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    return crud.get_request_by_id(db, req_id)
@router.post("/request/{req_id}/confirm")
def confirm_fund_reception_endpoint(
    req_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    PD confirms receipt of funds. This triggers the wallet updates and transaction creation.
    """
    # Security: Only PD can confirm
    if current_user.role != models.UserRole.PD:
         # Or allow ADMIN too if needed for testing
         raise HTTPException(403, "Only Project Directors can confirm receipt.")

    try:
        crud.confirm_fund_reception(db, req_id, current_user.id)
        return {"message": "Funds confirmed and wallets updated."}
    except ValueError as e:
        raise HTTPException(400, detail=str(e))

@router.get("/wallets-summary")
def get_wallets_summary(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    if current_user.role not in [models.UserRole.ADMIN, models.UserRole.PD]:
        raise HTTPException(403, "Access denied")
    return crud.get_all_wallets_summary(db)

@router.post("/request/{req_id}/process")
def process_request_endpoint(
    req_id: int, 
    payload: schemas.FundRequestReviewAction,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    if current_user.role != models.UserRole.ADMIN:
        raise HTTPException(403, "Admins only")
        
    try:
        crud.process_fund_request(db, req_id, payload, current_user.id)
        return {"message": "Request processed"}
    except ValueError as e:
        raise HTTPException(400, str(e))
    
