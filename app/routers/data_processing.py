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
from fastapi import Query, status,Form
import json # Ensure this is imported

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
from ..utils.email import send_bc_status_email, send_email_background
from fastapi.temp_pydantic_v1_params import Body


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


@router.get("/import/history", response_model=schemas.PageUploadHistory)
def read_upload_history(
    db: Session = Depends(get_db),
    page: int = Query(1, gt=0),
    limit: int = Query(20, gt=0),
    status: str = Query(None),
    search: str = Query(None),
    current_user: models.User = Depends(auth.get_current_user),
):
    return crud.get_upload_history_paginated(db, page=page, limit=limit, status=status, search=search)


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
    category: Optional[str] = Query(None),
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
        category=category,
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
    category: Optional[str] = Query(None),
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
            category=category,
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

@router.get("/caisse/reserved-breakdown")
def get_reserved_breakdown_endpoint(
    db: Session = Depends(get_db), 
    current_user: models.User = Depends(get_current_user)
):
    """
    Returns the 3-bucket breakdown of funds for the current user.
    """
    # 1. PENDING IN (Future Gap): PD Validated - Admin Paid
    pd_gap_reqs = db.query(models.FundRequest).filter(
        models.FundRequest.requester_id == current_user.id,
        models.FundRequest.status.in_([
            models.FundRequestStatus.VALIDATED_PD,
            models.FundRequestStatus.PARTIALLY_PAID
        ])
    ).all()
    
    pending_in_list = []
    for r in pd_gap_reqs:
        # Calculate gap using the new waterfall fields
        pd_val = r.pd_validated_amount or 0.0
        adm_paid = r.paid_amount or 0.0
        gap = pd_val - adm_paid
        
        if gap > 0.1:
            pending_in_list.append({
                "ref": r.request_number, 
                "amount": gap, 
                "desc": "Validated by PD, waiting for Admin release"
            })

    # 2. RESERVED ALIMENTATION (In Transit): Admin Paid - RAF Confirmed
    # We find PENDING CREDIT transactions in the user's wallet
    wallet = db.query(models.Caisse).filter(models.Caisse.user_id == current_user.id).first()
    alimentation_list = []
    
    if wallet:
        transit_txs = db.query(models.Transaction).filter(
            models.Transaction.caisse_id == wallet.id,
            models.Transaction.type == models.TransactionType.CREDIT,
            models.Transaction.status == models.TransactionStatus.PENDING
        ).all()
        
        alimentation_list = [
            {"ref": t.description, "amount": t.amount, "date": t.created_at}
            for t in transit_txs
        ]

    # 3. RESERVED EXPENSES (Liability): Active Expenses
    active_expenses = db.query(models.Expense).filter(
        models.Expense.requester_id == current_user.id,
        models.Expense.status.notin_([
            "PAID", 
            "ACKNOWLEDGED", 
            "REJECTED"
        ])
    ).all()
    
    expense_list = [
        {"id": e.id, "desc": e.remark, "beneficiary": e.beneficiary, "amount": e.amount, "status": e.status} 
        for e in active_expenses
    ]

    return {
        "pending_in": pending_in_list,
        "reserved_alimentation": alimentation_list,
        "reserved_expenses": expense_list
    }

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


@router.get("/export/excel")
def export_bcs_to_excel(
    format: str = "details", # or "headers"
    search: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Exports BCs to Excel based on user role and requested format.
    """
    df = crud.get_bc_export_dataframe(db, current_user, format, search)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='BC Export')
        
        # Auto-adjust columns width for better look
        worksheet = writer.sheets['BC Export']
        for i, col in enumerate(df.columns):
            column_len = max(df[col].astype(str).str.len().max(), len(col)) + 2
            worksheet.set_column(i, i, column_len)

    output.seek(0)
    
    filename = f"BC_Export_{format}_{datetime.now().strftime('%Y%m%d')}.xlsx"
    headers = {
        'Content-Disposition': f'attachment; filename="{filename}"'
    }
    return StreamingResponse(
        output, 
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers=headers
    )


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

@router.get("/bc/ready-for-acceptance", response_model=List[schemas.BCResponse])
def list_bcs_ready_for_act(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    # 1. Start the query
    query = db.query(models.BonDeCommande).join(
        models.InternalProject, models.BonDeCommande.project_id == models.InternalProject.id
    ).options(
        joinedload(models.BonDeCommande.sbc),
        joinedload(models.BonDeCommande.internal_project),
        joinedload(models.BonDeCommande.items)
    ).filter(
        models.BonDeCommande.status == models.BCStatus.APPROVED
    )

    # 2. Apply Visibility Security (Matrix Check)
    role_str = str(current_user.role).upper()
    
    # A. GLOBAL BYPASS: Only Admin, CEO, and RAF see everything blindly
    if "ADMIN" in role_str or "RAF" in role_str or "CEO" in role_str:
        pass 
        
    # B. SBC VISIBILITY: Only see their own assigned BCs
    elif "SBC" in role_str:
        if not current_user.sbc_id:
            return[]
        query = query.filter(models.BonDeCommande.sbc_id == current_user.sbc_id)
        
    # C. MATRIX VISIBILITY: PMs, PDs, QCs, Coordinators...
    else:
        # Define relevant roles for ACT generation and approval visibility
        relevant_actions =[
            models.ProjectActionType.ROLE_PD,
            models.ProjectActionType.ROLE_PM,
            models.ProjectActionType.ROLE_PC,
            models.ProjectActionType.ROLE_RQC,
            models.ProjectActionType.ACT_GENERATE,
            models.ProjectActionType.ACT_APPROVE_RQC, # <--- Vital for QC
            models.ProjectActionType.ACT_APPROVE_PM,
            models.ProjectActionType.ACT_APPROVE_PD
        ]

        # 1. Ask the Matrix for allowed Project IDs
        allowed_pids_query = db.query(models.ProjectWorkflow.project_id).filter(
            models.ProjectWorkflow.action_type.in_(relevant_actions),
            or_(
                models.ProjectWorkflow.primary_users.any(id=current_user.id),
                models.ProjectWorkflow.support_users.any(id=current_user.id)
            )
        )
        allowed_project_ids =[row[0] for row in allowed_pids_query.all()]

        # 2. Filter the main query
        if allowed_project_ids:
            query = query.filter(
                or_(
                    models.BonDeCommande.project_id.in_(allowed_project_ids),
                    models.BonDeCommande.creator_id == current_user.id # Creator always sees their own
                )
            )
        else:
            # If they have 0 assignments in the matrix, they ONLY see BCs they explicitly created
            query = query.filter(models.BonDeCommande.creator_id == current_user.id)

    return query.order_by(models.BonDeCommande.approved_l2_at.desc()).all()
@router.get("/bc/{bc_id}", response_model=schemas.BCResponse)
def get_bc_details(bc_id: int, db: Session = Depends(get_db), current_user: models.User = Depends(auth.get_current_user)):
    bc = crud.get_bc_by_id(db, bc_id, current_user)  # This function should implement the same visibility logic as above
    if not bc:
        raise HTTPException(status_code=404, detail="BC not found")

    # --- SECURITY CHECK: Global Admin / RAF / Finance can see everything ---
    if current_user.role in [models.UserRole.ADMIN, models.UserRole.RAF, models.UserRole.CEO]:
        return bc

    # --- SECURITY CHECK: SBC can see their own BCs ---
    if current_user.role == "SBC":
        if bc.sbc_id != current_user.sbc_id:
             raise HTTPException(403, "Access Denied: This BC does not belong to you.")
        return bc

    # --- SECURITY CHECK: Project Team Members ---
    is_assigned_to_project = db.query(models.ProjectWorkflow).filter(
        models.ProjectWorkflow.project_id == bc.project_id,
        or_(
            models.ProjectWorkflow.primary_users.any(id=current_user.id),
            models.ProjectWorkflow.support_users.any(id=current_user.id)
        )
    ).first()

    if not is_assigned_to_project and bc.creator_id != current_user.id:
         raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access Denied: You are not assigned to this project team.")

    return bc

@router.post("/bc/{bc_id}/submit")
def submit_bon_de_commande(
    bc_id: int, 
    background_tasks: BackgroundTasks, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    try:
        # Move from DRAFT to SUBMITTED
        bc = crud.submit_bc(db, bc_id, current_user.id, background_tasks)
        
        # # --- NEW: Notifications based on Matrix ---
        # target_approvers = crud.get_project_users_by_action(
        #     db, 
        #     bc.project_id, 
        #     models.ProjectActionType.BC_APPROVE_L1
        # )
        
        # # Fallback to PDs if no one assigned
        # if not target_approvers:
        #     target_approvers = db.query(models.User).filter(models.User.role == models.UserRole.PD).all()

        # pd_emails = [u.email for u in target_approvers if u.email]
        # if pd_emails:
        #     for u in target_approvers:
        #         crud.create_notification(
        #             db,
        #             recipient_id=u.id,
        #             type=models.NotificationType.TODO,
        #             module=models.NotificationModule.BC,
        #             title="Approval Required",
        #             message=f"BC {bc.bc_number} submitted by {current_user.first_name} requires L1 validation.",
        #             link=f"/configuration/bc/detail/{bc.id}",
        #             created_at=datetime.now(),
        #         )
        #     crud.send_notification_email_detailled(
        #         background_tasks,
        #         pd_emails,
        #         "BC Submitted - L1 Approval Required",
        #         "BC",
        #         "L1 Approval Required",
        #         {
        #             "id": bc.bc_number,
        #             "project": bc.internal_project.name,
        #             "beneficiary": bc.sbc.short_name,
        #             "total": f"{bc.total_amount_ttc:,.2f} MAD",
        #             "category": "Purchase Order",
        #             "remark": "A new Purchase Order (BC) has been submitted and requires Project Director validation."
        #         },
        #         link=f"/configuration/bc/detail/{bc.id}"
        #     )

        db.commit()
        return bc
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/bc/{bc_id}/approve-l1")
def approve_l1(
    bc_id: int,
    payload: schemas.BCApprovalRequest, # <--- NEW PAYLOAD

    background_tasks: BackgroundTasks, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    try:
        bc = crud.approve_bc_l1(db, bc_id, current_user.id, payload.comment, background_tasks)

        # FIND L2 APPROVERS
        # target_l2_approvers = crud.get_project_users_by_action(
        #     db, 
        #     bc.project_id, 
        #     models.ProjectActionType.BC_APPROVE_L2
        # )
        
        # if not target_l2_approvers:
        #     target_l2_approvers = db.query(models.User).filter(models.User.role == "Admin").all()

        # for admin in target_l2_approvers:
        #     crud.create_notification(
        #         db,
        #         recipient_id=admin.id,
        #         type=models.NotificationType.TODO,
        #         module=models.NotificationModule.BC,
        #         title="Final Approval Required",
        #         message=f"BC {bc.bc_number} validated L1. Pending final approval.",
        #         link=f"/configuration/bc/detail/{bc.id}",
        #         created_at=datetime.now(),
        #     )
        #     send_bc_status_email(bc, admin.email, "VALIDATED L1 (Waiting L2)", background_tasks)

        db.commit()
        return bc
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/bc/{bc_id}/approve-l2")
def approve_l2(
    bc_id: int,
    payload: schemas.BCApprovalRequest, # <--- NEW PAYLOAD

    background_tasks: BackgroundTasks, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    try:
        bc = crud.approve_bc_l2(db, bc_id, current_user.id, payload.comment, background_tasks)

        # # NOTIFY CREATOR
        # crud.create_notification(
        #     db,
        #     recipient_id=bc.creator_id,
        #     type=models.NotificationType.APP,
        #     module=models.NotificationModule.BC,
        #     title="BC Approved",
        #     message=f"Your BC {bc.bc_number} has been fully approved.",
        #     link=f"/configuration/bc/detail/{bc.id}",
        #     created_at=datetime.now(),
        # )
        # if bc.sbc and bc.sbc.email:
        #     send_bc_status_email(bc, bc.sbc.email, "APPROVED", background_tasks)

        db.commit()
        return bc
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/bc/{bc_id}", response_model=schemas.BCResponse)
def update_bc(
    bc_id: int,
    payload: schemas.BCCreate, # Reusing create schema for update
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    try:
        return crud.update_bon_de_commande(db, bc_id, payload, current_user.id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/bc/{bc_id}/cancel", status_code=status.HTTP_200_OK)
def cancel_bc_endpoint(
    bc_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    """Cancels a Bon de Commande if it's in DRAFT status."""
    try:
        crud.cancel_bc(db, bc_id, current_user.id)
        return {"message": "BC cancelled successfully."}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))

@router.post("/bc/{bc_id}/reject")
def reject_bon_de_commande(
    background_tasks: BackgroundTasks,  # <-- Add this parameter
    bc_id: int,
    rejection_data: schemas.BCRejectionRequest,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),

):
    try:
        return crud.reject_bc(
            db, bc_id=bc_id, reason=rejection_data.reason, rejector_id=current_user.id,background_tasks=background_tasks
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/bc/{bc_id}/pdf")
def get_bc_pdf(
    bc_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    bc = crud.get_bc_by_id(db, bc_id)
    if not bc:
        raise HTTPException(status_code=404, detail="Bon de Commande not found")

    pdf_buffer = pdf_generator.generate_bc_pdf(bc)
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
    try:
        contents = await file.read()
        stats = crud.bulk_assign_projects_only(db, contents)
        return {"message": "Project assignment complete", "stats": stats}
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/bc/item/{item_id}/validate")
def validate_item(
    item_id: int, 
    payload: schemas.ValidationPayload, # { action: "APPROVE"|"REJECT", comment: str }
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    return crud.validate_bc_item(db, item_id, current_user, payload.action, payload.comment, background_tasks)

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
    type_filter: Optional[str] = Query(None, alias="type_filter"),
    status: str = "ALL", # ADDED
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    search: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    return crud.get_transactions(
        db, current_user, page, limit, type_filter, status, start_date, end_date, search
    )

@router.get("/requests/pending")
def read_pending_requests(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    data = crud.get_pending_requests(db)
    return data

@router.post("/request")
def pm_submit_request(
    payload: schemas.FundRequestCreate, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    return crud.create_pm_fund_request(db, current_user.id, payload, background_tasks)

@router.post("/request/{req_id}/pd-validate")
def pd_validate(
    req_id: int,
    payload: schemas.PDReviewAction,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    req = db.query(models.FundRequest).get(req_id)
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")
    if req.status not in [models.FundRequestStatus.SUBMITTED, models.FundRequestStatus.PARTIALLY_PAID]:
        raise HTTPException(status_code=400, detail="Request not at the PD level.")
    
    return crud.pd_validate_request(db, req_id, current_user.id, payload, background_tasks)

@router.post("/request/{req_id}/admin-authorize")
def admin_authorize(
    req_id: int,
    payload: schemas.AdminReviewAction,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    req = db.query(models.FundRequest).get(req_id)
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")
    if req.status not in [models.FundRequestStatus.VALIDATED_PD, models.FundRequestStatus.PARTIALLY_PAID]:
        raise HTTPException(status_code=400, detail="Request must be validated by PD first.")
    
    return crud.admin_authorize_request(db, req_id, current_user.id, payload, background_tasks)

@router.post("/request/{req_id}/raf-confirm")
async def raf_confirm(
    req_id: int,
    item_confirmations: str = Form(...), # JSON string
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    file_name = f"RAF_CONFIRM_{req_id}_{file.filename}"
    save_path = f"uploads/caisse_reception/{file_name}"
    with open(save_path, "wb") as buffer:
        buffer.write(await file.read())

    confirm_dict = json.loads(item_confirmations)
    return crud.raf_confirm_reception(db, req_id, current_user.id, confirm_dict, file_name, background_tasks)

@router.get("/request/{req_id}", response_model=schemas.FundRequestDetail)
def get_fund_request_details(
    req_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    return crud.get_request_by_id(db, req_id)

@router.get("/wallets-summary")
def get_wallets_summary(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    if current_user.role not in [models.UserRole.ADMIN, models.UserRole.PD, models.UserRole.RAF]:
        raise HTTPException(403, "Access denied")
    return crud.get_all_wallets_summary(db)

@router.post("/request/{req_id}/acknowledge-variance")
def ack_variance(req_id: int, payload: dict = Body(...), db: Session = Depends(get_db)):
    return crud.acknowledge_variance(db, req_id, payload.get("note"))

@router.get("/history/grouped")
def get_history_grouped(
    page: int = 1,
    limit: int = 10,
    status: Optional[str] = None,
    db: Session = Depends(get_db)
):
    return crud.get_grouped_history(db, page, limit, status)

@router.get("/by-sbc/{sbc_id}")
def get_bc_items_for_sbc_selection(
    sbc_id: int, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    items = crud.get_bc_items_by_sbc(db, sbc_id)
    output = []
    for item in items:
        site = item.merged_po.site_code if item.merged_po else "No Site"
        desc = item.merged_po.item_description[:40] if item.merged_po else "No Description"
        
        output.append({
            "value": item.id,
            "label": f"{item.bc.bc_number} | {site} ({desc}...)",
            "bc_number": item.bc.bc_number
        })
    return output

@router.get("/reception-file/{filename}")
def get_reception_file(filename: str):
    cwd = os.getcwd() 
    file_path = os.path.join(cwd, "uploads", "caisse_reception", filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found at: {file_path}")
    return FileResponse(file_path)

@router.put("/bulk-update-category")
def bulk_update_category(
    payload: schemas.BulkCategoryUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    if current_user.role != models.UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Only Admins can manage categories.")
    
    count = crud.bulk_update_po_categories(db, payload.po_ids, payload.category)
    return {"message": f"Successfully updated {count} lines to {payload.category}."}
