# app/routers/expenses.py
from datetime import datetime
import io
import os
import shutil
import logging
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, status
from fastapi.responses import StreamingResponse, Response
import pandas as pd
from sqlalchemy.orm import Session, joinedload
from typing import List,Optional
import mimetypes # Ensure this is imported at the top
import json

from .. import crud, models, schemas, auth
from ..dependencies import get_current_user, get_db
from ..utils.pdf_generator import generate_expense_pdf # Import the new PDF generator
from fastapi import UploadFile, File, Form
from fastapi.responses import FileResponse

router = APIRouter(prefix="/api/expenses", tags=["expenses"])


# --- HELPER: Role Checker ---
def require_roles(user, roles):
    user_role_str = str(user.role).upper().strip() if not isinstance(user.role, str) else user.role.upper().strip()
    
    allowed_roles = []
    for r in roles:
        r_str = str(r).upper().strip() if not isinstance(r, str) else r.upper().strip()
        allowed_roles.append(r_str)
        # Handle PD variations
        if r_str == "PD": allowed_roles.append("PROJECT DIRECTOR")
        if r_str == "PROJECT DIRECTOR": allowed_roles.append("PD")

    if user_role_str not in allowed_roles:
        raise HTTPException(
            status_code=403, 
            detail=f"Forbidden. Your role '{user_role_str}' is not in {allowed_roles}"
        )


# ==========================
# 1. CREATE & LIST
# ==========================

@router.post("/", response_model=schemas.ExpenseResponse)
async def create_expense_endpoint(
    background_tasks: BackgroundTasks,
    # Accept the file
    file: UploadFile = File(None), 
    # Accept the rest of the data as a JSON string inside a form field
    payload_str: str = Form(...), 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    require_roles(current_user,[models.UserRole.PM, models.UserRole.COORDINATEUR, models.UserRole.ADMIN, models.UserRole.PD])
    
    # Parse the JSON string back into your Pydantic Schema
    try:
        payload_dict = json.loads(payload_str)
        payload = schemas.ExpenseCreate(**payload_dict)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid payload format: {str(e)}")

    # Handle File Saving
    filename = None
    if file:
        os.makedirs("uploads/expenses", exist_ok=True)
        filename = f"EXP_{current_user.id}_{file.filename}"
        file_path = os.path.join("uploads", "expenses", filename)
        with open(file_path, "wb") as buffer:
            buffer.write(await file.read())

    try:
        # Pass the filename to the CRUD function
        return crud.create_expense(db, payload, current_user.id, background_tasks, filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/search", response_model=schemas.PaginatedResponse)
def search_expenses_endpoint(
    scope: str = "my",
    page: int = 1,
    limit: int = 20,
    project_id: Optional[int] = None,
    exp_type: Optional[str] = None,
    beneficiary: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    # Security: Define who can access which scope
    if scope == "pay":
        # Only RAF and ADMIN can see what needs to be paid
        require_roles(current_user, ["RAF", "ADMIN"])
    
    elif scope in ["l1", "l2", "all", "history"]:
        # RAF should be able to see History and All requests for Audit purposes
        # PD and ADMIN can also see these
        require_roles(current_user, ["PD", "ADMIN", "RAF"])

    filters = {
        "project_id": project_id,
        "exp_type": exp_type,
        "beneficiary": beneficiary,
        "start_date": start_date,
        "end_date": end_date
    }
    
    return crud.search_expenses(db, current_user, scope, page, limit, filters)

@router.get("/my-requests", response_model=List[schemas.ExpenseResponse])
def get_my_requests(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    """Personal view: My drafts and my payments."""
    return crud.list_personal_requests(db, current_user)


@router.get("/all-requests", response_model=List[schemas.ExpenseResponse])
def get_all_requests(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    """Global view: Management oversight."""
    require_roles(current_user, ["ADMIN", "PD", "RAF"])
    return crud.list_all_requests_global(db, current_user)



@router.get("/wallets-summary")
def get_wallets_summary(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """Admin view of all wallets."""
    require_roles(current_user, [models.UserRole.ADMIN, models.UserRole.PD, models.UserRole.RAF])
    return crud.get_all_wallets_summary(db)


@router.post("/run-compliance-checks")
def run_daily_checks(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Admin trigger for 24h reminders."""
    require_roles(current_user, [models.UserRole.ADMIN])
    count = crud.check_missing_expense_uploads(db, background_tasks)
    return {"message": f"Sent {count} reminders."}

# backend/app/routers/expenses.py

@router.get("/export/excel")
def export_expenses_to_excel(
    format: str = "details", 
    search: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    df = crud.get_expense_export_dataframe(db, current_user, format, search)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Expense Export')
        
        # Formatting
        worksheet = writer.sheets['Expense Export']
        workbook = writer.book
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
        
        for i, col in enumerate(df.columns):
            column_len = max(df[col].astype(str).str.len().max(), len(col)) + 2
            worksheet.set_column(i, i, column_len)

    output.seek(0)
    filename = f"Expenses_{format}_{datetime.now().strftime('%Y%m%d')}.xlsx"
    return StreamingResponse(
        output, 
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'}
    )

# ==========================
# 5. LISTS (FILTERED)
# ==========================

@router.get("/pending-l1", response_model=List[schemas.ExpenseResponse])
def get_pending_l1(db: Session = Depends(get_db), current_user: models.User = Depends(auth.get_current_user)):
    query = db.query(models.Expense).options(
        joinedload(models.Expense.internal_project),
        joinedload(models.Expense.requester)
    ).filter(models.Expense.status == models.ExpenseStatus.SUBMITTED)

    # --- STAKEHOLDER FILTER ---
    if current_user.role not in [models.UserRole.ADMIN]:
        query = query.join(
            models.ProjectStakeholder,
            models.Expense.project_id == models.ProjectStakeholder.project_id
        ).filter(
            models.ProjectStakeholder.user_id == current_user.id
        )
    # --------------------------

    return query.order_by(models.Expense.created_at.desc()).all()


@router.get("/pending-l2", response_model=List[schemas.ExpenseResponse])
def get_pending_l2(db: Session = Depends(get_db), current_user: models.User = Depends(auth.get_current_user)):
    query = db.query(models.Expense).options(
            joinedload(models.Expense.internal_project),
            joinedload(models.Expense.requester)
        ).filter(models.Expense.status == models.ExpenseStatus.PENDING_L2)

        # --- STAKEHOLDER FILTER ---
    if current_user.role not in [models.UserRole.ADMIN, models.UserRole.RAF]:
        query = query.join(
            models.ProjectStakeholder,
            models.Expense.project_id == models.ProjectStakeholder.project_id
        ).filter(
            models.ProjectStakeholder.user_id == current_user.id
        )
    # --------------------------

    return query.order_by(models.Expense.created_at.desc()).all()



@router.get("/pending-payment", response_model=List[schemas.ExpenseResponse])
def get_pending_payment(db: Session = Depends(get_db), current_user: models.User = Depends(auth.get_current_user)):
    require_roles(current_user, ["RAF", "ADMIN"])
    # "APPROVED_L2" means Admin validated, now waiting for PD to pay
    return db.query(models.Expense).filter(models.Expense.status == models.ExpenseStatus.APPROVED_L2).all()
@router.get("/paid-history", response_model=List[schemas.ExpenseResponse])
def get_paid_history(
    db: Session = Depends(get_db), 
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Returns expenses that are PAID or ACKNOWLEDGED.
    Used by PDs/Admins to review history and upload missing files.
    """
    require_roles(current_user, [models.UserRole.RAF, models.UserRole.ADMIN, models.UserRole.PD])
    
    return db.query(models.Expense).filter(
        models.Expense.status.in_([
            models.ExpenseStatus.PAID, 
            models.ExpenseStatus.ACKNOWLEDGED
        ])
    ).order_by(models.Expense.updated_at.desc()).all()

@router.get("/summary-counts")
def get_expense_summary_counts(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    counts = {"l1": 0, "l2": 0, "pay": 0}
    
    # Helper to apply project filter if needed
    def filter_by_stakeholder(query, role_needed=None):
        if current_user.role not in [models.UserRole.ADMIN, models.UserRole.RAF]:
            query = query.join(models.ProjectStakeholder, models.Expense.project_id == models.ProjectStakeholder.project_id)
            query = query.filter(models.ProjectStakeholder.user_id == current_user.id)
            if role_needed:
                query = query.filter(models.ProjectStakeholder.role == role_needed)
        return query

    # L1: PDs (Filtered by assignment) or Admins
    if current_user.role in [models.UserRole.PD, models.UserRole.ADMIN]:
        q = db.query(models.Expense).filter(models.Expense.status == models.ExpenseStatus.SUBMITTED)
        # Only count projects where I am the PD
        q = filter_by_stakeholder(q, models.ProjectRoleType.PD) 
        counts["l1"] = q.count()

    # L2: Admin Only (Global)
    if current_user.role == models.UserRole.ADMIN:
        counts["l2"] = db.query(models.Expense).filter(models.Expense.status == models.ExpenseStatus.PENDING_L2).count()

    # Pay: RAF/Admin (Global)
    if current_user.role in [models.UserRole.RAF, models.UserRole.ADMIN]:
        counts["pay"] = db.query(models.Expense).filter(models.Expense.status == models.ExpenseStatus.APPROVED_L2).count()
        
    return counts

# @router.get("/caisse/reserved-breakdown")
# def get_reserved_breakdown(db: Session = Depends(get_db), current_user = Depends(get_current_user)):
    
#     # 1. Pending In (PD Gap)
#     pd_gap_reqs = db.query(models.FundRequest).filter(
#         models.FundRequest.requester_id == current_user.id,
#         models.FundRequest.status.in_([
#             models.FundRequestStatus.VALIDATED_PD,
#             models.FundRequestStatus.PARTIALLY_PAID
#         ])
#     ).all()
    
#     pending_in_list = []
#     for r in pd_gap_reqs:
#         gap = (r.pd_validated_amount or 0.0) - (r.paid_amount or 0.0)
#         if gap > 0.1:
#             pending_in_list.append({
#                 "ref": r.request_number, 
#                 "amount": gap, 
#                 "desc": "PD Validated, awaiting Admin"
#             })

#     # 2. Reserved Alimentation (In Transit)
#     transit_txs = db.query(models.Transaction).join(models.FundRequest).filter(
#         models.Transaction.caisse_id == current_user.caisse.id,
#         models.Transaction.type == models.TransactionType.CREDIT,
#         models.Transaction.status == models.TransactionStatus.PENDING
#     ).all()
    
#     alimentation_list = [
#         {"ref": t.related_request.request_number, "amount": t.amount, "date": t.created_at}
#         for t in transit_txs
#     ]

#     # 3. Reserved Expenses
#     active_expenses = db.query(models.Expense).filter(
#         models.Expense.requester_id == current_user.id,
#         models.Expense.status.notin_([
#             models.ExpenseStatus.PAID, 
#             models.ExpenseStatus.ACKNOWLEDGED,
#             models.ExpenseStatus.REJECTED
#         ])
#     ).all()
    
#     expense_list = [
#         {"id": e.id, "desc": e.description, "amount": e.amount, "status": e.status} 
#         for e in active_expenses
#     ]

#     return {
#         "pending_in": pending_in_list,
#         "reserved_alimentation": alimentation_list,
#         "reserved_expenses": expense_list
#     }

@router.get("/sbc/{sbc_id}/ledger", response_model=List[dict])
def get_sbc_financial_status(
    sbc_id: int, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Public-style Ledger for a Subcontractor.
    Access: Internal Staff (PM/PD/Admin) to verify debts before paying.
    """
    # Security: Ensure role is internal
    if current_user.role == models.UserRole.SBC:
        # If an SBC calls this, verify they are only looking at THEIR own ID
        if current_user.sbc_id != sbc_id:
            raise HTTPException(status_code=403, detail="Cannot view other SBC ledgers")
            
    return crud.get_sbc_ledger(db, sbc_id)


@router.get("/{id}", response_model=schemas.ExpenseResponse)
def get_expense_details(
    id: int, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Get single expense details with strict security checks.
    """
    # 1. Fetch Expense with all relationships needed for the UI
    expense = db.query(models.Expense).options(
        joinedload(models.Expense.internal_project),
        joinedload(models.Expense.requester),
        joinedload(models.Expense.acts), # For Batch details
        joinedload(models.Expense.l1_approver),
        joinedload(models.Expense.l2_approver)
    ).filter(models.Expense.id == id).first()

    if not expense:
        raise HTTPException(status_code=404, detail="Expense not found")
    
    # --- SECURITY LOGIC ---

    # 1. Global Admin / RAF: Access Everything
    if current_user.role in [models.UserRole.ADMIN, models.UserRole.RAF]:
        return expense

    # 2. Personal Access: Creator or Beneficiary (SBC)
    if expense.requester_id == current_user.id:
        return expense
    
    if expense.beneficiary_user_id == current_user.id:
        return expense

    # 3. Project Context: Is the user a Stakeholder on THIS project?
    # This covers PDs, PMs, and Coordinators assigned to this specific project.
    is_stakeholder = db.query(models.ProjectStakeholder).filter(
        models.ProjectStakeholder.user_id == current_user.id,
        models.ProjectStakeholder.project_id == expense.project_id
    ).first()

    if is_stakeholder:
        return expense

    # --- ACCESS DENIED ---
    raise HTTPException(status_code=403, detail="Not authorized to view this expense.")


@router.get("/attachment/{filename}")
def get_expense_attachment(
    filename: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    # Security: Ensure user has access (simplified for now to PD/Admin/Owner)
    # In production, check if filename belongs to an expense user can see
    
    file_path = f"uploads/expenses/{filename}"
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found on server")
        
    return FileResponse(file_path)

@router.get("/{id}/download-receipt")
def download_expense_receipt(
    id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    expense = db.query(models.Expense).get(id)
    if not expense:
        raise HTTPException(status_code=404, detail="Expense not found")

    filename = expense.signed_doc_url or expense.attachment
    if not filename:
        raise HTTPException(status_code=404, detail="No file uploaded.")

    file_path = os.path.join("uploads/expenses", filename)

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found on disk.")

    # --- THE FIX: Automatically detect if it's image/jpeg or application/pdf ---
    mime_type, _ = mimetypes.guess_type(file_path)
    
    return FileResponse(
        file_path, 
        media_type=mime_type or "application/octet-stream", 
        filename=filename
    )


# ==========================
# 2. APPROVAL WORKFLOW
# ==========================

@router.post("/{id}/submit")
def submit_expense(
    id: int, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks() # For Emails
):
    """Move from DRAFT to SUBMITTED."""
    expense = db.query(models.Expense).get(id)
    if not expense: raise HTTPException(404, "Not found")
    if expense.requester_id != current_user.id: raise HTTPException(403, "Not owner")
    
    try:
        return crud.submit_expense(db, id, background_tasks)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/{id}/approve-l1", response_model=schemas.ExpenseResponse)
def approve_l1(
    id: int, 
    payload: schemas.ExpenseApproveAction, # <-- Accept Comment

    db: Session = Depends(get_db), 
    current_user: models.User = Depends(auth.get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """PD Approval"""
    require_roles(current_user, [models.UserRole.PD, models.UserRole.ADMIN])
    try:
        return crud.approve_expense_l1(db, id, current_user.id, background_tasks, payload.comment)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/{id}/approve-l2", response_model=schemas.ExpenseResponse)
def approve_l2(
    background_tasks: BackgroundTasks,
    id: int, 
    payload: schemas.ExpenseApproveAction, # <-- Accept Comment

    db: Session = Depends(get_db), 
    current_user: models.User = Depends(auth.get_current_user),
):
    """Admin Approval"""
    require_roles(current_user, [models.UserRole.ADMIN])
    try:
        return crud.approve_expense_l2(db, id, current_user.id, background_tasks,payload.comment)
    except ValueError as e:
        raise HTTPException(400, str(e))
@router.post("/{id}/reject", response_model=schemas.ExpenseResponse)
def reject_expense(
    id: int,
    payload: schemas.ExpenseReject,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    """Reject at any stage. Money returns to Balance."""
    require_roles(current_user, [models.UserRole.PD, models.UserRole.ADMIN])
    
    exp = db.query(models.Expense).get(id)
    if not exp:
        raise HTTPException(status_code=404, detail="Expense not found")
    
    # 1. REFUND LOGIC (With Safety Checks for None)
    caisse = db.query(models.Caisse).filter(models.Caisse.user_id == exp.requester_id).first()
    
    if caisse:
        # Treat None as 0.0 to prevent TypeError
        current_reserved = caisse.reserved_balance if caisse.reserved_balance is not None else 0.0
        current_balance = caisse.balance if caisse.balance is not None else 0.0
        
        # Apply Refund
        caisse.reserved_balance = current_reserved - exp.amount
        caisse.balance = current_balance + exp.amount
        
    # 2. Update Status
    exp.status = models.ExpenseStatus.REJECTED
    exp.rejection_reason = payload.reason
    
    db.commit()
    db.refresh(exp) # Refresh to get updated data
    
    return exp # <--- CRITICAL: MUST RETURN THE OBJECT


# ==========================
# 3. PAYMENT & CLOSURE
# ==========================

@router.get("/{id}/pdf")
def get_payment_voucher_pdf(
    id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """Generate the Payment Voucher PDF for physical signature."""
    expense = db.query(models.Expense).get(id)
    if not expense: raise HTTPException(404, "Not found")
    
    # Only allow if Approved L2 (Ready for payment)
    if expense.status not in [models.ExpenseStatus.APPROVED_L2, models.ExpenseStatus.PAID, models.ExpenseStatus.ACKNOWLEDGED]:
        raise HTTPException(400, "Expense must be approved by Admin (L2) before generating voucher.")

    pdf_buffer = generate_expense_pdf(expense)
    
    return StreamingResponse(
        pdf_buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=Voucher_{expense.id}.pdf"}
    )


@router.post("/{id}/confirm-payment")
def confirm_payment(
    id: int, 
    # Use File(None) to make it optional, but correct type
    file: UploadFile = File(None), 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    require_roles(current_user, [models.UserRole.RAF, models.UserRole.ADMIN])
    
    filename = None
    if file:
        # Define storage path
        upload_dir = "uploads/expenses"
        os.makedirs(upload_dir, exist_ok=True) # Create folder if not exists
        
        # Save file with unique name
        filename = f"EXP_{id}_{file.filename}"
        file_path = os.path.join(upload_dir, filename)
        
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

    try:
        # Update DB with filename
        crud.confirm_expense_payment(db, id, filename, current_user.id,background_tasks=BackgroundTasks())
        return {"message": "Payment confirmed", "filename": filename}
    except Exception as e:
        raise HTTPException(400, str(e))

    
@router.post("/{id}/acknowledge")
def acknowledge_receipt(
    id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """Beneficiary (SBC or PM) confirms receipt."""
    try:
        crud.acknowledge_payment(db, id, current_user.id, background_tasks=BackgroundTasks())
        return {"message": "Receipt acknowledged."}
    except ValueError as e:
        raise HTTPException(403, str(e))

@router.put("/{id}", response_model=schemas.ExpenseResponse)
def update_expense(
    id: int,
    payload: schemas.ExpenseCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Edit a DRAFT expense.
    Handles financial adjustments (refunds/deductions) automatically.
    """
    require_roles(current_user, [models.UserRole.PM, models.UserRole.ADMIN])
    
    try:
        # Calls the updated CRUD function
        updated_exp = crud.update_expense(db, id, payload, current_user.id)
        
        # If the user decided to Submit immediately during the edit
        if not payload.is_draft:
            # We can trigger the notification manually here or rely on crud
            # Let's send the PD notification
            pd_emails = crud.get_emails_by_role(db, models.UserRole.PD)
            crud.send_notification_email(
                background_tasks,
                pd_emails,
                "Expense Submitted (Edited)",
                "",
                {
                    "message": f"PM {current_user.first_name} has edited and submitted an expense.",
                    "details": {"Amount": f"{updated_exp.amount} MAD"},
                    "link": "/expenses?tab=l1"
                }
            )
            
        return updated_exp
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
 