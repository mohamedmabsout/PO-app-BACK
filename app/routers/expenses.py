# app/routers/expenses.py
from datetime import datetime
import io
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, status
from fastapi.responses import StreamingResponse, Response
from fastapi.temp_pydantic_v1_params import Body
import pandas as pd
from sqlalchemy.orm import Session
from typing import List

from .. import crud, models, schemas, auth
from ..dependencies import get_current_user, get_db
from ..utils.pdf_generator import generate_expense_pdf # Import the new PDF generator

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
def create_expense(
    payload: schemas.ExpenseCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    """
    Create Expense (Draft or Submitted). 
    Money is moved to 'Reserved' immediately.
    """
    # Allowed: PMs, Coordinators, Admins, PDs
    require_roles(current_user, [models.UserRole.PM, models.UserRole.COORDINATEUR, models.UserRole.ADMIN, models.UserRole.PD])
    try:
        return crud.create_expense(db, payload, current_user.id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/my-requests", response_model=List[schemas.ExpenseResponse])
def get_my_requests(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    """Returns expenses created by the logged-in user."""
    return crud.list_my_requests(db, current_user)



@router.get("/wallets-summary")
def get_wallets_summary(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """Admin view of all wallets."""
    require_roles(current_user, [models.UserRole.ADMIN, models.UserRole.PD])
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


@router.get("/export/excel")
def export_expenses(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    df = crud.get_expenses_export_dataframe(db)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Expenses')
    output.seek(0)
    
    return StreamingResponse(
        output,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': 'attachment; filename="Expenses_Export.xlsx"'}
    )


# ==========================
# 5. LISTS (FILTERED)
# ==========================

@router.get("/pending-l1", response_model=List[schemas.ExpenseResponse])
def get_pending_l1(db: Session = Depends(get_db), current_user: models.User = Depends(auth.get_current_user)):
    require_roles(current_user, ["PD", "ADMIN"])
    return db.query(models.Expense).filter(models.Expense.status == models.ExpenseStatus.SUBMITTED).all()

@router.get("/pending-l2", response_model=List[schemas.ExpenseResponse])
def get_pending_l2(db: Session = Depends(get_db), current_user: models.User = Depends(auth.get_current_user)):
    require_roles(current_user, ["ADMIN"])
    return db.query(models.Expense).filter(models.Expense.status == models.ExpenseStatus.PENDING_L1).all()

@router.get("/pending-payment", response_model=List[schemas.ExpenseResponse])
def get_pending_payment(db: Session = Depends(get_db), current_user: models.User = Depends(auth.get_current_user)):
    require_roles(current_user, ["PD", "ADMIN"])
    # "APPROVED_L2" means Admin validated, now waiting for PD to pay
    return db.query(models.Expense).filter(models.Expense.status == models.ExpenseStatus.APPROVED_L2).all()

@router.get("/{id}", response_model=schemas.ExpenseResponse)
def get_expense_details(
    id: int, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """Get single expense details."""
    expense = db.query(models.Expense).get(id)
    if not expense:
        raise HTTPException(status_code=404, detail="Expense not found")
    
    # Security: PM sees own, PD/Admin sees all, SBC sees if beneficiary
    is_owner = expense.requester_id == current_user.id
    is_admin_pd = current_user.role in [models.UserRole.ADMIN, models.UserRole.PD]
    is_beneficiary = expense.beneficiary_user_id == current_user.id

    if not (is_owner or is_admin_pd or is_beneficiary):
        raise HTTPException(status_code=403, detail="Not authorized to view this expense.")
        
    return expense


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
    db: Session = Depends(get_db), 
    current_user: models.User = Depends(auth.get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """PD Approval"""
    require_roles(current_user, [models.UserRole.PD, models.UserRole.ADMIN])
    try:
        return crud.approve_expense_l1(db, id, current_user.id, background_tasks)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/{id}/approve-l2", response_model=schemas.ExpenseResponse)
def approve_l2(
    id: int, 
    db: Session = Depends(get_db), 
    current_user: models.User = Depends(auth.get_current_user),
):
    """Admin Approval"""
    require_roles(current_user, [models.UserRole.ADMIN])
    try:
        return crud.approve_expense_l2(db, id, current_user.id)
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
    
    # We use a custom delete logic if it's draft, or reject logic if submitted
    # But standard reject is fine for audit trail
    # Note: You need to implement reject_expense in crud to handle refunding 'reserved_balance'
    # For now assuming you have it or will add it.
    
    # Simple implementation:
    exp = db.query(models.Expense).get(id)
    if not exp: raise HTTPException(404, "Not found")
    
    # Refund logic (simplified here, ideally move to CRUD)
    caisse = db.query(models.Caisse).filter(models.Caisse.user_id == exp.requester_id).first()
    if caisse:
        caisse.reserved_balance -= exp.amount
        caisse.balance += exp.amount
        
    exp.status = models.ExpenseStatus.REJECTED
    exp.rejection_reason = payload.reason
    db.commit()
    return exp


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
    payload: dict = Body(...), # Expects {"attachment": "url"}
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """PD confirms cash handover. Money leaves Reserved forever."""
    require_roles(current_user, [models.UserRole.PD, models.UserRole.ADMIN])
    
    attachment = payload.get("attachment") # Optional now, but triggers reminders if missing
    
    try:
        crud.confirm_expense_payment(db, id, attachment)
        return {"message": "Payment confirmed. Reserved funds deducted."}
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
        crud.acknowledge_payment(db, id, current_user.id)
        return {"message": "Receipt acknowledged."}
    except ValueError as e:
        raise HTTPException(403, str(e))


# ==========================
# 4. ADMIN & STATS
# ==========================
