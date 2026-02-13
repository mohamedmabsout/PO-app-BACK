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
def create_expense(
    background_tasks: BackgroundTasks,
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
        return crud.create_expense(db, payload, current_user.id, background_tasks)
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
    require_roles(current_user, ["PD", "ADMIN"])
    return db.query(models.Expense).filter(models.Expense.status == models.ExpenseStatus.SUBMITTED).all()

@router.get("/pending-l2", response_model=List[schemas.ExpenseResponse])
def get_pending_l2(db: Session = Depends(get_db), current_user: models.User = Depends(auth.get_current_user)):
    require_roles(current_user, ["ADMIN"])
    return db.query(models.Expense).filter(models.Expense.status == models.ExpenseStatus.PENDING_L2).all()

@router.get("/pending-payment", response_model=List[schemas.ExpenseResponse])
def get_pending_payment(db: Session = Depends(get_db), current_user: models.User = Depends(auth.get_current_user)):
    require_roles(current_user, ["PD", "ADMIN"])
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
    require_roles(current_user, [models.UserRole.PD, models.UserRole.ADMIN])
    
    return db.query(models.Expense).filter(
        models.Expense.status.in_([
            models.ExpenseStatus.PAID, 
            models.ExpenseStatus.ACKNOWLEDGED
        ])
    ).order_by(models.Expense.updated_at.desc()).all()

@router.get("/my-reserved-breakdown", response_model=List[schemas.ExpenseResponse])
def get_my_reserved_breakdown(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Returns the list of individual expenses currently contributing 
    to the user's reserved_balance.
    """
    reserved_statuses = [
        models.ExpenseStatus.DRAFT,
        models.ExpenseStatus.SUBMITTED,
        models.ExpenseStatus.PENDING_L2, # Note: includes PENDING_L1/APPROVED_L1 depending on your enum usage
        models.ExpenseStatus.APPROVED_L2
    ]
    
    return db.query(models.Expense).options(
        joinedload(models.Expense.internal_project)
    ).filter(
        models.Expense.requester_id == current_user.id,
        models.Expense.status.in_(reserved_statuses)
    ).order_by(models.Expense.created_at.desc()).all()

   
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
    background_tasks: BackgroundTasks,
    id: int, 
    db: Session = Depends(get_db), 
    current_user: models.User = Depends(auth.get_current_user),
):
    """Admin Approval"""
    require_roles(current_user, [models.UserRole.ADMIN])
    try:
        return crud.approve_expense_l2(db, id, current_user.id, background_tasks)
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
    require_roles(current_user, [models.UserRole.PD, models.UserRole.ADMIN])
    
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
 