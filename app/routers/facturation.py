import os
import shutil
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, UploadFile, File, Form
from fastapi.responses import StreamingResponse, FileResponse
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime

from .. import crud, models, schemas, auth
from ..dependencies import get_db
from ..utils.invoice_packer import create_invoice_zip # Our ZIP utility

router = APIRouter(prefix="/api/facturation", tags=["facturation"])

# ==========================================
# 1. SBC ROUTES (Generation & Personal List)
# ==========================================



@router.get("/payable-acts", response_model=List[schemas.PayableActResponse])
def get_sbc_payable_acts(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    if current_user.role != models.UserRole.SBC:
        raise HTTPException(status_code=403, detail="Only SBC users can access this.")
    
    if not current_user.sbc_id:
        raise HTTPException(status_code=400, detail="User not linked to an SBC profile.")

    return crud.get_payable_acts_for_sbc_invoicing(db, current_user.sbc_id)



@router.post("/generate-bundle")
async def generate_facture_bundle(
    payload: schemas.InvoiceCreate, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Creates the invoice record and returns the ZIP bundle."""
    sbc_id = current_user.sbc_id if current_user.role == models.UserRole.SBC else payload.sbc_id
    if not sbc_id:
        raise HTTPException(status_code=400, detail="SBC link required.")

    try:
        # Create record in DB
        new_invoice = crud.create_invoice_bundle(db, sbc_id, payload.act_ids, payload.invoice_number)
        
        # Generate the ZIP in memory
        zip_buffer = create_invoice_zip(new_invoice)
        
        # Notify RAF
        background_tasks.add_task(crud.notify_raf_new_invoice, db, new_invoice, background_tasks)


        filename = f"Payment_File_{new_invoice.invoice_number}.zip"
        return StreamingResponse(
            iter([zip_buffer.getvalue()]), 
            media_type="application/x-zip-compressed",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/my-invoices", response_model=List[schemas.InvoiceListItem])
def get_my_invoices(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    return crud.get_invoices_by_sbc(db, current_user.sbc_id)

# ==========================================
# 2. RAF ROUTES (Verification & Payment)
# ==========================================

@router.get("/all", response_model=List[schemas.InvoiceListItem])
def get_all_invoices(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """RAF view to see every submitted invoice."""
    if current_user.role not in [models.UserRole.RAF, models.UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Unauthorized")
    return crud.get_all_invoices(db)

@router.get("/{id}", response_model=schemas.InvoiceDetail)
def get_invoice_details(id: int, db: Session = Depends(get_db)):
    invoice = crud.get_invoice_by_id(db, id)
    if not invoice: raise HTTPException(404, "Invoice not found")
    return invoice

@router.post("/{id}/verify")
def verify_invoice(
    id: int, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """RAF confirms physical folder received."""
    return crud.verify_invoice_physical(db, id, current_user.id)

@router.post("/{id}/pay")
async def pay_invoice(
    id: int, 
    file: UploadFile = File(...), 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """RAF uploads bank receipt and closes the file."""
    # Save file logic
    upload_dir = "uploads/payments"
    os.makedirs(upload_dir, exist_ok=True)
    filename = f"PAY_{id}_{file.filename}"
    with open(os.path.join(upload_dir, filename), "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
        
    return crud.mark_invoice_paid(db, id, filename)

@router.post("/{id}/reject")
def reject_invoice(
    id: int, 
    payload: schemas.ExpenseReject, # Reuse reason schema
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """RAF rejects invoice. ACTs become payable again."""
    return crud.reject_invoice(db, id, payload.reason)

@router.get("/receipt/{filename}")
def get_payment_receipt(filename: str):
    # Security: Ensure filename is clean to prevent path traversal
    safe_filename = os.path.basename(filename)
    path = f"uploads/payments/{safe_filename}"
    
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
        
    return FileResponse(path)