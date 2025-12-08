from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from .. import crud, schemas, auth, models
from ..dependencies  import get_db

router = APIRouter(prefix="/api/sbcs", tags=["SBC Management"])

@router.post("/", response_model=schemas.SBCResponse)
def create_new_sbc(
    # Use Form(...) for text fields because we are uploading files
    sbc_code: Optional[str] = Form(None),
    short_name: str = Form(...),
    name: str = Form(...),
    start_date: Optional[str] = Form(None),
    ceo_name: Optional[str] = Form(None),
    email: Optional[str] = Form(None),
    rib: Optional[str] = Form(None),
    bank_name: Optional[str] = Form(None),
    tax_reg_end_date: Optional[str] = Form(None),
    
    # Files
    contract_file: Optional[UploadFile] = File(None),
    tax_file: Optional[UploadFile] = File(None),
    
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    # Consolidate form data into a dict for CRUD
    form_data = {
        "sbc_code": sbc_code,
        "short_name": short_name,
        "name": name,
        "start_date": start_date,
        "ceo_name": ceo_name,
        "email": email,
        "rib": rib,
        "bank_name": bank_name,
        "tax_reg_end_date": tax_reg_end_date
    }
    
    return crud.create_sbc(db, form_data, contract_file, tax_file, current_user.id)

@router.get("/pending", response_model=List[schemas.SBCResponse])
def get_pending_sbcs_list(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    # Optional: Check if user is PD or Admin
    return crud.get_pending_sbcs(db)
@router.get("/active", response_model=List[schemas.SBCResponse])
def get_active_sbcs_list(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    return crud.get_active_sbcs(db)

@router.post("/{sbc_id}/approve")
def approve_sbc_endpoint(
    sbc_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    # Ideally check permission here: if current_user.role != PD...
    try:
        return crud.approve_sbc(db, sbc_id, current_user.id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.post("/{sbc_id}/reject")
def reject_sbc_endpoint(
    sbc_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    return crud.reject_sbc(db, sbc_id)
