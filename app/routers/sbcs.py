from fastapi import APIRouter, BackgroundTasks, Depends, UploadFile, File, Form, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from .. import crud, schemas, auth, models
from ..dependencies  import get_db,require_admin

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
@router.get("/all", response_model=List[schemas.SBCResponse])
def get_all_sbcs_list(
    db: Session = Depends(get_db),
    search: Optional[str] = None,
    current_user: models.User = Depends(auth.get_current_user)
):
    return crud.get_all_sbcs(db, search=search)

@router.post("/{sbc_id}/approve")
async def approve_sbc_endpoint(
    sbc_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_admin),
):
    return await crud.approve_sbc(db, sbc_id, current_user.id, background_tasks)

@router.post("/{sbc_id}/reject")
def reject_sbc_endpoint(
    sbc_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    return crud.reject_sbc(db, sbc_id)

@router.get("/{sbc_id}", response_model=schemas.SBCResponse)
def get_sbc_by_id(
    sbc_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    sbc = crud.get_sbc_by_id(db, sbc_id)

    if not sbc:
        raise HTTPException(status_code=404, detail="SBC not found")

    return sbc

@router.put("/{sbc_id}", response_model=schemas.SBCResponse)
def update_sbc(
    sbc_id: int,
    short_name: Optional[str] = Form(None),
    name: Optional[str] = Form(None),
    ceo_name: Optional[str] = Form(None),
    email: Optional[str] = Form(None),
    rib: Optional[str] = Form(None),
    bank_name: Optional[str] = Form(None),
    tax_reg_end_date: Optional[str] = Form(None),
    contract_file: Optional[UploadFile] = File(None),
    tax_file: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    return crud.update_sbc(
        db, sbc_id,
        {
            "short_name": short_name,
            "name": name,
            "ceo_name": ceo_name,
            "email": email,
            "rib": rib,
            "bank_name": bank_name,
            "tax_reg_end_date": tax_reg_end_date,
        },
        contract_file,
        tax_file,
        current_user.id
    )
