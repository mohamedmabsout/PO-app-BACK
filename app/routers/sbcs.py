from datetime import date
from fastapi import APIRouter, BackgroundTasks, Depends, UploadFile, File, Form, HTTPException
from fastapi_mail import MessageSchema, MessageType, FastMail, ConnectionConfig
from sqlalchemy.orm import Session
from typing import List, Optional
from .. import crud, schemas, auth, models
from ..dependencies  import get_db,require_admin
from ..config import conf
import secrets
router = APIRouter(prefix="/api/sbcs", tags=["SBC Management"])
@router.post("/", response_model=schemas.SBCResponse)
def create_new_sbc(
    sbc_code: Optional[str] = Form(None),
    sbc_type: Optional[str] = Form(None),
    short_name: str = Form(...),
    name: str = Form(...),
    start_date: Optional[date] = Form(None),
    ceo_name: Optional[str] = Form(None),
    email: Optional[str] = Form(None),
    rib: Optional[str] = Form(None),
    bank_name: Optional[str] = Form(None),
    tax_reg_end_date: Optional[str] = Form(None),
    # Files
    contract_file: Optional[UploadFile] = File(None),
    tax_file: Optional[UploadFile] = File(None),

    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    
    form_data = {
        "sbc_code": sbc_code,
        "sbc_type": sbc_type,
        "short_name": short_name,
        "name": name,
        "start_date": start_date,
        "ceo_name": ceo_name,
        "email": email,
        "rib": rib,
        "bank_name": bank_name,
        "tax_reg_end_date": tax_reg_end_date
    }

    # 👉 ici: save files + CRUD create
    return crud.sbc.create(
        db=db,
        data=form_data,
        contract_file=contract_file,
        tax_file=tax_file,
        created_by=current_user.id,
    )

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

@router.get("/my-kpis", response_model=schemas.SBCKpiSummary)
def get_my_sbc_kpis(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    return crud.get_sbc_kpis(db, user=current_user)

@router.get("/my-acceptances", response_model=List[schemas.SBCAcceptance])
def get_my_sbc_acceptances(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    return crud.get_sbc_acceptances(db, user=current_user)

@router.post("/{sbc_id}/approve")
async def approve_sbc_endpoint(
    sbc_id: int,
    background_tasks: BackgroundTasks, # <--- Import this
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user) # Assuming Admin/PD
):
    try:
        # Call the updated CRUD
        sbc, new_user = crud.approve_sbc(db, sbc_id, current_user.id)
        
        # If a new user was created, send the Invite Email
        if new_user and new_user.reset_token:
            
            reset_link = f"https://po.sib.co.ma/reset-password?token={new_user.reset_token}"
            
            html = f"""
            <h3>SBC Approval Notification</h3>
            <p>Hello {new_user.first_name},</p>
            <p>Your Sub-Contractor account ({sbc.short_name}) has been <strong>APPROVED</strong>.</p>
            <p>A user account has been created for you. Please click the link below to set your password and access the portal:</p>
            <br>
            <a href="{reset_link}" style="padding: 10px 20px; background-color: #007bff; color: white; text-decoration: none; border-radius: 5px;">Set Password</a>
            <br><br>
            <p>Or copy this link: {reset_link}</p>
            """

            message = MessageSchema(
                subject="SIB Portal - Account Approved",
                recipients=[new_user.email],
                body=html,
                subtype=MessageType.html
            )

            # Send email in background
            fm = FastMail(conf)
            background_tasks.add_task(fm.send_message, message)
            
            return {"message": f"SBC Approved and Invitation Email sent to {new_user.email}"}
        
        return {"message": "SBC Approved (User already existed)"}

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error approving SBC: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
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
     # ✅ Log pour vérifier
    print(f"🔍 SBC ICE: {sbc.ice}")
    print(f"🔍 SBC RC: {sbc.rc}")
    
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
    ice: Optional[str] = Form(None),          # ✅ add
    rc: Optional[str] = Form(None),  
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
            "ice": ice,   # ✅ add
            "rc": rc,
            "tax_reg_end_date": tax_reg_end_date,
        },
        contract_file,
        tax_file,
        current_user.id
    )
