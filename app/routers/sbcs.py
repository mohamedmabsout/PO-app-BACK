from fastapi import APIRouter, BackgroundTasks, Depends, UploadFile, File, Form, HTTPException
from fastapi_mail import MessageSchema, MessageType, FastMail, ConnectionConfig
from sqlalchemy.orm import Session,joinedload
from typing import List, Optional
from .. import crud, schemas, auth, models
from ..dependencies  import get_db,require_admin
from ..config import conf
from fastapi.responses import FileResponse
import os
import secrets
router = APIRouter(prefix="/api/sbcs", tags=["SBC Management"])

@router.post("/", response_model=schemas.SBCResponse)
def create_new_sbc(
    background_tasks: BackgroundTasks,
    short_name: str = Form(...),
    name: str = Form(...),
    sbc_type: str = Form(...),
    sbc_code: Optional[str] = Form(None),
    ceo_name: Optional[str] = Form(None),
    email: Optional[str] = Form(None),
    phone_1: Optional[str] = Form(None),
    phone_2: Optional[str] = Form(None),
    address: Optional[str] = Form(None),
    city: Optional[str] = Form(None),
    rib: Optional[str] = Form(None),
    bank_name: Optional[str] = Form(None),
    ice: Optional[str] = Form(None),
    rc: Optional[str] = Form(None),
    contract_ref: Optional[str] = Form(None),
    tax_reg_end_date: Optional[str] = Form(None),
    start_date: Optional[str] = Form(None),
    contract_file: Optional[UploadFile] = File(None),
    tax_file: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    form_data = {
        "short_name": short_name, "name": name, "sbc_type": sbc_type,
        "sbc_code": sbc_code, "ceo_name": ceo_name, "email": email,
        "phone_1": phone_1, "phone_2": phone_2, "address": address, "city": city,
        "rib": rib, "bank_name": bank_name, "ice": ice, "rc": rc,
        "contract_ref": contract_ref, "tax_reg_end_date": tax_reg_end_date,
        "start_date": start_date
    }
    return crud.create_sbc(db, form_data, contract_file, tax_file, current_user.id, background_tasks)


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

@router.get("/generated-acts")
def get_sbc_generated_acts(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    if current_user.role != "SBC": return []
    
    # Fetch ACTs linked to SBC's BCs
    acts = db.query(models.ServiceAcceptance).join(models.BonDeCommande).options(
        joinedload(models.ServiceAcceptance.bc) # This loads the relationship
    ).filter(
        models.BonDeCommande.sbc_id == current_user.sbc_id
    ).order_by(models.ServiceAcceptance.created_at.desc()).all()

    return acts # Pydantic will serialize
@router.get("/acceptance-pipeline")
def get_sbc_acceptance_pipeline(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    if current_user.role != "SBC": return []

    # Get BCs with items that have started the process
    # Or just get all Approved BCs, as they are the candidates for acceptance
    bcs = db.query(models.BonDeCommande).filter(
        models.BonDeCommande.sbc_id == current_user.sbc_id,
        models.BonDeCommande.status == models.BCStatus.APPROVED
    ).all()
    
    result = []
    for bc in bcs:
        # Calculate stats for this BC
        ready_count = 0
        ready_amount = 0.0
        pending_count = 0
        
        for item in bc.items:
            if item.global_status == models.ItemGlobalStatus.READY_FOR_ACT:
                ready_count += 1
                ready_amount += (item.line_amount_sbc or 0)
            elif item.global_status in [models.ItemGlobalStatus.PENDING_PD_APPROVAL, models.ItemGlobalStatus.PENDING]:
                 pending_count += 1
        
        # Only include if there's activity
        if ready_count > 0 or pending_count > 0:
            result.append({
                "bc_id": bc.id,
                "bc_number": bc.bc_number,
                "project": bc.internal_project.name,
                "ready_count": ready_count,
                "ready_amount": ready_amount,
                "pending_count": pending_count,
                "status": "Ready to Generate" if ready_count > 0 else "In Progress"
            })
            
    return result

@router.get("/my-acceptances")
def read_sbc_acceptances(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    items = crud.get_sbc_acceptances(db, current_user)
    
    result = []
    for item in items:
        # Calculate Tax
        ht_amount = item.line_amount_sbc or 0.0
        tax_rate = item.applied_tax_rate or 0.0
        tax_amount = ht_amount * tax_rate
        ttc_amount = ht_amount + tax_amount

        result.append({
            "id": item.id,
            "bc_number": item.bc.bc_number,
            "po_no": item.merged_po.po_no,
            "site_code": item.merged_po.site_code,
            "description": item.merged_po.item_description,
            "quantity": item.quantity_sbc,
            "unit_price": item.unit_price_sbc,
            
            # --- NEW TAX FIELDS ---
            "total_ht": ht_amount,
            "tax_rate": tax_rate, # e.g. 0.20
            "tax_amount": tax_amount,
            "total_ttc": ttc_amount,
            # ----------------------

            "status": item.global_status,
            "act_number": item.act.act_number if item.act else "Pending Generation"
        })
    return result

@router.get("/act/{act_id}", response_model=schemas.ServiceAcceptanceDetail) 
def get_sbc_act_details(
    act_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    # 1. Fetch ACT with items
    act = db.query(models.ServiceAcceptance).options(
        joinedload(models.ServiceAcceptance.bc).joinedload(models.BonDeCommande.internal_project),
        joinedload(models.ServiceAcceptance.items).joinedload(models.BCItem.merged_po)
    ).filter(models.ServiceAcceptance.id == act_id).first()
    
    if not act:
        raise HTTPException(404, "Acceptance not found")
        
    # 2. Security Check (Crucial!)
    if current_user.role == "SBC":
        # Ensure this ACT belongs to a BC owned by this SBC
        if not act.bc or act.bc.sbc_id != current_user.sbc_id:
            raise HTTPException(403, "Access denied to this Acceptance record.")
            
    return act









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

    return sbc

@router.put("/{sbc_id}", response_model=schemas.SBCResponse)
def update_sbc(
    sbc_id: int,
    background_tasks: BackgroundTasks,
    short_name: Optional[str] = Form(None),
    name: Optional[str] = Form(None),
    sbc_type: Optional[str] = Form(None),
    ceo_name: Optional[str] = Form(None),
    email: Optional[str] = Form(None),
    phone_1: Optional[str] = Form(None),
    phone_2: Optional[str] = Form(None),
    address: Optional[str] = Form(None),
    city: Optional[str] = Form(None),
    rib: Optional[str] = Form(None),
    bank_name: Optional[str] = Form(None),
    ice: Optional[str] = Form(None),
    rc: Optional[str] = Form(None),
    contract_ref: Optional[str] = Form(None),
    tax_reg_end_date: Optional[str] = Form(None),
    contract_file: Optional[UploadFile] = File(None),
    tax_file: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    # Pack into dict
    form_data = {
        "short_name": short_name, "name": name, "sbc_type": sbc_type,
        "ceo_name": ceo_name, "email": email, "phone_1": phone_1,
        "phone_2": phone_2, "address": address, "city": city,
        "rib": rib, "bank_name": bank_name, "ice": ice, "rc": rc,
        "contract_ref": contract_ref, "tax_reg_end_date": tax_reg_end_date
    }
    return crud.update_sbc(db, sbc_id, form_data, contract_file, tax_file, current_user.id, background_tasks)


@router.get("/my-bc-items/{bc_id}", response_model=List[schemas.BCItemResponse])
def get_my_bc_item_details(
    bc_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    # 1. Security check: Does this BC belong to the logged-in SBC?
    bc = db.query(models.BonDeCommande).filter(
        models.BonDeCommande.id == bc_id,
        models.BonDeCommande.sbc_id == current_user.sbc_id
    ).first()
    
    if not bc:
        raise HTTPException(status_code=404, detail="Bon de Commande not found or access denied.")

    # 2. Return items with joined MergedPO data
    return db.query(models.BCItem).options(
        joinedload(models.BCItem.merged_po)
    ).filter(models.BCItem.bc_id == bc_id).all()

@router.get("/download-doc/{filename}")
def download_sbc_document(filename: str):
    # Path to your docs
    file_path = os.path.join("uploads", "sbc_docs", filename)
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found on server")
    
    # Return the file with explicit PDF media type
    return FileResponse(
        path=file_path, 
        media_type='application/pdf', 
        filename=filename
    )


@router.get("/{id}/advance-balance")
def read_sbc_advance_balance(id: int, db: Session = Depends(get_db)):
    balance = crud.get_sbc_unconsumed_balance(db, id)
    return {"sbc_id": id, "total_unconsumed": balance}