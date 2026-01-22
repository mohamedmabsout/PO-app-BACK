# app/routers/expenses.py
import datetime
import io
from fastapi import APIRouter, Depends, HTTPException
from fastapi.temp_pydantic_v1_params import Body
import pandas as pd
from sqlalchemy.orm import Session
from typing import List
from .. import crud, models, schemas, auth
from ..dependencies import get_current_user, get_db
from fastapi.responses import StreamingResponse
router = APIRouter(prefix="/api/expenses", tags=["expenses"])


def require_roles(user, roles):
    # Récupère la valeur du rôle de l'utilisateur (pas l'enum lui-même)
    if isinstance(user.role, models.UserRole):
        user_role_str = user.role.value.upper().strip()
    else:
        user_role_str = str(user.role).upper().strip()
    
    # On prépare la liste des rôles autorisés en majuscules
    allowed_roles = []
    for r in roles:
        if isinstance(r, models.UserRole):
            allowed_roles.append(r.value.upper().strip())
        else:
            allowed_roles.append(str(r).upper().strip())
    
    # LOGIQUE SPÉCIFIQUE : Si on autorise le "PROJECT DIRECTOR", on autorise aussi "PD"
    if "PROJECT DIRECTOR" in allowed_roles and "PD" not in allowed_roles:
        allowed_roles.append("PD")
    if "PD" in allowed_roles and "PROJECT DIRECTOR" not in allowed_roles:
        allowed_roles.append("PROJECT DIRECTOR")
    
    print(f"DEBUG - User role: '{user_role_str}'")
    print(f"DEBUG - Allowed roles: {allowed_roles}")
    
    if user_role_str not in allowed_roles:
        raise HTTPException(
            status_code=403, 
            detail=f"Action interdite pour le rôle : {user_role_str}. Rôles autorisés: {allowed_roles}"
        )

@router.post("/", response_model=schemas.ExpenseOut)
def post_expense(
    payload: schemas.ExpenseCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    require_roles(current_user, [models.UserRole.PM, models.UserRole.COORDINATEUR, models.UserRole.ADMIN, models.UserRole.PD])
    return crud.create_expense(db, current_user, payload)


@router.get("/my-requests", response_model=list[schemas.ExpenseOut])
def get_my_requests(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    return crud.list_my_requests(db, current_user)


@router.get("/pending-l1", response_model=list[schemas.ExpenseOut])
def get_pending_l1(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    # On autorise le PD ou l'Admin
    require_roles(current_user, ["PD", "ADMIN", "PROJECT DIRECTOR"])
    return crud.list_pending_l1(db)

@router.post("/{id}/approve-l1")
def approve_l1(
    id: int, 
    db: Session = Depends(get_db), 
    current_user: models.User = Depends(auth.get_current_user)
):
    # On vérifie que l'utilisateur est soit PD, soit ADMIN
    require_roles(current_user, ["PD", "ADMIN"])
    
    # Appel de la logique de validation avec l'ID de l'approbateur
    expense = crud.approve_expense_l1(db, id, current_user.id)  # Ajoutez current_user.id ici
    
    if not expense:
        raise HTTPException(status_code=404, detail="Dépense non trouvée")
        
    return expense

@router.get("/pending-l2", response_model=list[schemas.ExpenseOut])
def get_pending_l2(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    require_roles(current_user, [models.UserRole.ADMIN, models.UserRole.CEO])
    return crud.list_pending_l2(db)


@router.post("/{id}/approve-l2", response_model=schemas.ExpenseOut)
def post_approve_l2(
    id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    require_roles(current_user, [models.UserRole.ADMIN, models.UserRole.CEO])
    try:
        exp = crud.approve_l2(db, id, current_user)
        if not exp:
            raise HTTPException(404, "Expense not found")
        return exp
    except ValueError as e:
        msg = str(e)
        code = 400 if "Insufficient" in msg else 400
        raise HTTPException(code, msg)


@router.post("/{id}/reject", response_model=schemas.ExpenseOut)
def post_reject(
    id: int,
    payload: schemas.ExpenseReject,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    require_roles(current_user, [models.UserRole.PD, models.UserRole.ADMIN, models.UserRole.CEO])
    exp = crud.reject_expense(db, id, current_user, payload.reason)
    if not exp:
        raise HTTPException(404, "Expense not found")
    return exp
# @router.post("/{id}/confirm-payment")
# def confirm_payment(
    # id: int,
    # payload: schemas.ConfirmPaymentRequest,  # Corps de la requête
   #  db: Session = Depends(get_db),
   #  current_user: models.User = Depends(get_current_user)
# ):
   #  expense = db.query(models.Expense).filter(models.Expense.id == id).first()
    
   #  if not expense:
       #  raise HTTPException(404, "Dépense non trouvée.")
    
    # if not payload.attachment:
        # raise HTTPException(400, "L'attachement est obligatoire pour confirmer le paiement.")
    
    # Mise à jour du statut
   #  expense.status = "PAID"
    # expense.attachment = payload.attachment
    # expense.updated_at = datetime.utcnow()
    
    # try:
       #  db.commit()
        # db.refresh(expense)
        # return {"message": "Paiement confirmé avec succès", "expense": expense}
    # except Exception as e:
        # db.rollback()
        # raise HTTPException(500, f"Erreur lors de la confirmation: {str(e)}")

@router.get("/stats")
def get_expense_stats(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    # Cette fonction renvoie le solde de la caisse (balance)
    return crud.get_caisse_stats(db, current_user)

@router.get("/payable-acts")
def get_acts_for_expense(
    project_id: int,
    db: Session = Depends(get_db)
):
    # Renvoie les ACT approuvés mais non payés pour un projet donné
    return crud.get_payable_acts(db, project_id)

@router.post("/{id}/submit")
def submit_to_pd(id: int, db: Session = Depends(get_db)):
    try:
        return crud.submit_expense(db, id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    
@router.get("/export/excel")
def export_expenses_to_excel(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    # Optionnel: Restreindre l'export aux Admin/PD
    # require_roles(current_user, ["ADMIN", "PD"])

    df = crud.get_expenses_export_dataframe(db)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Petty Cash Details')
        
    output.seek(0)
    
    headers = {
        'Content-Disposition': 'attachment; filename="Extraction_Petty_Cash.xlsx"'
    }
    return StreamingResponse(
        output, 
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers=headers
    )
@router.get("/pending-payment", response_model=list[schemas.ExpenseOut])
def get_pending_payment(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    # Sécurité : Seul le PD (ou Admin) peut voir cette liste de paiement
    require_roles(current_user, ["PD", "PROJECT DIRECTOR", "ADMIN"])
    return crud.list_pending_payment(db)

@router.post("/{id}/confirm-payment")
def confirm_payment(
    id: int, 
    payload: dict = Body(...), # Reçoit {"attachment": "nom_du_fichier.pdf"}
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    require_roles(current_user, ["PD", "PROJECT DIRECTOR"])
    
    attachment = payload.get("attachment")
    if not attachment:
        raise HTTPException(status_code=400, detail="L'attachement est obligatoire")
        
    try:
        return crud.confirm_expense_payment(db, id, attachment)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
@router.get("/wallets-summary", response_model=None) # ✅ Ajoutez response_model=None
def get_wallets_summary(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    # Votre logique de récupération des caisses...
    return crud.get_all_wallets_summary(db)