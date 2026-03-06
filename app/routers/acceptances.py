# in app/routers/acceptances.py
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Query
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import or_
from .. import crud, models, schemas, auth
from ..dependencies import get_db
from fastapi.responses import StreamingResponse
import io
import pandas as pd
from datetime import datetime

router = APIRouter(prefix="/api/acceptances", tags=["Acceptance Management"])

@router.get("/all", response_model=List[schemas.ServiceAcceptance])
def list_all_acceptances(
    search: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Returns a list of all Service Acceptances filtered by user role and project assignments.
    Injects user_permissions for frontend action control.
    """
    return crud.get_all_acts(db, current_user, search=search)

@router.post("/generate", response_model=schemas.ServiceAcceptance)
def generate_act(
    payload: schemas.ACTGenerationRequest, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Creates a Service Acceptance (ACT) record for selected BC items.
    Checks for ACT_GENERATE permission via Project Matrix.
    """
    try:
        # Pass creator_id for permission check
        return crud.generate_act_record(db, payload.bc_id, current_user.id, payload.item_ids)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/sbc", response_model=List[schemas.ServiceAcceptance])
def get_my_acceptances(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    SBC View: Returns all ACTs belonging to the logged-in SBC user.
    """
    if current_user.role != "SBC" or not current_user.sbc_id:
        raise HTTPException(status_code=403, detail="Only SBC users can access this endpoint.")
    
    # Reuses the secure get_all_acts logic which handles SBC filtering
    return crud.get_all_acts(db, current_user)

@router.post("/item/{item_id}/validate")
def validate_item(
    item_id: int, 
    payload: schemas.ValidationPayload, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Validates a BC item (QC, PM, or PD approval).
    Checks for ACT_APPROVE_RQC, ACT_APPROVE_PM, or ACT_APPROVE_PD via Project Matrix.
    """
    try:
        return crud.validate_bc_item(db, item_id, current_user, payload.action, payload.comment)
    except ValueError as e:
        # Important: detailed error for frontend toast
        raise HTTPException(status_code=403, detail=str(e))

@router.get("/export/excel")
def export_acts_to_excel(
    format: str = "details", 
    search: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Exports Acceptance Certificates to Excel with security filtering.
    """
    df = crud.get_acceptance_export_dataframe(db, current_user, format, search)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Acceptance Export')
        
        worksheet = writer.sheets['Acceptance Export']
        for i, col in enumerate(df.columns):
            column_len = max(df[col].astype(str).str.len().max(), len(col)) + 2
            worksheet.set_column(i, i, min(column_len, 50))

    output.seek(0)
    
    filename = f"ACT_Export_{format}_{datetime.now().strftime('%Y%m%d')}.xlsx"
    headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
    return StreamingResponse(
        output, 
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers=headers
    )

@router.get("/{act_id}", response_model=schemas.ServiceAcceptance)
def get_act_details(
    act_id: int, 
    db: Session = Depends(get_db), 
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Fetches details for a single ACT with visibility security.
    """
    # Reuse the list logic with an ID filter for consistent security injection
    # Or implement a dedicated crud.get_act_by_id that injects perms
    act = db.query(models.ServiceAcceptance).options(
        joinedload(models.ServiceAcceptance.bc),
        joinedload(models.ServiceAcceptance.creator),
        joinedload(models.ServiceAcceptance.items)
    ).filter(models.ServiceAcceptance.id == act_id).first()

    if not act:
        raise HTTPException(status_code=404, detail="Acceptance not found")

    # Security check: reuse the same logic as list_all_acceptances but for one record
    # Simplified check for detail view
    is_admin = current_user.role == models.UserRole.ADMIN
    if not is_admin:
        # Check if user is linked to the project in any capacity
        is_assigned = db.query(models.ProjectWorkflow).filter(
            models.ProjectWorkflow.project_id == act.bc.project_id,
            or_(
                models.ProjectWorkflow.primary_users.any(id=current_user.id),
                models.ProjectWorkflow.support_users.any(id=current_user.id)
            )
        ).first()
        
        if not is_assigned and act.creator_id != current_user.id:
            # Check SBC ownership
            if current_user.role == "SBC" and act.bc.sbc_id == current_user.sbc_id:
                pass
            else:
                raise HTTPException(status_code=403, detail="Access Denied: You are not assigned to this project.")

    # Inject permissions manually for single record
    if is_admin:
        act.user_permissions = [e.value for e in models.ProjectActionType]
    else:
        workflows = db.query(models.ProjectWorkflow).filter(
            models.ProjectWorkflow.project_id == act.bc.project_id,
            or_(
                models.ProjectWorkflow.primary_users.any(id=current_user.id),
                models.ProjectWorkflow.support_users.any(id=current_user.id)
            )
        ).all()
        act.user_permissions = [w.action_type.value if hasattr(w.action_type, 'value') else w.action_type for w in workflows]

    return act
