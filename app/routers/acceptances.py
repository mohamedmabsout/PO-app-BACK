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

@router.post("/validate-items")
def validate_items(
    payload: schemas.BulkValidationPayload, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Bulk Validates BC items (QC, PM, or PD approval/rejection).
    Checks for permissions via Project Matrix once per BC.
    """
    try:
        return crud.validate_bc_items(
            db, 
            payload.bc_id, 
            payload.item_ids, 
            current_user, 
            payload.action, 
            payload.comment,
            background_tasks
        )
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))
@router.post("/bc/{bc_id}/generate-act")
def generate_act_endpoint(
    bc_id: int,
    payload: schemas.ACTGenerationRequest, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Creates a Service Acceptance (ACT) record for selected BC items.
    Checks for ACT_GENERATE permission via Project Matrix.
    """
    try:
        # Pass creator_id for permission check
        act = crud.generate_act_record(db, bc_id, current_user.id, payload.item_ids, background_tasks)
        # Note: Frontend might expect a file blob if it calls directly, 
        # but the snippet shows it handling blobs if it gets one.
        # If the frontend expects the PDF immediately:
        from ..utils import pdf_generator
        pdf_buffer = pdf_generator.generate_act_pdf(act)
        filename = f"ACT_{act.act_number}.pdf"
        return StreamingResponse(
            pdf_buffer,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/bc/{bc_id}/acts", response_model=List[schemas.ServiceAcceptance])
def get_acts_for_bc(
    bc_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Returns all ACTs generated for a specific BC.
    """
    return db.query(models.ServiceAcceptance).filter(models.ServiceAcceptance.bc_id == bc_id).all()

@router.get("/act/{act_id}/download")
def download_act_pdf(
    act_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Generates and returns the PDF for a specific ACT.
    """
    act = db.query(models.ServiceAcceptance).get(act_id)
    if not act:
        raise HTTPException(status_code=404, detail="ACT not found")
    
    from ..utils import pdf_generator
    pdf_buffer = pdf_generator.generate_act_pdf(act)
    filename = f"ACT_{act.act_number}.pdf"
    return StreamingResponse(
        pdf_buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

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
    current_user: models.User = Depends(auth.get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Validates a single BC item (Legacy/Individual check).
    """
    try:
        return crud.validate_bc_item(db, item_id, current_user, payload.action, payload.comment, background_tasks)
    except ValueError as e:
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
@router.get("/payable_act") # Ensure this is ABOVE @router.get("/{act_id}")
def get_payable_acts_endpoint(
    project_id: Optional[int] = Query(None, description="Filter by project (for PMs creating expenses)"),
    current_expense_id: Optional[int] = Query(None, description="Include acts from current draft"),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Unified endpoint for fetching Payable Acts.
    - PM/Admin/PD use this by passing ?project_id=X to create expenses.
    """
    
    # SCENARIO: PM/Admin fetching acts for a specific project expense
    if project_id is not None:
        return crud.get_payable_acts(db, project_id=project_id, current_expense_id=current_expense_id)
        
    raise HTTPException(status_code=400, detail="You must provide a project_id.")

@router.get("/{act_id}", response_model=schemas.ServiceAcceptance)
def get_act_details(
    act_id: int, 
    db: Session = Depends(get_db), 
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Fetches details for a single ACT with visibility security.
    """
    act = db.query(models.ServiceAcceptance).options(
        joinedload(models.ServiceAcceptance.bc),
        joinedload(models.ServiceAcceptance.creator),
        joinedload(models.ServiceAcceptance.items)
    ).filter(models.ServiceAcceptance.id == act_id).first()

    if not act:
        raise HTTPException(status_code=404, detail="Acceptance not found")

    is_admin = current_user.role == models.UserRole.ADMIN
    if not is_admin:
        is_assigned = db.query(models.ProjectWorkflow).filter(
            models.ProjectWorkflow.project_id == act.bc.project_id,
            or_(
                models.ProjectWorkflow.primary_users.any(id=current_user.id),
                models.ProjectWorkflow.support_users.any(id=current_user.id)
            )
        ).first()
        
        if not is_assigned and act.creator_id != current_user.id:
            if current_user.role == "SBC" and act.bc.sbc_id == current_user.sbc_id:
                pass
            else:
                raise HTTPException(status_code=403, detail="Access Denied: You are not assigned to this project.")

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
