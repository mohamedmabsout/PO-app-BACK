# backend/app/routers/internal_projects.py

import pandas as pd
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
import io
from sqlalchemy.orm import Session, joinedload
from starlette.responses import StreamingResponse

from .. import crud, models, schemas, auth
from ..dependencies import get_db

# Assuming your router prefix is defined like this:
router = APIRouter(prefix="/api/internal-projects", tags=["internal-projects"])

# --- 1. GET MATRIX CONFIGURATION ---
@router.get("/{id}/workflow", response_model=List[schemas.WorkflowConfigOut])
def get_project_workflow_matrix(
    id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Fetch the workflow configuration (Primary/Support users) for a specific project.
    """
    # Security: Allow any internal staff to view, or restrict to Stakeholders/Admins
    # For now, let's allow all authenticated users (read-only)
    
    project = db.query(models.InternalProject).get(id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    return crud.get_project_workflow_matrix(db, id)


# --- 2. UPDATE MATRIX CONFIGURATION ---
@router.post("/{id}/workflow")
def update_project_workflow_matrix(
    id: int,
    payload: schemas.ProjectMatrixUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    if current_user.role != models.UserRole.ADMIN:
        # Check if user is the PD of this project using the NEW check
        if not crud.check_workflow_permission(db, id, "ROLE_PD", current_user.id):
            raise HTTPException(status_code=403, detail="Unauthorized")

    crud.update_project_workflow_matrix(db, id, payload.configs)
    return {"message": "Configuration updated"}

@router.get("/{id}/workflow/export")
def export_project_workflow(
    id: int, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    # Security: Admin/RAF can export any. Others only if assigned to project.
    if current_user.role not in [models.UserRole.ADMIN, models.UserRole.RAF]:
        if not crud.check_workflow_permission(db, id, models.ProjectActionType.ROLE_PM, current_user.id):
             raise HTTPException(status_code=403, detail="Unauthorized")

    # Fetch workflows for ONLY this project
    workflows = db.query(models.ProjectWorkflow).options(
        joinedload(models.ProjectWorkflow.primary_users),
        joinedload(models.ProjectWorkflow.support_users)
    ).filter(models.ProjectWorkflow.project_id == id).all()
    
    # 2. Flatten for Excel
    data = []
    for w in workflows:
        data.append({
            "Action": w.action_type.value,
            "Primary Users": ", ".join([f"{u.first_name} {u.last_name}" for u in w.primary_users]),
            "Support Users": ", ".join([f"{u.first_name} {u.last_name}" for u in w.support_users])
        })
    
    # 3. Create DataFrame and Stream
    df = pd.DataFrame(data)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Project Workflow')
    output.seek(0)
    
    return StreamingResponse(
        output,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename="Workflow_Project_{id}.xlsx"'}
    )


@router.get("/export/workflow-matrix")
def export_workflow_matrix(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    # Security: Admins only
    if current_user.role != models.UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Admin only")

    # Fetch all workflows with project and user info
    workflows = db.query(models.ProjectWorkflow).options(
        joinedload(models.ProjectWorkflow.project),
        joinedload(models.ProjectWorkflow.primary_users),
        joinedload(models.ProjectWorkflow.support_users)
    ).all()
    
    data = []
    for w in workflows:
        data.append({
            "Project": w.project.name,
            "Action Type": w.action_type.value,
            "Primary Users": ", ".join([f"{u.first_name} {u.last_name}" for u in w.primary_users]),
            "Support Users": ", ".join([f"{u.first_name} {u.last_name}" for u in w.support_users])
        })
    
    df = pd.DataFrame(data)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Workflow Matrix')
        
    output.seek(0)
    return StreamingResponse(
        output,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': 'attachment; filename="Full_Workflow_Matrix.xlsx"'}
    )