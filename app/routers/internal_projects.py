# backend/app/routers/internal_projects.py

from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

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