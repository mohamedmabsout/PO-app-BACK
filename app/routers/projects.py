# in app/routers/projects.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from .. import auth, models

from .. import crud, schemas
from ..dependencies import get_db

router = APIRouter(
    prefix="/api/projects",  # All routes in this file will start with /api/projects
    tags=["projects"]        # This groups them nicely in the auto-docs
)

@router.post("/", response_model=schemas.Project)
def create_new_project(project: schemas.ProjectCreate, db: Session = Depends(get_db)):
    db_project = crud.get_project_by_name(db, name=project.name)
    if db_project:
        raise HTTPException(status_code=400, detail="Project with this name already exists")
    return crud.create_project(db=db, project=project)

@router.get("/", response_model=List[schemas.Project]) # Use List from typing
def read_projects(
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db),
    # --- THIS IS THE KEY CHANGE ---
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Retrieve a list of projects.
    Only accessible to authenticated users.
    """
    projects = crud.get_projects(db, skip=skip, limit=limit)
    return projects

@router.get("/all", response_model=List[schemas.Project])
def read_all_projects(db: Session = Depends(get_db)):
    """
    Retrieve ALL projects without pagination.
    Ideal for populating filter dropdowns on the frontend.
    """
    # We will use the existing crud.get_projects but without a limit.
    # We can set a very high limit or create a new specific crud function.
    # Let's create a new CRUD function for clarity.
    return crud.get_all_projects(db=db)


@router.get("/{project_id}", response_model=schemas.Project)
def read_project(
    project_id: int, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user) # Optionally protect this too
):
    db_project = crud.get_project(db, project_id=project_id)
    if db_project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return db_project

from starlette import status # Add this import

@router.delete("/{project_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_project_endpoint(
    project_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    db_project = crud.get_project(db, project_id=project_id)
    if db_project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    
    crud.delete_project(db=db, project_id=project_id)
    return {"ok": True} # Or just an empty response with 204