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

# @router.post("/", response_model=schemas.Project)
# def create_new_project(project: schemas.ProjectCreate, db: Session = Depends(get_db)):
#     db_project = crud.get_project_by_name(db, name=project.name)
#     if db_project:
#         raise HTTPException(status_code=400, detail="Project with this name already exists")
#     return crud.create_project(db=db, project=project)

# @router.get("/", response_model=List[schemas.Project]) # Use List from typing
# def read_projects(
#     skip: int = 0, 
#     limit: int = 100, 
#     db: Session = Depends(get_db),
#     # --- THIS IS THE KEY CHANGE ---
#     current_user: models.User = Depends(auth.get_current_user)
# ):
#     """
#     Retrieve a list of projects.
#     Only accessible to authenticated users.
#     """
#     projects = crud.get_projects(db, skip=skip, limit=limit)
#     return projects

# @router.get("/all", response_model=List[schemas.Project])
# def read_all_projects(db: Session = Depends(get_db)):
#     """
#     Retrieve ALL projects without pagination.
#     Ideal for populating filter dropdowns on the frontend.
#     """
#     # We will use the existing crud.get_projects but without a limit.
#     # We can set a very high limit or create a new specific crud function.
#     # Let's create a new CRUD function for clarity.
#     return crud.get_all_projects(db=db)


@router.get("/all-internal", response_model=List[schemas.InternalProject])
def read_all_internal_projects(db: Session = Depends(get_db)):
    """
    Retrieve ALL projects without pagination.
    Ideal for populating filter dropdowns on the frontend.
    """
    # We will use the existing crud.get_projects but without a limit.
    # We can set a very high limit or create a new specific crud function.
    # Let's create a new CRUD function for clarity.
    return crud.get_all_internal_projects(db=db)
@router.post("/internal", response_model=schemas.InternalProject)
def create_internal_project(
    project: schemas.InternalProjectCreate, 
    db: Session = Depends(get_db)
):
    # 1. Check for duplicates
    db_project = crud.get_internal_project_by_name(db, name=project.name)
    if db_project:
        raise HTTPException(status_code=400, detail="Internal Project with this name already exists")
    
    # 2. Create
    return crud.create_internal_project(db=db, project=project)

@router.get("/internal/{project_id}", response_model=schemas.InternalProject)
def read_internal_project(project_id: int, db: Session = Depends(get_db)):
    db_project = crud.get_internal_project(db, project_id=project_id)
    if db_project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return db_project

@router.get("/internal/{project_id}/sites", response_model=List[schemas.Site])
def read_internal_project_sites(project_id: int, db: Session = Depends(get_db)):
    """
    Returns all sites currently handled by this Internal Project.
    """
    return crud.get_sites_for_internal_project(db, project_id=project_id)


# @router.get("/{project_id}", response_model=schemas.Project)
# def read_project(
#     project_id: int, 
#     db: Session = Depends(get_db),
#     current_user: models.User = Depends(auth.get_current_user) # Optionally protect this too
# ):
#     db_project = crud.get_project(db, project_id=project_id)
#     if db_project is None:
#         raise HTTPException(status_code=404, detail="Project not found")
#     return db_project

# from starlette import status # Add this import

# @router.delete("/{project_id}", status_code=status.HTTP_204_NO_CONTENT)
# def delete_project_endpoint(
#     project_id: int,
#     db: Session = Depends(get_db),
#     current_user: models.User = Depends(auth.get_current_user)
# ):
#     db_project = crud.get_project(db, project_id=project_id)
#     if db_project is None:
#         raise HTTPException(status_code=404, detail="Project not found")
    
#     crud.delete_project(db=db, project_id=project_id)
#     return {"ok": True} # Or just an empty response with 204

@router.post("/site-rules", response_model=dict) # Change response model to return stats
def create_site_assignment_rule(
    rule: schemas.SiteAssignmentRuleCreate, 
    db: Session = Depends(get_db)
):
    # 1. Create and Save the Rule
    db_rule = models.SiteAssignmentRule(**rule.model_dump())
    db.add(db_rule)
    db.commit()
    db.refresh(db_rule)
    
    # 2. TRIGGER RE-EVALUATION
    # Immediately apply this rule to existing TBD data
    affected_rows = crud.apply_rule_retrospective(db, db_rule)
    
    return {
        "message": "Rule created successfully",
        "rule_id": db_rule.id,
        "affected_existing_pos": affected_rows # Tell the frontend how many items moved
    }
@router.post("/assign-site", response_model=dict)
def assign_site_to_internal_project(
    allocation_data: schemas.SiteAllocationCreate, 
    db: Session = Depends(get_db)
):
    """
    Manually assigns a Site to an Internal Project.
    Applies GLOBALLY to all POs with this Site ID.
    """
    
    # 1. Helper: We need a valid customer_project_id to satisfy the DB constraint.
    # We grab the first one associated with this site from the MergedPO table.
    # If the site is new and has no POs, we can't assign it yet (or use a placeholder).
    sample_po = db.query(models.MergedPO).filter(
        models.MergedPO.site_id == allocation_data.site_id
    ).first()
    
    # Fallback ID if no POs exist (Use ID 1 or handle error)
    # Ideally, you only assign sites that have data.
    cust_proj_id = sample_po.customer_project_id if sample_po else 1 

    # 2. Update or Create the Allocation Record
    existing_allocation = db.query(models.SiteProjectAllocation).filter(
        models.SiteProjectAllocation.site_id == allocation_data.site_id
    ).first()

    if existing_allocation:
        existing_allocation.internal_project_id = allocation_data.internal_project_id
    else:
        new_allocation = models.SiteProjectAllocation(
            site_id=allocation_data.site_id,
            internal_project_id=allocation_data.internal_project_id,
            customer_project_id=cust_proj_id # Just to satisfy the DB column
        )
        db.add(new_allocation)
    
    db.commit()

    # 3. GLOBAL UPDATE: Move all POs for this site to the new Project
    updated_rows = db.query(models.MergedPO).filter(
        models.MergedPO.site_id == allocation_data.site_id
    ).update(
        {models.MergedPO.internal_project_id: allocation_data.internal_project_id},
        synchronize_session=False
    )
    
    db.commit()

    return {
        "message": "Site globally assigned successfully", 
        "records_updated": updated_rows
    }
