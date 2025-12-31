# in app/routers/projects.py
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.orm import Session, joinedload,Query
from sqlalchemy import func
from typing import List, Optional
from datetime import date
from .. import auth, models
import pandas as pd
from io import BytesIO

from .. import crud, schemas
from ..dependencies import get_db, get_current_user, require_admin, require_management
from ..schemas import SiteCodeList, MergedPOSimple

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
def read_all_internal_projects(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user) # Anyone logged in can try
):
    """
    Retrieve ALL projects without pagination.
    Ideal for populating filter dropdowns on the frontend.
    """
    # We will use the existing crud.get_projects but without a limit.
    # We can set a very high limit or create a new specific crud function.
    # Let's create a new CRUD function for clarity.
    return crud.get_internal_projects_for_user(db, current_user)

@router.post("/internal", response_model=schemas.InternalProject)
def create_internal_project(
    project: schemas.InternalProjectCreate, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_admin) # <--- STOP PMs HERE
):
    # 1. Check for duplicates
    db_project = crud.get_internal_project_by_name(db, name=project.name)
    if db_project:
        raise HTTPException(status_code=400, detail="Internal Project with this name already exists")
    
    # 2. Create
    return crud.create_internal_project(db, project)

@router.get("/internal/{project_id}", response_model=schemas.InternalProject)
def read_internal_project(project_id: int, db: Session = Depends(get_db)):
    db_project = crud.get_internal_project(db, project_id=project_id)
    if db_project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return db_project

@router.get("/internal/{project_id}/sites", response_model=schemas.PageMergedPO) # Use Pagination Schema
def read_internal_project_sites(
    project_id: int,
    page: int = 1,
    size: int = 50,
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    return crud.get_sites_for_internal_project_paginated(db, project_id, page, size, search)

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
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_admin)
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
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_admin)
):
    """
    Manually assigns a Site to an Internal Project.
    Applies GLOBALLY to all POs with this Site ID.
    """
    
    # 1. Helper: We need a valid customer_project_id to satisfy the DB constraint.
    # We grab the first one associated with this site from the MergedPO table.
    # If the site is new and has no POs, we can't assign it yet (or use a placeholder).
    target_project_id = allocation_data.internal_project_id
    site_id = allocation_data.site_id

    # 1. Update/Create Site Allocation Record (Single Source of Truth)
    # Use a placeholder customer_project_id if needed, just to satisfy constraint
    sample_po = db.query(models.MergedPO).filter(models.MergedPO.site_id == site_id).first()
    cust_proj_id = sample_po.customer_project_id if sample_po else 1

    existing_allocation = db.query(models.SiteProjectAllocation).filter(
        models.SiteProjectAllocation.site_id == site_id
    ).first()

    if existing_allocation:
        existing_allocation.internal_project_id = target_project_id
    else:
        new_allocation = models.SiteProjectAllocation(
            site_id=site_id,
            internal_project_id=target_project_id,
            customer_project_id=cust_proj_id
        )
        db.add(new_allocation)
    
    db.commit()

    # 2. GLOBAL UPDATE: Update MergedPO records
    # Set them to PENDING_APPROVAL so the PM sees them.
    # Note: We update internal_project_id so the PM knows which project it's intended for.
    updated_rows = db.query(models.MergedPO).filter(
        models.MergedPO.site_id == site_id
    ).update({
        "internal_project_id": target_project_id,
        "assignment_status": models.AssignmentStatus.PENDING_APPROVAL,
        "assignment_date": datetime.now()
    }, synchronize_session=False)
    
    db.commit()

    # 3. Notification Logic
    target_project = db.query(models.InternalProject).get(target_project_id)
    if target_project and target_project.project_manager_id:
        crud.create_notification(
            db, 
            recipient_id=target_project.project_manager_id,
            type=models.NotificationType.TODO,
            title="Site Assignment Request",
            message=f"Site {site_id} assigned to '{target_project.name}'. Please review.",
            link="/projects/approvals"
        )
        db.commit() # Commit notification

    return {
        "message": "Site assigned successfully (Pending PM Approval)", 
        "records_updated": updated_rows
    }

@router.post("/assign-sites-bulk")
def assign_sites_bulk(
    payload: schemas.BulkSiteAssignment,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_admin)
):
    result = crud.bulk_assign_sites(db, payload.site_ids, payload.internal_project_id, current_user)
    
    if result["updated"] == 0 and result["skipped"] > 0:
        return {"message": "Warning: No sites updated. All selected sites are already assigned to other projects.", "details": result}
        
    return {"message": "Success", "details": result}
@router.get("/site-rules", response_model=List[schemas.SiteAssignmentRule])
def get_site_assignment_rules(db: Session = Depends(get_db)):
    """
    Fetch all active site assignment rules.
    """
    return db.query(models.SiteAssignmentRule).all()

@router.put("/internal/{project_id}", response_model=schemas.InternalProject)
def update_internal_project(
    project_id: int, 
    project: schemas.InternalProjectUpdate, 
    db: Session = Depends(get_db)
):
    updated_project = crud.update_internal_project(db, project_id=project_id, updates=project)
    if updated_project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return updated_project

@router.post("/merged-pos/search-by-sites", response_model=List[schemas.MergedPO])
def search_by_sites_batch(
    payload: schemas.BatchSearchRequest,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):


    # On récupère la liste de codes depuis le modèle Pydantic
    raw_codes = payload.site_codes or []

    # Nettoyage basique (trim, dédoublonnage, suppression des vides)
    clean_codes = sorted(
        set(c.strip() for c in raw_codes if c and c.strip())
    )

    if not clean_codes:
        return []

    # Appel au CRUD qui fait la vraie requête SQLAlchemy
    results = crud.search_merged_pos_by_site_codes(
        db=db,
        site_codes=clean_codes,
        start_date=payload.start_date,
        end_date=payload.end_date,
    )

    return results
@router.post("/site-dispatcher/upload")
async def upload_site_dispatcher_excel(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    # 1. Vérifier le type de fichier
    if not (file.filename.endswith(".xlsx") or file.filename.endswith(".xls")):
        raise HTTPException(
            status_code=400,
            detail="Veuillez uploader un fichier Excel (.xlsx ou .xls).",
        )

    try:
        content = await file.read()
        df = pd.read_excel(BytesIO(content))

        # 2. On veut AU MOINS une colonne "Site Code"
        required_cols = ["Site Code"]
        for col in required_cols:
            if col not in df.columns:
                raise HTTPException(
                    status_code=400,
                    detail=f"Colonne manquante dans le fichier Excel: {col}",
                )

        processed = 0
        errors = 0
        error_rows: list[dict] = []

        # 3. Boucle sur chaque ligne => dispatch par site_code
        for idx, row in df.iterrows():
            try:
                site_code = str(row["Site Code"]).strip()
                if not site_code or site_code.lower() == "nan":
                    continue

                # 🧠 LOGIQUE DE DISPATCH PAR SITE CODE
                crud.dispatch_site_by_code(
                    db=db,
                    site_code=site_code,
                    user_id=current_user.id,
                )

                processed += 1

            except Exception as e:
                errors += 1
                error_rows.append(
                    {"row": int(idx) + 2, "site_code": site_code, "error": str(e)}
                )

        db.commit()

        return {
            "processed": processed,
            "errors": errors,
            "error_rows": error_rows,
        }

    except HTTPException:
        raise
    except Exception as e:
        print("Dispatch Excel error:", e)

@router.get("/pending-approvals", response_model=List[schemas.MergedPO]) # Use appropriate schema
def get_my_pending_sites(db: Session = Depends(get_db), current_user: models.User = Depends(auth.get_current_user)):
    return crud.get_pending_sites_for_pm(db, current_user.id)

@router.post("/review-assignments")
def review_assignments(
    payload: schemas.ReviewPayload,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    count = crud.process_assignment_review(db, payload.merged_po_ids, payload.action, current_user)
    return {"message": f"Successfully processed {count} sites."}
