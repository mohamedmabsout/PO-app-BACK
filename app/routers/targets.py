from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from .. import crud, schemas, auth, models
from ..dependencies import get_db,get_current_user, require_admin
from ..models import UserRole

router = APIRouter(prefix="/api/targets", tags=["targets"])

# 1. SET TARGET: Only ADMIN can do this
# As per your requirement: "the plan column should be insert by the admin"
@router.post("/", response_model=schemas.UserTargetCreate)
def set_target(
    target: schemas.UserTargetCreate, 
    db: Session = Depends(get_db),
    # SECURED: This restricts access strictly to Admins
    current_user: models.User = Depends(require_admin) 
):
    crud.set_user_target(db, target)
    return target

# 2. GET MATRIX: Secured visibility
@router.get("/matrix", response_model=List[schemas.PerformanceMatrixRow])
def get_matrix(
    year: int,
    month: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    # LOGIC:
    # - If Admin: Pass None (See All)
    # - If PM/PD: Pass current_user.id (See Self)
    # - Others: Pass current_user.id (See Self, likely empty if they aren't PMs)
    
    # filter_id = None
    
    # if current_user.role != UserRole.ADMIN:
    #     filter_id = current_user.id
        
    return crud.get_performance_matrix(db, year, month, filter_user_id=None, current_user=current_user)
@router.get("/yearly-matrix", response_model=List[dict]) # Use generic dict or define strict schema
def get_yearly_matrix(year: int, db: Session = Depends(get_db),current_user: models.User = Depends(get_current_user)):
    return crud.get_planning_matrix(db, year,current_user)
@router.get("/planning/matrix/{year}", response_model=List[dict])
def get_planning_matrix_endpoint(year: int, db: Session = Depends(get_db),current_user: models.User = Depends(get_current_user)):
    return crud.get_planning_matrix(db, year,current_user)

@router.post("/planning/update")
def update_target_cell(payload: schemas.TargetUpdate, db: Session = Depends(get_db)):
    # Simple upsert logic
    target = db.query(models.UserPerformanceTarget).filter(
        models.UserPerformanceTarget.user_id == payload.user_id,
        models.UserPerformanceTarget.year == payload.year,
        models.UserPerformanceTarget.month == payload.month
    ).first()
    
    if not target:
        target = models.UserPerformanceTarget(
            user_id=payload.user_id, year=payload.year, month=payload.month
        )
        db.add(target)
    
    # Map frontend field names to DB columns
    if payload.field == "po_master": target.po_master_plan = payload.value
    elif payload.field == "po_update": target.po_monthly_update = payload.value
    elif payload.field == "acc_master": target.acceptance_master_plan = payload.value
    elif payload.field == "acc_update": target.acceptance_monthly_update = payload.value
    else:
        # Safety check for invalid field names
        raise HTTPException(status_code=400, detail=f"Invalid field: {payload.field}")
    
    # 4. Commit changes
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
    # 5. Return a simple success message (Dict, not ORM object)
    return {"status": "success", "updated_field": payload.field, "new_value": payload.value}

