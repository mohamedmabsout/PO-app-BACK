from fastapi import APIRouter, Depends, Query
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
    
    filter_id = None
    
    if current_user.role != UserRole.ADMIN:
        filter_id = current_user.id
        
    return crud.get_performance_matrix(db, year, month, filter_user_id=filter_id)
@router.get("/yearly-matrix", response_model=List[dict]) # Use generic dict or define strict schema
def get_yearly_matrix(year: int, db: Session = Depends(get_db)):
    return crud.get_yearly_matrix_data(db, year)
