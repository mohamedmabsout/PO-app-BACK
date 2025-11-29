from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from .. import crud, schemas, auth, models
from ..dependencies import get_db

router = APIRouter(prefix="/api/targets", tags=["targets"])

@router.post("/", response_model=schemas.UserTargetCreate)
def set_target(
    target: schemas.UserTargetCreate, 
    db: Session = Depends(get_db),
    # Only Admins should theoretically set targets, but we check logic in frontend for now
    current_user: models.User = Depends(auth.get_current_user) 
):
    crud.set_user_target(db, target)
    return target

@router.get("/matrix", response_model=List[schemas.PerformanceMatrixRow])
def get_matrix(
    year: int,
    month: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    return crud.get_performance_matrix(db, year, month)