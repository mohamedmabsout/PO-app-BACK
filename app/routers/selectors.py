# in backend/app/routers/selectors.py
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import List

from .. import models, schemas, auth,crud
from ..dependencies import get_db
from ..enum import UserRole
from ..dependencies import get_current_user


router = APIRouter(
    prefix="/api/selectors",
    tags=["selectors"]
)

# Endpoint for Customer dropdown
@router.get("/customers", response_model=List[schemas.Customer])
def get_customer_selector(
    search: str = Query(None, min_length=1, max_length=50),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Provides a list of customers for dropdowns, with optional search.
    """
    query = db.query(models.Customer)
    if search:
        query = query.filter(models.Customer.name.ilike(f"%{search}%"))
    return query.limit(20).all()

# Endpoint for Project Manager dropdown
@router.get("/project-managers", response_model=List[schemas.User])
def get_project_manager_selector(
    search: str = Query(None, min_length=1, max_length=50),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Provides a list of users with the 'Project Manager' role for dropdowns.
    """
    query = db.query(models.User).filter(models.User.role == UserRole.PM)
    if search:
        # Search by first name or last name
        query = query.filter(
            models.User.first_name.ilike(f"%{search}%") | \
            models.User.last_name.ilike(f"%{search}%")
        )
    return query.limit(20).all()

@router.get("/internal-projects/", response_model=List[schemas.InternalProject])
def get_internal_project_selector(
    search: str = Query(None, min_length=1, max_length=50),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """
    Provides a list of internal projects for dropdowns, with optional search.
    """
    return crud.get_internal_project_selector_for_user(db, current_user, search)


@router.get("/customer-projects", response_model=List[schemas.CustomerProject])
def get_customer_project_selector(
    search: str = Query(None, min_length=1, max_length=50),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Provides a list of customer projects for dropdowns, with optional search.
    """
    query = db.query(models.CustomerProject)
    if search:
        query = query.filter(models.CustomerProject.name.ilike(f"%{search}%"))
    return query.limit(20).all()