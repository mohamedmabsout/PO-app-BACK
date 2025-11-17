# backend/app/routers/sites.py

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List

from .. import crud, schemas, auth # Import your modules
from ..dependencies import get_db

router = APIRouter(
    prefix="/api/sites",
    tags=["Sites"],
    dependencies=[Depends(auth.get_current_user)] # Protect all site routes
)

@router.get("/", response_model=List[schemas.Site])
def read_all_sites(db: Session = Depends(get_db)):
    """
    Retrieve ALL unique sites.
    Used for populating the 'Site Code' filter dropdown on the frontend.
    """
    return crud.get_all_sites(db=db)