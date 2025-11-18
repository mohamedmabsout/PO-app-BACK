# Create a new file: backend/app/routers/summary.py

from typing import List
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from .. import crud, schemas, auth
from ..dependencies import get_db

router = APIRouter(
    prefix="/api/summary",
    tags=["Dashboard Summaries"],
    dependencies=[Depends(auth.get_current_user)]
)

@router.get("/financial-overview", response_model=schemas.FinancialSummary)
def get_financial_overview(db: Session = Depends(get_db)):
    """
    Provides a high-level financial overview of all processed POs.
    """
    return crud.get_total_financial_summary(db=db)
@router.get("/projects-overview", response_model=List[schemas.ProjectFinancials])
def get_projects_overview(db: Session = Depends(get_db)):
    return crud.get_projects_financial_summary(db=db)

@router.get("/value-by-category")
def get_value_by_category(db: Session = Depends(get_db)):
    return crud.get_po_value_by_category(db=db)