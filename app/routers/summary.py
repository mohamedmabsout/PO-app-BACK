# Create a new file: backend/app/routers/summary.py

from typing import List
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from .. import crud, schemas, auth
from ..dependencies import get_db
from fastapi import HTTPException

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
# summary.py router

# ... (imports) ...

@router.get("/internal-projects-overview", response_model=List[schemas.ProjectFinancials])
def get_internal_projects_overview(db: Session = Depends(get_db)):
    return crud.get_internal_projects_financial_summary(db=db)

@router.get("/customer-projects-overview", response_model=List[schemas.ProjectFinancials])
def get_customer_projects_overview(db: Session = Depends(get_db)):
    return crud.get_customer_projects_financial_summary(db=db)
@router.get("/value-by-category")
def get_value_by_category(db: Session = Depends(get_db)):
    return crud.get_po_value_by_category(db=db)
# --- NOUVEAU : Endpoint pour l'aperçu ANNUEL (Yearly) ---
@router.get("/yearly-overview", response_model=schemas.FinancialSummary)
def get_yearly_overview(year: int, db: Session = Depends(get_db)):
    summary = crud.get_financial_summary_for_year(db=db, year=year)
    if not summary:
        raise HTTPException(status_code=404, detail=f"No data found for year {year}")
    return summary

# --- NOUVEAU : Endpoint pour l'aperçu MENSUEL (Monthly) ---
@router.get("/monthly-overview", response_model=schemas.FinancialSummary)
def get_monthly_overview(year: int, month: int, db: Session = Depends(get_db)):
    summary = crud.get_financial_summary_for_month(db=db, year=year, month=month)
    if not summary:
        raise HTTPException(status_code=404, detail=f"No data found for {year}-{month}")
    return summary

# --- NOUVEAU : Endpoint pour l'aperçu HEBDOMADAIRE (Weekly) ---
@router.get("/weekly-overview", response_model=schemas.FinancialSummary)
def get_weekly_overview(year: int, week: int, db: Session = Depends(get_db)):
    summary = crud.get_financial_summary_for_week(db=db, year=year, week=week)
    if not summary:
        raise HTTPException(status_code=404, detail=f"No data found for week {week} of {year}")
    return summary