# Create a new file: backend/app/routers/summary.py

from typing import List, Optional
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


@router.get("/internal-projects-overview", response_model=List[schemas.ProjectFinancials])
def get_internal_projects_overview(db: Session = Depends(get_db)):
    return crud.get_internal_projects_financial_summary(db=db)

@router.get("/customer-projects-overview", response_model=List[schemas.ProjectFinancials])
def get_customer_projects_overview(db: Session = Depends(get_db)):
    return crud.get_customer_projects_financial_summary(db=db)
@router.get("/value-by-category")
def get_value_by_category(db: Session = Depends(get_db)):
    return crud.get_po_value_by_category(db=db)
@router.get("/yearly-overview", response_model=schemas.FinancialSummary)
def get_yearly_overview(year: int, db: Session = Depends(get_db)):
    # Call the new, consolidated function
    summary = crud.get_financial_summary_by_period(db=db, year=year)
    return summary

@router.get("/monthly-overview", response_model=schemas.FinancialSummary)
def get_monthly_overview(year: int, month: int, db: Session = Depends(get_db)):
    # Call the new, consolidated function
    summary = crud.get_financial_summary_by_period(db=db, year=year, month=month)
    return summary

@router.get("/weekly-overview", response_model=schemas.FinancialSummary)
def get_weekly_overview(year: int, week: int, db: Session = Depends(get_db)):
    # Call the new, consolidated function
    summary = crud.get_financial_summary_by_period(db=db, year=year, week=week)
    return summary

@router.get("/yearly-chart", response_model=List[schemas.MonthlyChartData])
def get_yearly_chart_data(year: int, db: Session = Depends(get_db)):
    """Returns correctly calculated aggregated data for each month of a year."""
    return crud.get_yearly_chart_data(db=db, year=year)

@router.get("/user-performance", response_model=schemas.UserPerformanceSummary)
def get_user_performance(
    user_id: int,
    year: int, 
    month: Optional[int] = None, 
    week: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """
    Returns performance stats for a specific user (PM/Admin/CEO) based on 
    the projects they manage.
    """
    return crud.get_user_performance_stats(
        db=db, 
        user_id=user_id, 
        year=year, 
        month=month, 
        week=week
    )

