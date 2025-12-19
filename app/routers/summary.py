# Create a new file: backend/app/routers/summary.py

from typing import List, Optional
from fastapi import APIRouter, Depends
import pandas as pd
import io
from sqlalchemy.orm import Session
from .. import crud, schemas, auth
from ..dependencies import get_db
from fastapi import HTTPException
from datetime import date
from ..models import UserRole
from .. import models
from ..dependencies import get_current_user
from fastapi import APIRouter, Depends, UploadFile, File, status
router = APIRouter(
    prefix="/api/summary",
    tags=["Dashboard Summaries"],
    dependencies=[Depends(auth.get_current_user)]
)

@router.get("/financial-overview", response_model=schemas.FinancialSummary)
def get_financial_overview(    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user) # 👈 Add this
):
    """
    Provides a high-level financial overview of all processed POs.
    """
    return crud.get_total_financial_summary(db, user=current_user)
@router.get("/internal-projects-overview", response_model=List[schemas.ProjectFinancials])
def get_internal_projects_overview(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user) # Add this
):  
    # Pass current_user to filter data
    return crud.get_internal_projects_financial_summary(db, user=current_user)

@router.get("/customer-projects-overview", response_model=List[schemas.ProjectFinancials])
def get_customer_projects_overview(db: Session = Depends(get_db)):
    return crud.get_customer_projects_financial_summary(db=db)
@router.get("/value-by-category")
def get_value_by_category(db: Session = Depends(get_db)):
    return crud.get_po_value_by_category(db=db)
@router.get("/yearly-overview", response_model=schemas.FinancialSummary)
def get_yearly_overview(year: int, db: Session = Depends(get_db),user: models.User = Depends(get_current_user)):
    # Call the new, consolidated function
    summary = crud.get_financial_summary_by_period(db=db, year=year,user=user)
    return summary

@router.get("/monthly-overview", response_model=schemas.FinancialSummary)
def get_monthly_overview(year: int, month: int, db: Session = Depends(get_db),user: models.User = Depends(get_current_user)):
    # Call the new, consolidated function
    summary = crud.get_financial_summary_by_period(db=db, year=year, month=month,user=user)
    return summary

@router.get("/weekly-overview", response_model=schemas.FinancialSummary)
def get_weekly_overview(year: int, week: int, db: Session = Depends(get_db),user: models.User = Depends(get_current_user)):
    # Call the new, consolidated function
    summary = crud.get_financial_summary_by_period(db=db, year=year, week=week,user=user)
    return summary

@router.get("/yearly-chart", response_model=List[schemas.MonthlyChartData])
def get_yearly_chart_data(year: int, db: Session = Depends(get_db),user: models.User = Depends(get_current_user)):
    """Returns correctly calculated aggregated data for each month of a year."""
    return crud.get_yearly_chart_data(db=db, year=year,user=user)

@router.get("/user-performance", response_model=schemas.UserPerformanceSummary)
def get_user_performance(
    user_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
     # SECURITY CHECK
    # 1. If I am Admin, I can see anyone.
    # 2. If I am NOT Admin, I can only see myself.
    if current_user.role != UserRole.ADMIN and current_user.id != user_id:
        raise HTTPException(status_code=403, detail="You cannot view performance data of other managers.")

    return crud.get_user_performance_stats(db, user_id, start_date, end_date)

@router.get("/aging-analysis")
def get_aging_analysis_endpoint(db: Session = Depends(get_db),user: models.User = Depends(get_current_user)):
    return crud.get_aging_analysis(db,user=user)
@router.post("/planning/import")
def import_planning_data(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    try:
        contents = file.file.read()
        df = pd.read_excel(io.BytesIO(contents))
        
        count = crud.import_planning_targets(db, df)
        return {"message": f"Successfully updated {count} target records."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
