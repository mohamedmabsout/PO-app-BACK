# Create a new file: backend/app/routers/summary.py

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