from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List , Optional
from pydantic import BaseModel
from datetime import datetime
from sqlalchemy import extract, func
from datetime import date
from .. import models, schemas
from ..dependencies import get_db,get_current_user
import re
import calendar
from ..services.pnl_engine import generate_draft_pnl_for_month, recalculate_fleet_pro_rata

router = APIRouter(prefix="/api/pnl", tags=["Profit & Loss"])


# ==========================================
# 1. FLEET OPERATIONS (Gasoil Agent / QC)
# ==========================================

class FleetCostInput(BaseModel):
    tl_name: str
    car_allocation: float = 0.0
    fuel_cost: float = 0.0
    jawaz_cost: float = 0.0
    ehs_tools: float = 0.0

class FleetCostUpdate(BaseModel):
    costs: List[FleetCostInput]

@router.get("/fleet-costs/{year}/{month}")
def get_fleet_costs(year: int, month: int, db: Session = Depends(get_db)):
    """
    Called by FleetOperations.jsx when it loads.
    Returns existing costs, or a blank template for Team Leaders found in Java.
    """
    period_str = f"{year}-{month:02d}"
    
    # 1. Check if we already saved costs for this month
    existing_costs = db.query(models.FieldOperationsCost).filter(
        models.FieldOperationsCost.period == period_str
    ).all()

    if existing_costs:
        return[
            {
                "id": c.id,
                "tl_name": c.tl_name,
                "car_allocation": c.car_allocation,
                "fuel_cost": c.fuel_cost,
                "jawaz_cost": c.jawaz_cost,
                "ehs_tools": c.ehs_tools
            } for c in existing_costs
        ]

    # 2. If no costs exist, auto-generate the list of Team Leaders 
    # based on the Java data we just fetched into LaborAllocations!
    tls_in_java = db.query(models.LaborAllocation.employee_name).filter(
        models.LaborAllocation.period == period_str,
        models.LaborAllocation.role_type == "Team Leader" # Assuming you tag TLs
    ).distinct().all()

    # Return a blank template for the frontend
    return [
        {
            "tl_name": tl[0],
            "car_allocation": 5500.0, # Default example values
            "fuel_cost": 0.0,         # Waiting for Gasoil Agent
            "jawaz_cost": 500.0,
            "ehs_tools": 200.0
        }
        for tl in tls_in_java
    ]

@router.post("/fleet-costs/{year}/{month}")
def save_fleet_costs(
    year: int, 
    month: int,
    payload: FleetCostUpdate, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """Called by the 'Save Fleet Costs' button. Now uses upsert logic."""
    period_str = f"{year}-{month:02d}"

    # Save/Update inputs
    for cost_data in payload.costs:
        # Look for existing record for this TL and period
        db_cost = db.query(models.FieldOperationsCost).filter(
            models.FieldOperationsCost.period == period_str,
            models.FieldOperationsCost.tl_name == cost_data.tl_name
        ).first()

        if db_cost:
            # Update existing
            db_cost.car_allocation = cost_data.car_allocation
            db_cost.fuel_cost = cost_data.fuel_cost
            db_cost.jawaz_cost = cost_data.jawaz_cost
            db_cost.ehs_tools = cost_data.ehs_tools
        else:
            # Create new
            db_cost = models.FieldOperationsCost(
                period=period_str,
                tl_name=cost_data.tl_name,
                car_allocation=cost_data.car_allocation,
                fuel_cost=cost_data.fuel_cost,
                jawaz_cost=cost_data.jawaz_cost,
                ehs_tools=cost_data.ehs_tools
            )
            db.add(db_cost)

    db.commit()
    recalculate_fleet_pro_rata(db, period_str)

    return {"message": "Fleet costs updated successfully."}


# ==========================================
# 2. LABOR ALLOCATION (Project Manager)
# ==========================================

@router.post("/generate-draft/{year}/{month}")
def trigger_monthly_closing(
    year: int, month: int, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """
    Called by the 'Generate Draft P&L' button in LaborAllocation.jsx.
    Connects to Java, fetches Pointages, and creates Draft Labor Allocations.
    """
    if not generate_draft_pnl_for_month:
        raise HTTPException(status_code=501, detail="P&L Engine not fully implemented yet.")

    try:
        result = generate_draft_pnl_for_month(db, year, month, current_user.id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/labor-allocations/{year}/{month}")
def get_labor_allocations(year: int, month: int, db: Session = Depends(get_db)):
    period_str = f"{year}-{month:02d}"
    allocations = db.query(models.LaborAllocation).filter(models.LaborAllocation.period == period_str).all()

    return {
        "allocations":[
            {
                "id": a.id,
                "employee_name": a.employee_name,
                "role_type": a.role_type,
                "tjm": a.tjm,
                "duid": a.java_project_name, 
                # Send the dates formatted nicely for the UI
                "start_date": a.start_date.strftime("%d/%m/%Y") if a.start_date else None,
                "end_date": a.end_date.strftime("%d/%m/%Y") if a.end_date else None,
                
                "java_validated_days": a.java_validated_days, 
                "allocated_days": a.allocated_days,
                "internal_project_id": a.internal_project_id,
                "project_name": a.internal_project.name if a.internal_project else None
            }
            for a in allocations
        ]
    }

# 1. Define the Schema for the incoming React data
class LaborAllocationUpdateItem(BaseModel):
    id: int
    allocated_days: float
    internal_project_id: Optional[int] = None

class LaborAllocationUpdate(BaseModel):
    allocations: List[LaborAllocationUpdateItem]

# 2. The Save Endpoint
# (Check your React code: if it calls /pnl/labor-allocations/save/2026/03, change the route below to match it!)
@router.post("/labor-allocations/{year}/{month}")
def save_labor_allocations(
    year: int, month: int,
    payload: LaborAllocationUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    period_str = f"{year}-{month:02d}"

    # A. Save the PM's allocations
    for item in payload.allocations:
        db_alloc = db.query(models.LaborAllocation).get(item.id)
        if db_alloc:
            db_alloc.allocated_days = item.allocated_days
            db_alloc.internal_project_id = item.internal_project_id
            db_alloc.allocated_by_id = current_user.id
            
    db.commit() 

    # B. RECALCULATE P&L LABOR COSTS
    # We sum (allocated_days * TJM * 1.32) for each project
    labor_sums = db.query(
        models.LaborAllocation.internal_project_id,
        func.sum(models.LaborAllocation.allocated_days * models.LaborAllocation.tjm * 1.32).label("total_labor")
    ).filter(
        models.LaborAllocation.period == period_str,
        models.LaborAllocation.internal_project_id.isnot(None)
    ).group_by(models.LaborAllocation.internal_project_id).all()

    # Reset all labor costs for this period to 0 first (in case PM removed days)
    db.query(models.ProjectPnL).filter(models.ProjectPnL.period == period_str).update({"labor_cost_field": 0.0})

    # Inject the new calculated costs
    for l_sum in labor_sums:
        pnl = db.query(models.ProjectPnL).filter(
            models.ProjectPnL.internal_project_id == l_sum.internal_project_id,
            models.ProjectPnL.period == period_str
        ).first()
        
        if pnl:
            pnl.labor_cost_field = l_sum.total_labor

    db.commit()
    recalculate_fleet_pro_rata(db, period_str)

    return {"message": "Labor allocations saved and P&L updated."}

from sqlalchemy import func

@router.get("/dashboard/{year}/{month}")
def get_pnl_dashboard(
    year: str, 
    month: str, 
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """
    Fetches the P&L snapshot for all projects.
    If year == 'OVERALL', it aggregates all history.
    Otherwise, fetches for a specific YYYY-MM period.
    """
    results =[]

    if year == "OVERALL":
        # Aggregate ALL historical P&L records grouped by Project
        aggregated_pnls = db.query(
            models.ProjectPnL.internal_project_id,
            func.sum(models.ProjectPnL.service_revenue).label("service_revenue"),
            func.sum(models.ProjectPnL.equipment_revenue).label("equipment_revenue"),
            func.sum(models.ProjectPnL.labor_cost_field).label("labor_cost_field"),
            func.sum(models.ProjectPnL.labor_cost_mgmt).label("labor_cost_mgmt"),
            func.sum(models.ProjectPnL.coop_cost_entreprise).label("coop_cost_entreprise"),
            func.sum(models.ProjectPnL.coop_cost_pp).label("coop_cost_pp"),
            func.sum(models.ProjectPnL.working_trip_cost).label("working_trip_cost"),
            func.sum(models.ProjectPnL.hosting_cost).label("hosting_cost"),
            func.sum(models.ProjectPnL.other_traveling_cost).label("other_traveling_cost"), # Add if you added this column
            func.sum(models.ProjectPnL.other_service_cost).label("other_service_cost"), # Add if you added this column
            func.sum(models.ProjectPnL.equipment_cost).label("equipment_cost"),
            func.sum(models.ProjectPnL.car_allocation_cost).label("car_allocation_cost"),
            func.sum(models.ProjectPnL.fuel_cost).label("fuel_cost"),
            func.sum(models.ProjectPnL.jawaz_cost).label("jawaz_cost"),
            func.sum(models.ProjectPnL.ehs_cost).label("ehs_cost"),
            func.sum(models.ProjectPnL.period_costs).label("period_costs"),
            func.sum(models.ProjectPnL.risk_reserve).label("risk_reserve")
        ).filter(
            models.ProjectPnL.internal_project_id.isnot(None)
        ).group_by(models.ProjectPnL.internal_project_id).all()

        for pnl in aggregated_pnls:
            project = db.query(models.InternalProject).get(pnl.internal_project_id)
            pm_name = "Unassigned"
            if project and project.project_manager:
                pm = project.project_manager
                pm_name = f"{pm.first_name} {pm.last_name}"

            results.append({
                "id": f"overall_{pnl.internal_project_id}", 
                "project_id": pnl.internal_project_id,
                "project_name": project.name if project else "Unknown Project",
                "pm_name": pm_name,
                "status": "AGGREGATED",
                
                "service_revenue": pnl.service_revenue or 0.0,
                "equipment_revenue": pnl.equipment_revenue or 0.0,
                
                # Labor Details
                "labor_cost": (pnl.labor_cost_field or 0.0) + (pnl.labor_cost_mgmt or 0.0),
                "labor_cost_field": pnl.labor_cost_field or 0.0,
                "labor_cost_mgmt": pnl.labor_cost_mgmt or 0.0,

                # Coop Details
                "coop_cost": (pnl.coop_cost_entreprise or 0.0) + (pnl.coop_cost_pp or 0.0),
                "coop_cost_entreprise": pnl.coop_cost_entreprise or 0.0,
                "coop_cost_pp": pnl.coop_cost_pp or 0.0,

                # Travel Details
                "travel_cost": (pnl.working_trip_cost or 0.0) + (pnl.hosting_cost or 0.0),
                "working_trip_cost": pnl.working_trip_cost or 0.0,
                "hosting_cost": pnl.hosting_cost or 0.0,

                # Other
                "other_service_cost": pnl.other_service_cost or 0.0,
                "equipment_cost": pnl.equipment_cost or 0.0,

                # Fleet Details
                "fleet_ops_cost": (pnl.car_allocation_cost or 0.0) + (pnl.fuel_cost or 0.0) + (pnl.jawaz_cost or 0.0) + (pnl.ehs_cost or 0.0),
                "car_allocation_cost": pnl.car_allocation_cost or 0.0,
                "fuel_cost": pnl.fuel_cost or 0.0,
                "jawaz_cost": pnl.jawaz_cost or 0.0,
                "ehs_cost": pnl.ehs_cost or 0.0,

                "period_costs": pnl.period_costs or 0.0,
                "risk_reserve": pnl.risk_reserve or 0.0
            })

    else:
        # Standard Month Fetching
        period_str = f"{year}-{int(month):02d}"
        pnls = db.query(models.ProjectPnL).filter(
            models.ProjectPnL.period == period_str
        ).all()
        
        for pnl in pnls:
            pm_name = "Unassigned"
            if pnl.internal_project and pnl.internal_project.project_manager:
                pm = pnl.internal_project.project_manager
                pm_name = f"{pm.first_name} {pm.last_name}"

            results.append({
                "id": pnl.id,
                "project_id": pnl.internal_project_id,
                "project_name": pnl.internal_project.name if pnl.internal_project else "Unknown Project",
                "pm_name": pm_name,
                "status": pnl.status.value,
                "service_revenue": pnl.service_revenue or 0.0,
                "equipment_revenue": pnl.equipment_revenue or 0.0,
                "labor_cost": (pnl.labor_cost_field or 0.0) + (pnl.labor_cost_mgmt or 0.0),
                "labor_cost_field": pnl.labor_cost_field or 0.0,
                "labor_cost_mgmt": pnl.labor_cost_mgmt or 0.0,
                "coop_cost": (pnl.coop_cost_entreprise or 0.0) + (pnl.coop_cost_pp or 0.0),
                "coop_cost_entreprise": pnl.coop_cost_entreprise or 0.0,
                "coop_cost_pp": pnl.coop_cost_pp or 0.0,
                "travel_cost": (pnl.working_trip_cost or 0.0) + (pnl.hosting_cost or 0.0),
                "working_trip_cost": pnl.working_trip_cost or 0.0,
                "hosting_cost": pnl.hosting_cost or 0.0,
                "other_service_cost": 0.0,
                "equipment_cost": pnl.equipment_cost or 0.0,
                "fleet_ops_cost": (pnl.car_allocation_cost or 0.0) + (pnl.fuel_cost or 0.0) + (pnl.jawaz_cost or 0.0) + (pnl.ehs_cost or 0.0),
                "car_allocation_cost": pnl.car_allocation_cost or 0.0,
                "fuel_cost": pnl.fuel_cost or 0.0,
                "jawaz_cost": pnl.jawaz_cost or 0.0,
                "ehs_cost": pnl.ehs_cost or 0.0,

                "period_costs": pnl.period_costs or 0.0,
                "risk_reserve": pnl.risk_reserve or 0.0
            })
            
    return results


class BackofficeExpenseInput(BaseModel):
    id: Optional[int] = None # Optional for new rows
    category: str
    amount: float

class BackofficeExpenseUpdate(BaseModel):
    expenses: List[BackofficeExpenseInput]

@router.get("/backoffice-expenses/{year}/{month}")
def get_backoffice_expenses(year: int, month: int, db: Session = Depends(get_db)):
    period_str = f"{year}-{month:02d}"
    
    # 1. Fetch Manual Expenses (Rent, Internet, etc.)
    expenses = db.query(models.BackofficeExpense).filter(
        models.BackofficeExpense.period == period_str
    ).all()

    result =[
        {
            "id": e.id,
            "category": e.category,
            "amount": e.amount,
            "is_system_generated": False
        }
        for e in expenses
    ]

    # 2. Fetch DEPOT Allocations for Traceability
    depot_allocs = db.query(models.LaborAllocation).filter(
        models.LaborAllocation.period == period_str,
        models.LaborAllocation.role_type == "DEPOT"
    ).all()

    depot_total = 0.0
    depot_details =[]
    
    for d in depot_allocs:
        # Math: TJM * 1.32 * Days
        cost = (d.tjm or 0.0) * 1.32 * (d.allocated_days or 0.0)
        depot_total += cost
        depot_details.append({
            "agent": d.employee_name,
            "tjm": d.tjm,
            "days": d.allocated_days,
            "cost": cost,
            "dates": f"{d.start_date} to {d.end_date}" if d.start_date else "Month"
        })

    # If we have Depot workers, inject them as Row #1 in the response
    if depot_total > 0:
        result.insert(0, {
            "id": "DEPOT_AUTO",
            "category": "DEPOT Affectations (Labor Cost)",
            "amount": depot_total,
            "is_system_generated": True,
            "details": depot_details # Send details to React!
        })

    return result


@router.post("/backoffice-expenses/{year}/{month}")
def save_backoffice_expenses(
    year: int, month: int,
    payload: BackofficeExpenseUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    period_str = f"{year}-{month:02d}"

    # 1. Delete old MANUAL entries
    db.query(models.BackofficeExpense).filter(
        models.BackofficeExpense.period == period_str
    ).delete()
    db.flush()

    # 2. Save new manual rows & calculate manual total
    manual_overhead = 0.0
    for item in payload.expenses:
        # Ignore the injected Depot row, it doesn't go in this table
        if item.category == "DEPOT Affectations (Labor Cost)" or getattr(item, 'is_system_generated', False):
            continue
            
        if item.amount > 0 and item.category.strip():
            new_exp = models.BackofficeExpense(
                period=period_str, category=item.category, 
                amount=item.amount, entered_by_id=current_user.id
            )
            db.add(new_exp)
            manual_overhead += item.amount

    # 3. Recalculate Depot total for the master distribution
    depot_allocs = db.query(models.LaborAllocation).filter(
        models.LaborAllocation.period == period_str,
        models.LaborAllocation.role_type == "DEPOT"
    ).all()
    depot_overhead = sum((d.tjm or 0) * 1.32 * (d.allocated_days or 0) for d in depot_allocs)

    total_overhead = manual_overhead + depot_overhead

    # 4. RECALCULATE P&L PERIOD COSTS (Pro-Rata)
    all_pnls = db.query(models.ProjectPnL).filter(models.ProjectPnL.period == period_str).all()
    total_company_revenue = sum((pnl.service_revenue or 0.0) + (pnl.equipment_revenue or 0.0) for pnl in all_pnls)

    for pnl in all_pnls:
        project_revenue = (pnl.service_revenue or 0.0) + (pnl.equipment_revenue or 0.0)
        if total_company_revenue > 0:
            pnl.period_costs = total_overhead * (project_revenue / total_company_revenue)
        else:
            pnl.period_costs = total_overhead / len(all_pnls) if len(all_pnls) > 0 else 0.0

    db.commit()
    return {"message": "Overhead costs saved and P&L updated."}