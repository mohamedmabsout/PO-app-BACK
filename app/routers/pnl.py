from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from typing import List , Optional
from pydantic import BaseModel
from datetime import datetime
from sqlalchemy import extract, func
from datetime import date
from .. import models, schemas
from ..dependencies import get_db,get_current_user
import re
import io
import calendar
import pandas as pd
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

    # Inject the new calculated costs — auto-create PnL record if missing
    for l_sum in labor_sums:
        pnl = db.query(models.ProjectPnL).filter(
            models.ProjectPnL.internal_project_id == l_sum.internal_project_id,
            models.ProjectPnL.period == period_str
        ).first()

        if pnl:
            pnl.labor_cost_field = l_sum.total_labor
        else:
            # Project has no PnL record for this period yet — create one
            # so the labor cost is not silently dropped
            new_pnl = models.ProjectPnL(
                internal_project_id=l_sum.internal_project_id,
                period=period_str,
                labor_cost_field=l_sum.total_labor,
                created_by_id=current_user.id
            )
            db.add(new_pnl)

    db.commit()
    recalculate_fleet_pro_rata(db, period_str)

    return {"message": "Labor allocations saved and P&L updated."}

from sqlalchemy import func

@router.get("/dashboard/{year}/{month}")
def get_pnl_dashboard(
    year: str, 
    month: str, 
    project_id: Optional[int] = Query(None), # NEW FILTER
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """
    Fetches P&L snapshot with dynamic pivoting:
    1. OVERALL + No Project = Group by Project (All time)
    2. OVERALL + Project = Group by Month (Project timeline)
    3. Specific Month + No Project = Group by Project (For that month)
    4. Specific Month + Project = Single Project Snapshot
    """
    
    # --- 1. CALCULATE UNASSIGNED LABOR COSTS (The Red Alert) ---
    unassigned_query = db.query(
        func.sum(models.LaborAllocation.allocated_days * models.LaborAllocation.tjm * 1.32)
    ).filter(
        models.LaborAllocation.internal_project_id.is_(None),
        models.LaborAllocation.role_type != "DEPOT" # Depot is already accounted for
    )
    
    if year != "OVERALL":
        period_str = f"{year}-{int(month):02d}"
        unassigned_query = unassigned_query.filter(models.LaborAllocation.period == period_str)
        
    unassigned_labor_cost = unassigned_query.scalar() or 0.0

    # Helper function for aggregated sums
    def get_sums():
        return[
            func.sum(models.ProjectPnL.service_revenue).label("service_revenue"),
            func.sum(models.ProjectPnL.equipment_revenue).label("equipment_revenue"),
            func.sum(models.ProjectPnL.labor_cost_field).label("labor_cost_field"),
            func.sum(models.ProjectPnL.labor_cost_mgmt).label("labor_cost_mgmt"),
            func.sum(models.ProjectPnL.coop_cost_entreprise).label("coop_cost_entreprise"),
            func.sum(models.ProjectPnL.coop_cost_pp).label("coop_cost_pp"),
            func.sum(models.ProjectPnL.working_trip_cost).label("working_trip_cost"),
            func.sum(models.ProjectPnL.hosting_cost).label("hosting_cost"),
            func.sum(models.ProjectPnL.other_traveling_cost).label("other_traveling_cost"), 
            func.sum(models.ProjectPnL.other_service_cost).label("other_service_cost"), 
            func.sum(models.ProjectPnL.equipment_cost).label("equipment_cost"),
            func.sum(models.ProjectPnL.car_allocation_cost).label("car_allocation_cost"),
            func.sum(models.ProjectPnL.fuel_cost).label("fuel_cost"),
            func.sum(models.ProjectPnL.jawaz_cost).label("jawaz_cost"),
            func.sum(models.ProjectPnL.ehs_cost).label("ehs_cost"),
            func.sum(models.ProjectPnL.period_costs).label("period_costs"),
            func.sum(models.ProjectPnL.risk_reserve).label("risk_reserve"),
        ]

    results =[]

    # Check if caller is a PM — if so, restrict to their own projects
    is_pm = (current_user.role or "").upper() == "PM"
    pm_project_ids = None
    if is_pm:
        pm_projects = db.query(models.InternalProject.id).filter(
            models.InternalProject.project_manager_id == current_user.id
        ).all()
        pm_project_ids = [p.id for p in pm_projects]

    # --- SCENARIO A: OVERALL Timeline for a Single Project ---
    if year == "OVERALL" and project_id:
        # PM can only view their own project
        if is_pm and project_id not in (pm_project_ids or []):
            return {"unassigned_labor_cost": 0.0, "data": []}

        aggregated_pnls = db.query(
            models.ProjectPnL.period.label("group_key"),
            *get_sums()
        ).filter(
            models.ProjectPnL.internal_project_id == project_id
        ).group_by(models.ProjectPnL.period).order_by(models.ProjectPnL.period).all()

        project = db.query(models.InternalProject).get(project_id)
        pm_name = f"{project.project_manager.first_name} {project.project_manager.last_name}" if project and project.project_manager else "Unassigned"

        for pnl in aggregated_pnls:
            results.append({
                "id": f"period_{pnl.group_key}",
                "project_id": project_id,
                "project_name": pnl.group_key, # REPURPOSED: Sending the Month (e.g., '2026-03') to display in the column header
                "pm_name": pm_name,
                "status": "TIMELINE",
                "service_revenue": pnl.service_revenue or 0.0,
                "equipment_revenue": pnl.equipment_revenue or 0.0,
                "labor_cost": (pnl.labor_cost_field or 0.0) + (pnl.labor_cost_mgmt or 0.0),
                "labor_cost_field": pnl.labor_cost_field or 0.0,
                "labor_cost_mgmt": pnl.labor_cost_mgmt or 0.0,
                "coop_cost": (pnl.coop_cost_entreprise or 0.0) + (pnl.coop_cost_pp or 0.0),
                "coop_cost_entreprise": pnl.coop_cost_entreprise or 0.0,
                "coop_cost_pp": pnl.coop_cost_pp or 0.0,
                "travel_cost": (pnl.working_trip_cost or 0.0) + (pnl.hosting_cost or 0.0) + (pnl.other_traveling_cost or 0.0),
                "working_trip_cost": pnl.working_trip_cost or 0.0,
                "hosting_cost": pnl.hosting_cost or 0.0,
                "other_travel_cost": pnl.other_traveling_cost or 0.0,
                "other_service_cost": pnl.other_service_cost or 0.0,
                "equipment_cost": pnl.equipment_cost or 0.0,
                "fleet_ops_cost": (pnl.car_allocation_cost or 0.0) + (pnl.fuel_cost or 0.0) + (pnl.jawaz_cost or 0.0) + (pnl.ehs_cost or 0.0),
                "car_allocation_cost": pnl.car_allocation_cost or 0.0,
                "fuel_cost": pnl.fuel_cost or 0.0,
                "jawaz_cost": pnl.jawaz_cost or 0.0,
                "ehs_cost": pnl.ehs_cost or 0.0,
                "period_costs": pnl.period_costs or 0.0,
                "risk_reserve": pnl.risk_reserve or 0.0
            })

    # --- SCENARIO B: OVERALL for All Projects ---
    elif year == "OVERALL" and not project_id:
        b_filter = [models.ProjectPnL.internal_project_id.isnot(None)]
        if is_pm and pm_project_ids is not None:
            b_filter.append(models.ProjectPnL.internal_project_id.in_(pm_project_ids))

        aggregated_pnls = db.query(
            models.ProjectPnL.internal_project_id.label("group_key"),
            *get_sums()
        ).filter(*b_filter).group_by(models.ProjectPnL.internal_project_id).all()

        for pnl in aggregated_pnls:
            project = db.query(models.InternalProject).get(pnl.group_key)
            pm_name = f"{project.project_manager.first_name} {project.project_manager.last_name}" if project and project.project_manager else "Unassigned"

            results.append({
                "id": f"overall_{pnl.group_key}", 
                "project_id": pnl.group_key,
                "project_name": project.name if project else "Unknown Project",
                "pm_name": pm_name,
                "status": "AGGREGATED",
                "service_revenue": pnl.service_revenue or 0.0,
                "equipment_revenue": pnl.equipment_revenue or 0.0,
                "labor_cost": (pnl.labor_cost_field or 0.0) + (pnl.labor_cost_mgmt or 0.0),
                "labor_cost_field": pnl.labor_cost_field or 0.0,
                "labor_cost_mgmt": pnl.labor_cost_mgmt or 0.0,
                "coop_cost": (pnl.coop_cost_entreprise or 0.0) + (pnl.coop_cost_pp or 0.0),
                "coop_cost_entreprise": pnl.coop_cost_entreprise or 0.0,
                "coop_cost_pp": pnl.coop_cost_pp or 0.0,
                "travel_cost": (pnl.working_trip_cost or 0.0) + (pnl.hosting_cost or 0.0) + (pnl.other_traveling_cost or 0.0),
                "working_trip_cost": pnl.working_trip_cost or 0.0,
                "hosting_cost": pnl.hosting_cost or 0.0,
                "other_travel_cost": pnl.other_traveling_cost or 0.0,
                "other_service_cost": pnl.other_service_cost or 0.0,
                "equipment_cost": pnl.equipment_cost or 0.0,
                "fleet_ops_cost": (pnl.car_allocation_cost or 0.0) + (pnl.fuel_cost or 0.0) + (pnl.jawaz_cost or 0.0) + (pnl.ehs_cost or 0.0),
                "car_allocation_cost": pnl.car_allocation_cost or 0.0,
                "fuel_cost": pnl.fuel_cost or 0.0,
                "jawaz_cost": pnl.jawaz_cost or 0.0,
                "ehs_cost": pnl.ehs_cost or 0.0,
                "period_costs": pnl.period_costs or 0.0,
                "risk_reserve": pnl.risk_reserve or 0.0
            })

    # --- SCENARIO C & D: Specific Month ---
    else:
        period_str = f"{year}-{int(month):02d}"
        query = db.query(models.ProjectPnL).filter(models.ProjectPnL.period == period_str)

        if project_id:
            query = query.filter(models.ProjectPnL.internal_project_id == project_id)

        if is_pm and pm_project_ids is not None:
            query = query.filter(models.ProjectPnL.internal_project_id.in_(pm_project_ids))

        pnls = query.all()
        
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
                "travel_cost": (pnl.working_trip_cost or 0.0) + (pnl.hosting_cost or 0.0) + (pnl.other_traveling_cost or 0.0),
                "working_trip_cost": pnl.working_trip_cost or 0.0,
                "hosting_cost": pnl.hosting_cost or 0.0,
                "other_travel_cost": pnl.other_traveling_cost or 0.0,
                "other_service_cost": pnl.other_service_cost or 0.0,
                "equipment_cost": pnl.equipment_cost or 0.0,
                "fleet_ops_cost": (pnl.car_allocation_cost or 0.0) + (pnl.fuel_cost or 0.0) + (pnl.jawaz_cost or 0.0) + (pnl.ehs_cost or 0.0),
                "car_allocation_cost": pnl.car_allocation_cost or 0.0,
                "fuel_cost": pnl.fuel_cost or 0.0,
                "jawaz_cost": pnl.jawaz_cost or 0.0,
                "ehs_cost": pnl.ehs_cost or 0.0,
                "period_costs": pnl.period_costs or 0.0,
                "risk_reserve": pnl.risk_reserve or 0.0
            })
            
    # Return wrapper containing the data array AND the unassigned cost
    return {
        "unassigned_labor_cost": unassigned_labor_cost,
        "data": results
    }


class BackofficeExpenseInput(BaseModel):
    id: Optional[int] = None # Optional for new rows
    category: str
    amount: float

class BackofficeExpenseUpdate(BaseModel):
    expenses: List[BackofficeExpenseInput]

@router.get("/backoffice-expenses/{year}/{month}")
def get_backoffice_expenses(year: str, month: str, db: Session = Depends(get_db)):

    is_overall = year == "OVERALL"
    period_str = None if is_overall else f"{year}-{int(month):02d}"

    result = []

    # 1. Fetch DEPOT Allocations and break down into salary components
    depot_query = db.query(models.LaborAllocation).filter(
        models.LaborAllocation.role_type == "DEPOT"
    )
    if not is_overall:
        depot_query = depot_query.filter(models.LaborAllocation.period == period_str)
    depot_allocs = depot_query.all()

    depot_salary_details = []
    brut_salary_total = 0.0
    net_salary_total = 0.0
    cnss_ir_total = 0.0

    for d in depot_allocs:
        tjm = d.tjm or 0.0
        days = d.allocated_days or 0.0

        # Salary components
        net = tjm * days  # Net salary (base)
        charges = net * 0.32  # CNSS + IR @ 32%
        brut = net + charges  # Brut = Net + Charges

        brut_salary_total += brut
        net_salary_total += net
        cnss_ir_total += charges

        depot_salary_details.append({
            "agent": d.employee_name,
            "tjm": tjm,
            "days": days,
            "net": net,
            "charges": charges,
            "brut": brut,
            "dates": f"{d.start_date} to {d.end_date}" if d.start_date else ("Month" if not is_overall else d.period)
        })

    # 2. Add salary breakdown rows if DEPOT workers exist
    if brut_salary_total > 0:
        # Brut Salary (counted in total)
        result.append({
            "id": "DEPOT_BRUT_SALARY",
            "category": "Brut Salary (Backoffice)",
            "amount": brut_salary_total,
            "is_system_generated": True,
            "subcategory": "Salary Components",
            "is_countable": True,  # Include in total
            "details": depot_salary_details
        })

        # Net Salary (breakdown only, not counted in total)
        result.append({
            "id": "DEPOT_NET_SALARY",
            "category": "Net Salary (Backoffice)",
            "amount": net_salary_total,
            "is_system_generated": True,
            "subcategory": "Salary Components",
            "is_countable": False,  # For display/breakdown only
            "details": depot_salary_details
        })

        # CNSS + IR (breakdown only, not counted in total)
        result.append({
            "id": "DEPOT_CNSS_IR",
            "category": "CNSS + IR (Backoffice)",
            "amount": cnss_ir_total,
            "is_system_generated": True,
            "subcategory": "Salary Components",
            "is_countable": False,  # For display/breakdown only
            "details": depot_salary_details
        })

    # 3. Fetch All Backoffice Expenses (including Java-generated traveling costs)
    expense_query = db.query(models.BackofficeExpense)
    if not is_overall:
        expense_query = expense_query.filter(models.BackofficeExpense.period == period_str)
    expenses = expense_query.all()

    # Define traveling cost categories that come from Java
    java_traveling_categories = [
        "Hosting Costs (Backoffice)",
        "Working trip Costs (Backoffice)",
        "other travelling Costs (Backoffice)"
    ]

    # Add expenses — aggregate by category when OVERALL
    if is_overall:
        category_totals = {}
        for e in expenses:
            cat = e.category
            if cat not in category_totals:
                is_java_expense = cat in java_traveling_categories
                category_totals[cat] = {
                    "id": f"AGG_{cat}",
                    "category": cat,
                    "amount": 0.0,
                    "is_system_generated": is_java_expense,
                    "is_countable": True
                }
            category_totals[cat]["amount"] += (e.amount or 0.0)
        result.extend(category_totals.values())
    else:
        for e in expenses:
            is_java_expense = e.category in java_traveling_categories
            result.append({
                "id": e.id,
                "category": e.category,
                "amount": e.amount,
                "is_system_generated": is_java_expense,
                "is_countable": True
            })

    return result


# ==========================================
# 2b. DISTRIBUTION CONFIGURATION
# ==========================================

class DistributionConfigItem(BaseModel):
    internal_project_id: int
    percentage: float

class DistributionConfigUpdate(BaseModel):
    projects: List[DistributionConfigItem]


@router.get("/distribution-config/{year}/{month}")
def get_distribution_config(
    year: str, month: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    period_str = f"{year}-{int(month):02d}"

    # Check if manual config exists for this period
    configs = db.query(models.DistributionConfig).filter(
        models.DistributionConfig.period == period_str
    ).all()

    if configs:
        # Manual mode — return stored percentages
        projects = []
        for c in configs:
            proj = db.query(models.InternalProject).get(c.internal_project_id)
            projects.append({
                "internal_project_id": c.internal_project_id,
                "project_name": proj.name if proj else "Unknown",
                "percentage": c.percentage
            })
        return {"mode": "manual", "projects": projects}

    # Auto mode — compute revenue-based percentages on-the-fly
    pnls = db.query(models.ProjectPnL).filter(
        models.ProjectPnL.period == period_str
    ).all()

    total_revenue = sum(
        (p.service_revenue or 0.0) + (p.equipment_revenue or 0.0) for p in pnls
    )

    projects = []
    for p in pnls:
        proj = db.query(models.InternalProject).get(p.internal_project_id)
        rev = (p.service_revenue or 0.0) + (p.equipment_revenue or 0.0)
        pct = (rev / total_revenue * 100.0) if total_revenue > 0 else (100.0 / len(pnls) if pnls else 0.0)
        projects.append({
            "internal_project_id": p.internal_project_id,
            "project_name": proj.name if proj else "Unknown",
            "percentage": round(pct, 2)
        })

    return {"mode": "auto", "projects": projects}


@router.post("/distribution-config/{year}/{month}")
def save_distribution_config(
    year: str, month: str,
    payload: DistributionConfigUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    period_str = f"{year}-{int(month):02d}"

    # Validate sum == 100
    total_pct = sum(item.percentage for item in payload.projects)
    if abs(total_pct - 100.0) > 0.01:
        raise HTTPException(
            status_code=422,
            detail=f"Percentages must sum to 100%. Current sum: {total_pct:.2f}%"
        )

    # Delete existing config for this period
    db.query(models.DistributionConfig).filter(
        models.DistributionConfig.period == period_str
    ).delete()
    db.flush()

    # Insert new config
    for item in payload.projects:
        db.add(models.DistributionConfig(
            period=period_str,
            internal_project_id=item.internal_project_id,
            percentage=item.percentage,
            created_by_id=current_user.id
        ))

    db.commit()
    return {"message": "Distribution config saved.", "mode": "manual"}


@router.delete("/distribution-config/{year}/{month}")
def reset_distribution_config(
    year: str, month: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    period_str = f"{year}-{int(month):02d}"
    db.query(models.DistributionConfig).filter(
        models.DistributionConfig.period == period_str
    ).delete()
    db.commit()
    return {"message": "Distribution reset to automatic (revenue-based)."}


@router.get("/distribution-config/{year}/{month}/export")
def export_distribution_config(
    year: str, month: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    period_str = f"{year}-{int(month):02d}"

    # Get current config (manual or auto-computed)
    configs = db.query(models.DistributionConfig).filter(
        models.DistributionConfig.period == period_str
    ).all()

    rows = []
    if configs:
        for c in configs:
            proj = db.query(models.InternalProject).get(c.internal_project_id)
            rows.append({
                "Project ID": c.internal_project_id,
                "Project Name": proj.name if proj else "Unknown",
                "Percentage (%)": c.percentage
            })
    else:
        # Auto mode — compute from revenue
        pnls = db.query(models.ProjectPnL).filter(
            models.ProjectPnL.period == period_str
        ).all()
        total_revenue = sum(
            (p.service_revenue or 0.0) + (p.equipment_revenue or 0.0) for p in pnls
        )
        for p in pnls:
            proj = db.query(models.InternalProject).get(p.internal_project_id)
            rev = (p.service_revenue or 0.0) + (p.equipment_revenue or 0.0)
            pct = (rev / total_revenue * 100.0) if total_revenue > 0 else (100.0 / len(pnls) if pnls else 0.0)
            rows.append({
                "Project ID": p.internal_project_id,
                "Project Name": proj.name if proj else "Unknown",
                "Percentage (%)": round(pct, 2)
            })

    df = pd.DataFrame(rows)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Distribution Config')
        worksheet = writer.sheets['Distribution Config']
        for idx, col in enumerate(df.columns):
            max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2 if len(df) > 0 else len(col) + 2
            worksheet.set_column(idx, idx, max_len)
    output.seek(0)

    return StreamingResponse(
        output,
        headers={"Content-Disposition": f"attachment; filename=Distribution_Config_{period_str}.xlsx"},
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@router.post("/distribution-config/{year}/{month}/import")
async def import_distribution_config(
    year: str, month: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    period_str = f"{year}-{int(month):02d}"

    # Read uploaded Excel
    contents = await file.read()
    df = pd.read_excel(io.BytesIO(contents))

    # Validate columns
    required_cols = {"Project ID", "Percentage (%)"}
    if not required_cols.issubset(set(df.columns)):
        raise HTTPException(status_code=422, detail=f"Excel must contain columns: {required_cols}")

    # Validate data
    errors = []
    for i, row in df.iterrows():
        pid = row["Project ID"]
        pct = row["Percentage (%)"]
        proj = db.query(models.InternalProject).get(int(pid))
        if not proj:
            errors.append(f"Row {i+2}: Project ID {pid} not found.")
        if not isinstance(pct, (int, float)) or pct < 0:
            errors.append(f"Row {i+2}: Invalid percentage '{pct}'.")

    if errors:
        raise HTTPException(status_code=422, detail="; ".join(errors))

    total_pct = df["Percentage (%)"].sum()
    if abs(total_pct - 100.0) > 0.01:
        raise HTTPException(
            status_code=422,
            detail=f"Percentages must sum to 100%. Current sum: {total_pct:.2f}%"
        )

    # Delete existing config and insert imported data
    db.query(models.DistributionConfig).filter(
        models.DistributionConfig.period == period_str
    ).delete()
    db.flush()

    for _, row in df.iterrows():
        db.add(models.DistributionConfig(
            period=period_str,
            internal_project_id=int(row["Project ID"]),
            percentage=float(row["Percentage (%)"]),
            created_by_id=current_user.id
        ))

    db.commit()
    return {"message": f"Imported {len(df)} project distributions.", "mode": "manual"}


@router.post("/backoffice-expenses/{year}/{month}")
def save_backoffice_expenses(
    year: int, month: int,
    payload: BackofficeExpenseUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    period_str = f"{year}-{month:02d}"

    # Define traveling cost categories that come from Java (don't delete these)
    java_traveling_categories = [
        "Hosting Costs (Backoffice)",
        "Working trip Costs (Backoffice)",
        "other travelling Costs (Backoffice)"
    ]

    # 1. Delete only MANUAL entries (preserve Java-generated traveling costs)
    db.query(models.BackofficeExpense).filter(
        models.BackofficeExpense.period == period_str,
        ~models.BackofficeExpense.category.in_(java_traveling_categories)
    ).delete()
    db.flush()

    # 2. Save new manual rows & calculate manual total
    manual_overhead = 0.0
    for item in payload.expenses:
        # Ignore system-generated rows and DEPOT salary rows
        if getattr(item, 'is_system_generated', False):
            continue

        # Skip traveling costs (they're managed by Java/PnL engine)
        if item.category in java_traveling_categories:
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

    # 3b. Add Java-generated traveling costs
    java_traveling_overhead = db.query(func.sum(models.BackofficeExpense.amount)).filter(
        models.BackofficeExpense.period == period_str,
        models.BackofficeExpense.category.in_(java_traveling_categories)
    ).scalar() or 0.0

    total_overhead = manual_overhead + depot_overhead + java_traveling_overhead

    # 4. RECALCULATE P&L PERIOD COSTS
    all_pnls = db.query(models.ProjectPnL).filter(models.ProjectPnL.period == period_str).all()

    # Check if manual distribution config exists
    dist_configs = db.query(models.DistributionConfig).filter(
        models.DistributionConfig.period == period_str
    ).all()

    if dist_configs:
        # Manual mode — use configured percentages
        config_map = {dc.internal_project_id: dc.percentage for dc in dist_configs}
        for pnl in all_pnls:
            pct = config_map.get(pnl.internal_project_id, 0.0)
            pnl.period_costs = total_overhead * (pct / 100.0)
    else:
        # Auto mode — revenue-based pro-rata
        total_company_revenue = sum((pnl.service_revenue or 0.0) + (pnl.equipment_revenue or 0.0) for pnl in all_pnls)
        for pnl in all_pnls:
            project_revenue = (pnl.service_revenue or 0.0) + (pnl.equipment_revenue or 0.0)
            if total_company_revenue > 0:
                pnl.period_costs = total_overhead * (project_revenue / total_company_revenue)
            else:
                pnl.period_costs = total_overhead / len(all_pnls) if len(all_pnls) > 0 else 0.0

    db.commit()
    return {"message": "Overhead costs saved and P&L updated."}


# ==========================================
# 3. DETAILED BREAKDOWN FOR EXPORT
# ==========================================

@router.get("/detailed-breakdown/{year}/{month}")
def get_detailed_breakdown(
    year: str,
    month: str,
    project_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """
    Returns detailed cost breakdown with sources and components for export.
    Shows the breakdown of each major cost category.
    """
    period_str = f"{year}-{int(month):02d}" if year != "OVERALL" else "OVERALL"
    breakdown = []

    # Build query for project filter
    pnl_query = db.query(models.ProjectPnL)
    labor_query = db.query(models.LaborAllocation)

    if year != "OVERALL":
        pnl_query = pnl_query.filter(models.ProjectPnL.period == period_str)
        labor_query = labor_query.filter(models.LaborAllocation.period == period_str)

    if project_id:
        pnl_query = pnl_query.filter(models.ProjectPnL.internal_project_id == project_id)
        labor_query = labor_query.filter(models.LaborAllocation.internal_project_id == project_id)

    pnls = pnl_query.all()
    labor_allocs = labor_query.all()
    fleet_costs = db.query(models.FieldOperationsCost).filter(
        models.FieldOperationsCost.period == period_str
    ).all() if year != "OVERALL" else []

    # ======= REVENUES =======
    for pnl in pnls:
        project_name = pnl.internal_project.name if pnl.internal_project else "Unknown"

        if (pnl.service_revenue or 0) > 0:
            breakdown.append({
                "category": "Service Revenue",
                "subcategory": "Service Revenue",
                "description": "Service delivery revenue",
                "amount": pnl.service_revenue or 0,
                "project_name": project_name,
                "source": "Revenue System",
                "type": "revenue"
            })

        if (pnl.equipment_revenue or 0) > 0:
            breakdown.append({
                "category": "Equipment Revenue",
                "subcategory": "Equipment Revenue",
                "description": "Equipment rental/sales revenue",
                "amount": pnl.equipment_revenue or 0,
                "project_name": project_name,
                "source": "Revenue System",
                "type": "revenue"
            })

    # ======= LABOR COSTS =======
    for labor in labor_allocs:
        if labor.allocated_days and labor.allocated_days > 0:
            project_name = labor.internal_project.name if labor.internal_project else "Unassigned"
            daily_cost = (labor.tjm or 0) * 1.32  # TJM * 1.32 for social charges
            total_cost = labor.allocated_days * daily_cost

            breakdown.append({
                "category": "Labor Costs",
                "subcategory": "Labor Allocation",
                "description": f"{labor.employee_name} ({labor.role_type}) - {labor.allocated_days} days @ {labor.tjm} MAD/day",
                "amount": total_cost,
                "project_name": project_name,
                "source": "Labor Allocation System",
                "type": "cost"
            })

    # ======= COOP COSTS =======
    for pnl in pnls:
        project_name = pnl.internal_project.name if pnl.internal_project else "Unknown"

        if (pnl.coop_cost_entreprise or 0) > 0:
            breakdown.append({
                "category": "Cooperative Costs",
                "subcategory": "Entreprise Cooperative",
                "description": "Cooperative fees (Entreprise type)",
                "amount": pnl.coop_cost_entreprise or 0,
                "project_name": project_name,
                "source": "P&L System",
                "type": "cost"
            })

        if (pnl.coop_cost_pp or 0) > 0:
            breakdown.append({
                "category": "Cooperative Costs",
                "subcategory": "Personal Provider Cooperative",
                "description": "Cooperative fees (Personal Provider type)",
                "amount": pnl.coop_cost_pp or 0,
                "project_name": project_name,
                "source": "P&L System",
                "type": "cost"
            })

    # ======= TRAVELING COSTS =======
    for pnl in pnls:
        project_name = pnl.internal_project.name if pnl.internal_project else "Unknown"

        if (pnl.working_trip_cost or 0) > 0:
            breakdown.append({
                "category": "Traveling Costs",
                "subcategory": "Working Trip Costs",
                "description": "Java working trip expenses",
                "amount": pnl.working_trip_cost or 0,
                "project_name": project_name,
                "source": "P&L System",
                "type": "cost"
            })

        if (pnl.hosting_cost or 0) > 0:
            breakdown.append({
                "category": "Traveling Costs",
                "subcategory": "Hosting Costs",
                "description": "Java hosting/accommodation costs",
                "amount": pnl.hosting_cost or 0,
                "project_name": project_name,
                "source": "P&L System",
                "type": "cost"
            })

        if (pnl.other_traveling_cost or 0) > 0:
            breakdown.append({
                "category": "Traveling Costs",
                "subcategory": "Other Traveling Costs",
                "description": "Other miscellaneous traveling costs",
                "amount": pnl.other_traveling_cost or 0,
                "project_name": project_name,
                "source": "P&L System",
                "type": "cost"
            })

        if (pnl.other_service_cost or 0) > 0:
            breakdown.append({
                "category": "Traveling Costs",
                "subcategory": "Other Service Costs",
                "description": "Other service-related expenses",
                "amount": pnl.other_service_cost or 0,
                "project_name": project_name,
                "source": "P&L System",
                "type": "cost"
            })

    # ======= FIELD OPERATIONS COSTS =======
    for pnl in pnls:
        project_name = pnl.internal_project.name if pnl.internal_project else "Unknown"

        if (pnl.car_allocation_cost or 0) > 0:
            breakdown.append({
                "category": "Field Operations",
                "subcategory": "Car Allocation",
                "description": "Vehicle allocation and maintenance costs",
                "amount": pnl.car_allocation_cost or 0,
                "project_name": project_name,
                "source": "Fleet Operations System",
                "type": "cost"
            })

        if (pnl.fuel_cost or 0) > 0:
            breakdown.append({
                "category": "Field Operations",
                "subcategory": "Fuel",
                "description": "Fuel and gas expenses",
                "amount": pnl.fuel_cost or 0,
                "project_name": project_name,
                "source": "Fleet Operations System",
                "type": "cost"
            })

        if (pnl.jawaz_cost or 0) > 0:
            breakdown.append({
                "category": "Field Operations",
                "subcategory": "Highway Toll (JAWAZ)",
                "description": "Highway toll and passage fees",
                "amount": pnl.jawaz_cost or 0,
                "project_name": project_name,
                "source": "Fleet Operations System",
                "type": "cost"
            })

        if (pnl.ehs_cost or 0) > 0:
            breakdown.append({
                "category": "Field Operations",
                "subcategory": "EHS Tools & Equipment",
                "description": "Safety and equipment tools costs",
                "amount": pnl.ehs_cost or 0,
                "project_name": project_name,
                "source": "Fleet Operations System",
                "type": "cost"
            })

    # ======= BACKOFFICE COSTS =======
    backoffice_expenses = db.query(models.BackofficeExpense).filter(
        models.BackofficeExpense.period == period_str
    ).all() if year != "OVERALL" else []

    for expense in backoffice_expenses:
        breakdown.append({
            "category": "Backoffice Costs",
            "subcategory": expense.category,
            "description": f"{expense.category} expense",
            "amount": expense.amount or 0,
            "project_name": "Company",
            "source": "Backoffice Expense System",
            "type": "cost"
        })

    # ======= DEPOT LABOR (included in backoffice) =======
    if year != "OVERALL":
        depot_allocs = db.query(models.LaborAllocation).filter(
            models.LaborAllocation.period == period_str,
            models.LaborAllocation.role_type == "DEPOT"
        ).all()

        for depot in depot_allocs:
            if depot.allocated_days and depot.allocated_days > 0:
                daily_cost = (depot.tjm or 0) * 1.32
                total_cost = depot.allocated_days * daily_cost

                breakdown.append({
                    "category": "Backoffice Costs",
                    "subcategory": "DEPOT Labor",
                    "description": f"DEPOT staff - {depot.employee_name} ({depot.allocated_days} days)",
                    "amount": total_cost,
                    "project_name": "Company",
                    "source": "Labor Allocation System",
                    "type": "cost"
                })

    # ======= PERIOD COSTS (if available) =======
    for pnl in pnls:
        project_name = pnl.internal_project.name if pnl.internal_project else "Unknown"

        if (pnl.period_costs or 0) > 0:
            breakdown.append({
                "category": "Period Costs",
                "subcategory": "Overhead Allocation",
                "description": "Pro-rata share of company overhead",
                "amount": pnl.period_costs or 0,
                "project_name": project_name,
                "source": "Backoffice Distribution",
                "type": "cost"
            })

        if (pnl.risk_reserve or 0) > 0:
            breakdown.append({
                "category": "Period Costs",
                "subcategory": "Risk Reserve",
                "description": "Risk and contingency reserve",
                "amount": pnl.risk_reserve or 0,
                "project_name": project_name,
                "source": "P&L System",
                "type": "cost"
            })

    return breakdown