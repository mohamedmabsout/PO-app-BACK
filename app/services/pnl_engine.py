from sqlalchemy.orm import Session
from datetime import datetime
import calendar
from .. import models
from .java_client import JavaApiClient
from sqlalchemy import extract, func
from datetime import date
import re
from sqlalchemy import extract, func
from datetime import date

def generate_draft_pnl_for_month(db: Session, year: int, month: int, generated_by_id: int):
    period_str = f"{year}-{month:02d}"
    
    # 1. Fetch Java Data
    client = JavaApiClient()
    java_data = client.get_monthly_closing_data(year, month)
    
    labor_list = java_data.get("laborSummary",[]) if java_data else []
    expense_list = java_data.get("expenseSummary",[]) if java_data else[]

    # 2. Clear old draft allocations and PNLs for this month
    db.query(models.LaborAllocation).filter(models.LaborAllocation.period == period_str).delete()
    db.query(models.ProjectPnL).filter(models.ProjectPnL.period == period_str).delete()
    db.flush()

    pnl_map = {}

    def get_or_create_pnl(project_id: int):
        if project_id is None:
            return None
            
        if project_id not in pnl_map:
            pnl = models.ProjectPnL(
                internal_project_id=project_id,
                period=period_str,
                status=models.PnLStatus.DRAFT,
                created_by_id=generated_by_id,
                # Explicitly initialize to 0.0 to prevent NoneType errors
                service_revenue=0.0,
                equipment_revenue=0.0,
                coop_cost_pp=0.0,
                working_trip_cost=0.0,
                hosting_cost=0.0,
                labor_cost_field=0.0,
                labor_cost_mgmt=0.0
            )
            db.add(pnl)
            pnl_map[project_id] = pnl
        return pnl_map[project_id]
    all_mapped_pos = db.query(models.MergedPO.site_code, models.MergedPO.internal_project_id).filter(
        models.MergedPO.site_code.isnot(None),
        models.MergedPO.internal_project_id.isnot(None)
    ).all()

    def strict_match_duid(duid_input):
        if not duid_input: return None
        duid_clean = duid_input.strip()
        
        # LEVEL 1: Exact Match
        for sc, pid in all_mapped_pos:
            if sc == duid_clean: return pid
            
        # LEVEL 2: Clean Match (Ignore spaces, dashes, case)
        duid_super_clean = re.sub(r'[\s\-_]', '', duid_clean).lower()
        for sc, pid in all_mapped_pos:
            sc_super_clean = re.sub(r'[\s\-_]', '', sc).lower()
            if duid_super_clean == sc_super_clean: return pid
            
        # We removed the loose substring match to prevent false positives like 'DEPOT' matching 'DEPLOIEMENT'
        return None 


    # --- 3. PROCESS LABOR (Java 'Green P' -> Python LaborAllocation) ---
    count_added = 0
    for lab in labor_list:
        duid = lab.get("duid", "").strip()
        agent_name = lab.get("agentName", "Unknown")
        days = float(lab.get("count", 0))
        tjm = float(lab.get("tjm", 0.0))
        role = lab.get("role", "Unknown")
        # 1. Parse the dates coming from Java
        start_d_str = lab.get("startDate")
        end_d_str = lab.get("endDate")
        
        start_d_obj = datetime.strptime(start_d_str, "%Y-%m-%d").date() if start_d_str else None
        end_d_obj = datetime.strptime(end_d_str, "%Y-%m-%d").date() if end_d_str else None


        if days <= 0: continue

        internal_project_id = None
        
        if "depot" in duid.lower() or "dépôt" in duid.lower():
            # TRACEABILITY: Save the exact row so the PM/Admin can see WHO was at the depot
            alloc = models.LaborAllocation(
                period=period_str,
                start_date=start_d_obj,
                end_date=end_d_obj,
                employee_name=agent_name,
                role_type="DEPOT", # <--- Custom tag!
                tjm=tjm,
                java_project_name=duid,
                java_validated_days=days,
                allocated_days=days,
                internal_project_id=None, # Depot doesn't belong to a specific Huawei project
                allocated_by_id=generated_by_id
            )
            db.add(alloc)
            count_added += 1
            
            continue # Move to the next employee

        else:
            # Normal Project Labor: Use strict matcher
            internal_project_id = strict_match_duid(duid)
            get_or_create_pnl(internal_project_id)

        alloc = models.LaborAllocation(
            period=period_str,
            start_date=start_d_obj, # <--- NEW
            end_date=end_d_obj,     # <--- NEW
            employee_name=agent_name,
            role_type=role,
            tjm=tjm,
            java_project_name=duid, # <--- Pure DUID (No hack)
            java_validated_days=days,
            allocated_days=days,
            internal_project_id=internal_project_id,
            allocated_by_id=generated_by_id
        )
        db.add(alloc)
        count_added += 1

    # --- 4. CALCULATE REVENUES & OTHER COSTS ---
    
    # A. REVENUE (From MergedPO Acceptances)
    active_pos = db.query(
        models.MergedPO.internal_project_id,
        func.sum(models.MergedPO.accepted_ac_amount).label("rev_ac"),
        func.sum(models.MergedPO.accepted_pac_amount).label("rev_pac")
    ).filter(
        models.MergedPO.internal_project_id.isnot(None),
        (
            (extract('year', models.MergedPO.date_ac_ok) == year) & (extract('month', models.MergedPO.date_ac_ok) == month) |
            (extract('year', models.MergedPO.date_pac_ok) == year) & (extract('month', models.MergedPO.date_pac_ok) == month)
        )
    ).group_by(models.MergedPO.internal_project_id).all()

    for po_data in active_pos:
        pnl = get_or_create_pnl(po_data.internal_project_id)
        pnl.service_revenue = (pnl.service_revenue or 0.0) + (po_data.rev_ac or 0.0) + (po_data.rev_pac or 0.0)

    # B. COOPERATION COSTS (Trigger: ACT Creation Date)
    # We sum all ServiceAcceptances created in this month, resolved to project + SBC type.
    # Using the ACT's total_amount_ht so the cost is recognized when the work is accepted,
    # not when the BC reaches APPROVED status.
    active_acts = db.query(
        models.BonDeCommande.project_id,
        models.SBC.sbc_type,
        func.sum(models.ServiceAcceptance.total_amount_ht).label("act_total")
    ).join(
        models.BonDeCommande, models.ServiceAcceptance.bc_id == models.BonDeCommande.id
    ).join(
        models.SBC  # BonDeCommande → SBC (same join path as before)
    ).filter(
        models.BonDeCommande.project_id.isnot(None),
        extract('year', models.ServiceAcceptance.created_at) == year,
        extract('month', models.ServiceAcceptance.created_at) == month
    ).group_by(
        models.BonDeCommande.project_id,
        models.SBC.sbc_type
    ).all()
 
    for act_data in active_acts:
        pnl = get_or_create_pnl(act_data.project_id)
        if pnl:
            amount = float(act_data.act_total or 0.0)
            # Route to the correct bucket based on SBC type
            if act_data.sbc_type == "PP":
                pnl.coop_cost_pp = (pnl.coop_cost_pp or 0.0) + amount
            else:
                pnl.coop_cost_entreprise = (pnl.coop_cost_entreprise or 0.0) + amount
 

    # C. PYTHON CAISSE EXPENSES (Travel & Other)
    # We filter by 'PAID' or 'ACKNOWLEDGED' within this month
    caisse_expenses = db.query(
        models.Expense.project_id,
        models.Expense.exp_type,
        func.sum(models.Expense.amount).label("total")
    ).filter(
        models.Expense.project_id.isnot(None),
        models.Expense.status.in_([models.ExpenseStatus.PAID, models.ExpenseStatus.ACKNOWLEDGED]),
        extract('year', models.Expense.payment_confirmed_at) == year,
        extract('month', models.Expense.payment_confirmed_at) == month
    ).group_by(models.Expense.project_id, models.Expense.exp_type).all()

    for exp in caisse_expenses:
        pnl = get_or_create_pnl(exp.project_id)
        if not pnl: continue
        
        amount = float(exp.total or 0.0)
        
        if exp.exp_type == "Transport":
            pnl.other_traveling_cost = (pnl.other_traveling_cost or 0.0) + amount
        elif exp.exp_type in["Achat", "Service", "Other"]:
            pnl.other_service_cost = (pnl.other_service_cost or 0.0) + amount
            
        # NOTE: We ignore 'ACCEPTANCE_PP' and 'AVANCE_SBC' here because 
        # they are now calculated at the BC creation stage above!


    # D. JAVA TRAVEL EXPENSES (From Java 'Notes de Frais')
    for exp in expense_list:
        duid = exp.get("duid", "").strip()
        exp_type = exp.get("expenseTypeName")
        amount = float(exp.get("totalAmount", 0.0))
        
        if amount <= 0: continue

        internal_project_id = strict_match_duid(duid) # Reuse the matcher we built earlier

        if internal_project_id:
            pnl = get_or_create_pnl(internal_project_id)
            if pnl:
                if exp_type == "Hébergement":
                    pnl.hosting_cost = (pnl.hosting_cost or 0.0) + amount
                elif exp_type in ["Frais journée", "Divers"]:
                    pnl.working_trip_cost = (pnl.working_trip_cost or 0.0) + amount

    db.commit()
    return {"message": "Draft P&L Generated", "labor_records": count_added, "projects_affected": len(pnl_map)}



def recalculate_fleet_pro_rata(db: Session, period_str: str):
    """
    Distributes Fleet Costs (Fuel, Car, Jawaz, EHS) across Project P&Ls 
    based on the percentage of days the Team Leader worked on each project.
    """
    # 1. Reset all fleet costs for this period in the P&L to 0.0 
    # (To prevent infinite addition if we run this multiple times)
    db.query(models.ProjectPnL).filter(models.ProjectPnL.period == period_str).update({
        "car_allocation_cost": 0.0,
        "fuel_cost": 0.0,
        "jawaz_cost": 0.0,
        "ehs_cost": 0.0
    })
    db.flush()

    # 2. Get all Fleet Costs entered by the Gasoil Agent for this month
    fleet_costs = db.query(models.FieldOperationsCost).filter(
        models.FieldOperationsCost.period == period_str
    ).all()

    for fleet in fleet_costs:
        # 3. Find where this specific TL worked this month
        allocations = db.query(models.LaborAllocation).filter(
            models.LaborAllocation.period == period_str,
            models.LaborAllocation.employee_name == fleet.tl_name,
            models.LaborAllocation.internal_project_id.isnot(None) # Only count mapped projects
        ).all()

        # 4. Calculate total days worked by this TL to find the denominator
        total_tl_days = sum(a.allocated_days for a in allocations)
        
        if total_tl_days <= 0:
            continue # TL didn't work, so costs are absorbed as a loss (or ignored)

        # 5. Apply the Pro-Rata Math for each project
        for alloc in allocations:
            ratio = alloc.allocated_days / total_tl_days

            pnl = db.query(models.ProjectPnL).filter(
                models.ProjectPnL.internal_project_id == alloc.internal_project_id,
                models.ProjectPnL.period == period_str
            ).first()

            if pnl:
                pnl.car_allocation_cost += (fleet.car_allocation * ratio)
                pnl.fuel_cost += (fleet.fuel_cost * ratio)
                pnl.jawaz_cost += (fleet.jawaz_cost * ratio)
                pnl.ehs_cost += (fleet.ehs_tools * ratio)

    db.commit()