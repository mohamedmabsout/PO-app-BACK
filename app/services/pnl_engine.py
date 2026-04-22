from sqlalchemy.orm import Session
from datetime import datetime
import calendar
from .. import models
from .java_client import JavaApiClient
from sqlalchemy import and_, case, extract, func, or_
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

    # 2. Build lookup of existing allocations so PM work is preserved on re-fetch
    existing_allocs = db.query(models.LaborAllocation).filter(
        models.LaborAllocation.period == period_str
    ).all()
    # key: (employee_name, java_project_name) -> row
    existing_alloc_map = {(a.employee_name, a.java_project_name): a for a in existing_allocs}
    java_keys_seen = set()

    pnl_map = {}

    def get_or_create_pnl(project_id: int):
        if project_id is None:
            return None

        if project_id not in pnl_map:
            existing_pnl = db.query(models.ProjectPnL).filter(
                models.ProjectPnL.internal_project_id == project_id,
                models.ProjectPnL.period == period_str
            ).first()

            if existing_pnl:
                # Reset only Java-sourced revenue/cost fields; preserve labor, fleet, period_costs
                existing_pnl.service_revenue = 0.0
                existing_pnl.equipment_revenue = 0.0
                existing_pnl.coop_cost_pp = 0.0
                existing_pnl.coop_cost_entreprise = 0.0
                existing_pnl.working_trip_cost = 0.0
                existing_pnl.hosting_cost = 0.0
                existing_pnl.other_traveling_cost = 0.0
                existing_pnl.other_service_cost = 0.0
                pnl_map[project_id] = existing_pnl
            else:
                pnl = models.ProjectPnL(
                    internal_project_id=project_id,
                    period=period_str,
                    status=models.PnLStatus.DRAFT,
                    created_by_id=generated_by_id,
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


    # --- 3. PROCESS LABOR (upsert: update Java fields, preserve PM allocations) ---
    count_added = 0
    count_updated = 0
    for lab in labor_list:
        duid = (lab.get("duid") or "").strip()
        agent_name = lab.get("agentName", "Unknown")
        days = float(lab.get("count", 0))
        tjm = float(lab.get("tjm", 0.0))
        role = lab.get("role", "Unknown")
        start_d_str = lab.get("startDate")
        end_d_str = lab.get("endDate")

        start_d_obj = datetime.strptime(start_d_str, "%Y-%m-%d").date() if start_d_str else None
        end_d_obj = datetime.strptime(end_d_str, "%Y-%m-%d").date() if end_d_str else None

        if days <= 0: continue

        key = (agent_name, duid)
        java_keys_seen.add(key)
        existing = existing_alloc_map.get(key)

        if "depot" in duid.lower() or "dépôt" in duid.lower():
            if existing:
                existing.start_date = start_d_obj
                existing.end_date = end_d_obj
                existing.role_type = "DEPOT"
                existing.tjm = tjm
                existing.java_validated_days = days
                # preserve allocated_days (PM may have adjusted it)
                count_updated += 1
            else:
                db.add(models.LaborAllocation(
                    period=period_str,
                    start_date=start_d_obj,
                    end_date=end_d_obj,
                    employee_name=agent_name,
                    role_type="DEPOT",
                    tjm=tjm,
                    java_project_name=duid,
                    java_validated_days=days,
                    allocated_days=days,
                    internal_project_id=None,
                    allocated_by_id=generated_by_id
                ))
                count_added += 1
            continue

        internal_project_id = strict_match_duid(duid)
        get_or_create_pnl(internal_project_id)

        if existing:
            existing.start_date = start_d_obj
            existing.end_date = end_d_obj
            existing.role_type = role
            existing.tjm = tjm
            existing.java_validated_days = days
            # preserve allocated_days and internal_project_id set by PM
            count_updated += 1
        else:
            db.add(models.LaborAllocation(
                period=period_str,
                start_date=start_d_obj,
                end_date=end_d_obj,
                employee_name=agent_name,
                role_type=role,
                tjm=tjm,
                java_project_name=duid,
                java_validated_days=days,
                allocated_days=days,
                internal_project_id=internal_project_id,
                allocated_by_id=generated_by_id
            ))
            count_added += 1

    # Remove rows that Java no longer reports (employee left the project)
    for key, alloc in existing_alloc_map.items():
        if key not in java_keys_seen:
            db.delete(alloc)

    # --- 4. CALCULATE REVENUES & OTHER COSTS ---
    
            # A. REVENUE (From MergedPO Acceptances)
    active_pos = db.query(
        models.MergedPO.internal_project_id,
        func.sum(case(
            (
                and_(
                    extract('year', models.MergedPO.date_ac_ok) == year,
                    extract('month', models.MergedPO.date_ac_ok) == month
                ),
                models.MergedPO.accepted_ac_amount
            ),
            else_=0
        )).label("rev_ac"),
        func.sum(case(
            (
                and_(
                    extract('year', models.MergedPO.date_pac_ok) == year,
                    extract('month', models.MergedPO.date_pac_ok) == month
                ),
                models.MergedPO.accepted_pac_amount
            ),
            else_=0
        )).label("rev_pac")
    ).join(
        models.InternalProject,
        models.MergedPO.internal_project_id == models.InternalProject.id
    ).filter(   
        models.MergedPO.internal_project_id.isnot(None),
        models.MergedPO.internal_control == 1,
        or_(
            and_(
                extract('year', models.MergedPO.date_ac_ok) == year,
                extract('month', models.MergedPO.date_ac_ok) == month
            ),
            and_(
                extract('year', models.MergedPO.date_pac_ok) == year,
                extract('month', models.MergedPO.date_pac_ok) == month
            )
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
    depot_traveling_costs = {}  # Track by category for backoffice

    for exp in expense_list:
        duid = (exp.get("duid") or "").strip()
        exp_type = exp.get("expenseTypeName", "").strip()
        amount = float(exp.get("totalAmount", 0.0))

        if amount <= 0: continue

        # Check if this expense belongs to DEPOT
        is_depot = "depot" in duid.lower() or "dépôt" in duid.lower()

        if is_depot:
            # Accumulate DEPOT traveling costs by category
            category_map = {
                "Hébergement": "Hosting Costs (Backoffice)",
                "Frais journée": "Working trip Costs (Backoffice)",
                "Frais Gazouil": "other travelling Costs (Backoffice)",
                "Frais divers": "other travelling Costs (Backoffice)"
            }
            category = category_map.get(exp_type, "other travelling Costs (Backoffice)")
            depot_traveling_costs[category] = depot_traveling_costs.get(category, 0.0) + amount
        else:
            # Regular project expenses
            internal_project_id = strict_match_duid(duid)

            if internal_project_id:
                pnl = get_or_create_pnl(internal_project_id)
                if not pnl: continue

                if exp_type == "Hébergement":
                    pnl.hosting_cost = (pnl.hosting_cost or 0.0) + amount
                elif exp_type == "Frais journée":
                    pnl.working_trip_cost = (pnl.working_trip_cost or 0.0) + amount
                elif exp_type == "Frais Gazouil":
                    pnl.other_traveling_cost = (pnl.other_traveling_cost or 0.0) + amount
                elif exp_type == "Frais divers":
                    pnl.other_service_cost = (pnl.other_service_cost or 0.0) + amount
                else:
                    # Unrecognized expense type from Java, log it for future analysis
                    print(f"Unrecognized Java expense type: '{exp_type}' for DUID '{duid}' with amount {amount}")

    # Store DEPOT traveling costs in BackofficeExpense table as system-generated
    if depot_traveling_costs:
        for category, amount in depot_traveling_costs.items():
            if amount > 0:
                # Check if this category already exists for this period
                existing = db.query(models.BackofficeExpense).filter(
                    models.BackofficeExpense.period == period_str,
                    models.BackofficeExpense.category == category
                ).first()

                if existing:
                    # Update with Java data (system-generated takes precedence)
                    existing.amount = amount
                else:
                    # Create new
                    new_exp = models.BackofficeExpense(
                        period=period_str,
                        category=category,
                        amount=amount,
                        entered_by_id=generated_by_id  # Mark as system-generated
                    )
                    db.add(new_exp)

    db.commit()
    return {"pnls_created": len(pnl_map), "labor_rows_added": count_added, "labor_rows_updated": count_updated}


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