from datetime import datetime, date, timedelta, timezone as dt_timezone # Renommé pour éviter le conflit

from select import select
from fastapi import BackgroundTasks
from pytz import timezone
from sqlalchemy.orm import Session
from typing import List, Optional
from . import auth
from . import models, schemas
import pandas as pd
from sqlalchemy.orm import joinedload,Query
import sqlalchemy as sa
from sqlalchemy import func, case, extract, and_,distinct,union_all
from sqlalchemy.sql.functions import coalesce # More explicit import
from sqlalchemy.orm import aliased
from .enum import ProjectType, UserRole, SBCStatus, BCStatus, NotificationType, BCType,AssignmentStatus, ValidationState, ItemGlobalStatus, SBCType
import pandas as pd
import io
import re
import os
import shutil
from pathlib import Path
from .database import SessionLocal # Import the session factory
import logging
from fastapi_mail import FastMail, MessageSchema, MessageType
import secrets
from .config import conf
from datetime import datetime, timedelta
from sqlalchemy import func,or_
from .utils.pdf_generator import generate_act_pdf
logger = logging.getLogger(__name__)


UPLOAD_DIR = "uploads/sbc_docs"

PAYMENT_TERM_MAP = {
    "【TT】▍AC1 (80.00%, INV AC -15D, Complete 80%) / AC2 (20.00%, INV AC -15D, Complete 100%) ▍": "AC1 80 | PAC 20",
    "AC1 (80%, Invoice AC -15D, Complete 80%) / AC2 (20%, Invoice AC -15D, Complete 100%) ▍": "AC1 80 | PAC 20",
    "【TT】▍AC1 (80.00%, INV AC -30D, Complete 80%) / AC2 (20.00%, INV AC -30D, Complete 100%) ▍": "AC1 80 | PAC 20",
    "AC1 (80%, Invoice AC -30D, Complete 80%) / AC2 (20%, Invoice AC -30D, Complete 100%) ▍": "AC1 80 | PAC 20",
    "【TT】▍AC1 (100.00%, INV AC -15D, Complete 100%) ▍": "AC PAC 100%",
    "【TT】▍AC1 (100.00%, INV AC -30D, Complete 100%) ▍": "AC PAC 100%",
    "AC1 (100%, Invoice AC -15D, Complete 100%) ▍": "AC PAC 100%",
    "AC1 (100%, Invoice AC -30D, Complete 100%) ▍": "AC PAC 100%",
    "COD": "AC PAC 100%",
}


def get_user_by_email(db: Session, email: str):
    return db.query(models.User).filter(models.User.email == email).first()


def get_user_by_username(db: Session, username: str):
    return db.query(models.User).filter(models.User.username == username).first()

def get_user(db:Session, user_id=int):
    return db.query(models.User).filter(models.User.id == user_id).first()
    
def create_user(db: Session, user: schemas.UserCreate):
    hashed_password = auth.get_password_hash(user.password)
    # Create a new User model instance, but without the plain password
    db_user = models.User(
        email=user.email,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
        role=user.role,
        hashed_password=hashed_password,
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

def update_user(db: Session, db_user: models.User, user_update: schemas.UserUpdate):
    """
    Updates a user record based on the provided Pydantic schema.
    Only updates fields that were actually sent in the request (exclude_unset=True).
    """
    # 1. Generate a dictionary of updates, ignoring fields that weren't sent
    update_data = user_update.model_dump(exclude_unset=True)

    # 2. Iterate through the dictionary and update the SQLAlchemy object
    for key, value in update_data.items():
        setattr(db_user, key, value)

    # 3. Save changes
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    
    return db_user
def get_users(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.User).offset(skip).limit(limit).all()


def get_project(db: Session, project_id: int):
    return db.query(models.Project).filter(models.Project.id == project_id).first()


# Function to get a project by its name
def get_project_by_name(db: Session, name: str):
    return db.query(models.Project).filter(models.Project.name == name).first()


# Function to get a list of all projects
def get_projects(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Project).offset(skip).limit(limit).all()
def get_all_projects(db: Session):
    """Returns all project records from the database."""
    return db.query(models.Project).order_by(models.Project.name).all()
def get_all_internal_projects(db: Session):
    """Returns all internal project records from the database."""
    return db.query(models.InternalProject).order_by(models.InternalProject.name).all()
def get_all_sites(db:Session):
    return db.query(models.Site).order_by(models.Site.site_code).all()

# Function to create a new project
def create_project(db: Session, project: schemas.ProjectCreate):
    project_data = project.model_dump()

    # Step 2: Intercept and convert the date strings
    # We define the expected format from the frontend/Excel ('DD/MM/YYYY')
    date_format = "%d/%m/%Y"  # Use %Y for 4-digit year, %m for month, %d for day

    if project_data.get("start_date"):
        # strptime means "string parse time" - it converts a string to a datetime object
        project_data["start_date"] = datetime.strptime(
            project_data["start_date"], date_format
        ).date()

    if project_data.get("plan_end_date"):
        project_data["plan_end_date"] = datetime.strptime(
            project_data["plan_end_date"], date_format
        ).date()

    # Step 3: Create the SQLAlchemy model instance using the MODIFIED dictionary
    db_project = models.Project(**project_data)
    db.add(db_project)
    db.commit()
    db.refresh(db_project)
    return db_project


def delete_project(db: Session, project_id: int):
    db_project = (
        db.query(models.Project).filter(models.Project.id == project_id).first()
    )
    if db_project:
        db.delete(db_project)
        db.commit()
    return db_project


def get_or_create(db: Session, model, **kwargs):
    instance = db.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False # Returns instance and "was not created" flag
    else:
        instance = model(**kwargs)
        db.add(instance)
        db.flush() # Use flush to get the ID without committing
        return instance, True # Returns instance and "was created" flag

def create_raw_purchase_orders_from_dataframe(db: Session, df: pd.DataFrame, user_id: int):    # Standardize column names from the Excel file - This part is perfect.
    df.rename(columns={
        'PO NO.': 'po_no', 'PO Line NO.': 'po_line_no', 'Project Name': 'project_code',
        'Site Code': 'site_code', 'Customer': 'customer','PO Status': 'po_status',
        'Item Description': 'item_description', 'Payment Terms': 'payment_terms_raw',
        'Unit Price': 'unit_price', 'Requested Qty': 'requested_qty', 'Publish Date': 'publish_date'
    }, inplace=True, errors='ignore')
    if 'publish_date' in df.columns:
        # Ensure it is a datetime object
        df['publish_date'] = pd.to_datetime(df['publish_date'], errors='coerce')
        
        # Define the specific date to target (Jan 1, 2026)
        target_date = pd.Timestamp("2026-01-01")
        
        # Create a mask for rows that match exactly this date
        mask = df['publish_date'].dt.date == target_date.date()
        
        # Shift those specific rows back by 1 day
        df.loc[mask, 'publish_date'] = df.loc[mask, 'publish_date'] - pd.Timedelta(days=1)

    df['uploader_id'] = user_id
    
    # Hydrate Customers and Sites ONLY
    customer_map = {name: get_or_create(db, models.Customer, name=name)[0] for name in df['customer'].dropna().unique()} if 'customer' in df.columns else {}
    site_map = {code: get_or_create(db, models.Site, site_code=code)[0] for code in df['site_code'].dropna().unique()} if 'site_code' in df.columns else {}
    db.commit() # Commit new customers/sites

    df['customer_id'] = df['customer'].map({c.name: c.id for c in customer_map.values()}) if customer_map else None
    df['site_id'] = df['site_code'].map({s.site_code: s.id for s in site_map.values()}) if site_map else None
    
    model_columns = [c.key for c in models.RawPurchaseOrder.__table__.columns if c.key != 'id']
    df_to_insert = df[[col for col in model_columns if col in df.columns]]
    
    records = df_to_insert.to_dict("records")
    db.bulk_insert_mappings(models.RawPurchaseOrder, records)
    db.commit()
    return len(records)
def get_internal_project_id_from_rules(db: Session, customer_project_name: str, tbd_project_id: int):
    """
    Applies assignment rules to a customer project name to find the correct internal project ID.
    """
    if not customer_project_name:
        return tbd_project_id

    # Fetch all rules from the database
    # In a high-performance system, this could be cached.
    rules = db.query(models.ProjectAssignmentRule).all()

    for rule in rules:
        if rule.rule_type == "STARTS_WITH" and customer_project_name.startswith(rule.pattern):
            return rule.internal_project_id
        elif rule.rule_type == "ENDS_WITH" and customer_project_name.endswith(rule.pattern):
            return rule.internal_project_id
        elif rule.rule_type == "CONTAINS" and rule.pattern in customer_project_name:
            return rule.internal_project_id
            
    # If no rules match, return the ID for the "To Be Determined" project
    return tbd_project_id




def resolve_internal_project(
    db: Session, 
    site_id: int, 
    site_code: str, 
    publish_date: datetime, 
    customer_project_id: int,
    tbd_project_id: int
):
    """
    Advanced Matching Logic:
    1. Manual Override (Global Site Allocation)
    2. Rules (Ordered by Priority DESC) -> First full match wins
    3. Default (TBD)
    """
    
    # 1. Manual Override (Highest Priority)
    if site_id:
        allocation = db.query(models.SiteProjectAllocation).filter(
            models.SiteProjectAllocation.site_id == site_id
        ).first()
        if allocation:
            return allocation.internal_project_id

    if not site_code:
        return tbd_project_id

    # 2. Fetch Rules ordered by Priority (Highest first)
    # Optimization: In a huge system, cache this list in Redis or memory
    rules = db.query(models.SiteAssignmentRule).order_by(
        models.SiteAssignmentRule.id.desc()
    ).all()

    # 3. Iterate and Check Conditions
    for rule in rules:
        # A. String Checks
        if rule.starts_with and not site_code.startswith(rule.starts_with):
            continue # Fail
        if rule.ends_with and not site_code.endswith(rule.ends_with):
            continue # Fail
        if rule.contains_str and rule.contains_str not in site_code:
            continue # Fail
            
        # B. Context Checks
        if rule.customer_project_id and rule.customer_project_id != customer_project_id:
            continue # Fail
            
        # C. Date Checks (Comparing Date vs DateTime, strictly need .date())
        p_date = publish_date.date() if isinstance(publish_date, datetime) else publish_date
        
        if rule.min_publish_date and p_date and p_date < rule.min_publish_date:
            continue
        if rule.max_publish_date and p_date and p_date > rule.max_publish_date:
            continue

        # If we survived all checks, this is the winner!
        return rule.internal_project_id

    # 4. No rule matched -> TBD
    return tbd_project_id
def apply_rule_retrospective(db: Session, rule: models.SiteAssignmentRule):
    """
    Re-evaluates TBD items against the SPECIFIC new rule.
    """
    tbd_project = db.query(models.InternalProject).filter_by(name="To Be Determined").first()
    if not tbd_project: return 0

    # 1. Get candidate POs (Only TBD ones to save performance)
    candidates = db.query(models.MergedPO).filter(
        models.MergedPO.internal_project_id == tbd_project.id
    ).all()

    updates = []
    
    # 2. Python-side Check
    for po in candidates:
        # Check String patterns
        if rule.starts_with and not (po.site_code and po.site_code.startswith(rule.starts_with)): continue
        if rule.ends_with and not (po.site_code and po.site_code.endswith(rule.ends_with)): continue
        if rule.contains_str and not (po.site_code and rule.contains_str in po.site_code): continue
        
        # Check Context
        if rule.customer_project_id and po.customer_project_id != rule.customer_project_id: continue
        
        # Check Dates
        p_date = po.publish_date.date() if po.publish_date else None
        if rule.min_publish_date and (not p_date or p_date < rule.min_publish_date): continue
        if rule.max_publish_date and (not p_date or p_date > rule.max_publish_date): continue
        
        # Match Found!
        updates.append(po.id)

    # 3. Bulk Update
    if updates:
        db.query(models.MergedPO).filter(models.MergedPO.id.in_(updates)).update(
            {models.MergedPO.internal_project_id: rule.internal_project_id},
            synchronize_session=False
        )
        db.commit()
    
    return len(updates)


def process_and_merge_pos(db: Session):
    # 1. Ensure "To Be Determined" Project exists
    tbd_project = db.query(models.InternalProject).filter_by(name="To Be Determined").first()
    if not tbd_project:
        # Assuming you updated InternalProject to handle Enum or string for project_type
        tbd_project = models.InternalProject(name="To Be Determined", project_type=ProjectType.TBD) 
        db.add(tbd_project)
        db.commit()
    tbd_project_id = tbd_project.id

    # 2. Fetch Unprocessed Data
    unprocessed_pos = db.query(models.RawPurchaseOrder).filter(
        models.RawPurchaseOrder.is_processed == False
    ).options(joinedload(models.RawPurchaseOrder.site)).all()
    
    if not unprocessed_pos:
        return 0

    # 3. Hydrate Customer Projects (Just creating labels now)
    customer_project_names = {po.project_code for po in unprocessed_pos if po.project_code}
    existing_cust_projs = {p.name: p for p in db.query(models.CustomerProject).filter(models.CustomerProject.name.in_(customer_project_names)).all()}
    
    for name in customer_project_names:
        if name not in existing_cust_projs:
            # Note: No internal_project_id passed here anymore
            new_cust_proj = models.CustomerProject(name=name)
            db.add(new_cust_proj)
    
    db.commit()
    all_cust_projs_map = {p.name: p for p in db.query(models.CustomerProject).filter(models.CustomerProject.name.in_(customer_project_names)).all()}

    # 4. De-duplicate logic (Same as before)
    unique_pos_map = {}
    for po in unprocessed_pos:
        key = (po.po_no, po.po_line_no)
        if key not in unique_pos_map or po.publish_date > unique_pos_map[key].publish_date:
            unique_pos_map[key] = po
    clean_pos_list = list(unique_pos_map.values())

    # 5. Process Merge
    po_ids_to_check = [f"{po.po_no}-{po.po_line_no}" for po in clean_pos_list]
    existing_merged_map = {mp.po_id: mp for mp in db.query(models.MergedPO).filter(models.MergedPO.po_id.in_(po_ids_to_check)).all()}

    for po in clean_pos_list:
        po_id = f"{po.po_no}-{po.po_line_no}"
        customer_project = all_cust_projs_map.get(po.project_code)
        if not customer_project: continue

        final_internal_project_id = resolve_internal_project(
            db, 
            site_id=po.site_id, 
            site_code=po.site.site_code if po.site else None,
            # NEW ARGUMENTS PASSED HERE:
            publish_date=po.publish_date,
            customer_project_id=customer_project.id,
            tbd_project_id=tbd_project_id
        )


        if po_id in existing_merged_map:
            # UPDATE
            merged_po = existing_merged_map[po_id]
            
            # --- FINANCIAL UPDATES (Always Apply) ---
            if po.requested_qty == 0:
                merged_po.requested_qty = 0
                merged_po.line_amount_hw = 0
            else:
                merged_po.requested_qty = po.requested_qty
                merged_po.unit_price = po.unit_price
                merged_po.line_amount_hw = (po.unit_price or 0) * (po.requested_qty or 0)
            
            merged_po.publish_date = po.publish_date
            merged_po.site_id = po.site_id
            merged_po.site_code = po.site.site_code if po.site else None
            
            # --- ASSIGNMENT PRESERVATION LOGIC (THE FIX) ---
            # 1. Is the PO currently unassigned or TBD?
            current_is_tbd = (merged_po.internal_project_id is None) or \
                             (merged_po.internal_project_id == tbd_project_id)
            
            # 2. Only update if it is currently TBD
            if current_is_tbd:
                merged_po.internal_project_id = final_internal_project_id
            
            # NOTE: If it is ALREADY assigned to Project A, we DO NOT overwrite it with TBD.
            # This preserves manual assignments or historical rule matches.

        else:
            # INSERT (New PO)
            # For new POs, we always apply the resolved project (Rule or TBD)
            new_merged_po = models.MergedPO(
                po_id=po_id,
                raw_po_id=po.id,
                customer_project_id=customer_project.id,
                internal_project_id=final_internal_project_id, 
                site_id=po.site_id,
                site_code=po.site.site_code if po.site else None,
                po_no=po.po_no,
                po_line_no=po.po_line_no,
                item_description=po.item_description,
                payment_term=PAYMENT_TERM_MAP.get(po.payment_terms_raw, "UNKNOWN"),
                unit_price=po.unit_price,
                requested_qty=po.requested_qty,
                line_amount_hw=(po.unit_price or 0) * (po.requested_qty or 0),
                publish_date=po.publish_date,
            )
            db.add(new_merged_po)


    # 6. Cleanup
    unprocessed_ids = [po.id for po in unprocessed_pos]
    if unprocessed_ids:
        db.query(models.RawPurchaseOrder).filter(models.RawPurchaseOrder.id.in_(unprocessed_ids)).update({"is_processed": True})
    
    db.commit()
    return len(clean_pos_list)
def process_po_file_background(file_path: str, history_id: int, user_id: int):
    """
    Background task to process the PO file.
    """
    # Create a NEW database session for this background task
    db = SessionLocal() 
    
    try:
        # 1. Read the file from the temp path
        df = pd.read_excel(file_path)
        
        # 2. Run the existing logic
        # Note: We pass the db session we just created
        new_record_ids = create_raw_purchase_orders_from_dataframe(db, df, user_id)
        processed_count = process_and_merge_pos(db)
        
        # 3. Update History to SUCCESS
        history = db.query(models.UploadHistory).get(history_id)
        if history:
            history.status = "SUCCESS"
            history.total_rows = processed_count # Or updated_count, whichever you prefer to track
            history.notes = f"Processed {processed_count} MergedPOs"
            db.commit()
            
        # Optional: Send Notification to User here using create_notification
        create_notification(
            db, 
            recipient_id=user_id,
            type=models.NotificationType.APP,
            title="Acceptance Import Complete",
            message=f"File processed successfully. {processed_count} Merged POs updated.",
            link="/site-dispatcher"
        )
        db.commit()
    except Exception as e:
        # 4. Handle Errors
        logger.error(f"Background Task Failed: {e}", exc_info=True) # exc_info gives the traceback
        db.rollback()
        history = db.query(models.UploadHistory).get(history_id)
        if history:
            history.status = "FAILED"
            history.error_message = str(e)[:500] # Truncate error if too long
            db.commit()
            
    finally:
        # 5. Cleanup
        db.close()
        # Delete the temp file to save space
        if os.path.exists(file_path):
            os.remove(file_path)
# backend/app/crud.py

def process_acceptance_file_background(file_path: str, history_id: int, user_id: int):
    """
    Background task: Reads file, saves raw data, triggers processing, updates history.
    """
    db = SessionLocal()
    try:
        # 1. Read and Clean the Excel File
        acceptance_df = pd.read_excel(file_path)
        
        # Standardize Headers (Excel -> DB Column Names)
        column_mapping = {
            'ShipmentNO.': 'shipment_no', 
            'AcceptanceQty': 'acceptance_qty', 
            'ApplicationProcessed': 'application_processed_date',
            'PONo.': 'po_no', 
            'POLineNo.': 'po_line_no', 
        }
        acceptance_df.rename(columns=column_mapping, inplace=True)
        
        # Data Type Conversion & Validation
        acceptance_df['application_processed_date'] = pd.to_datetime(acceptance_df['application_processed_date'], errors='coerce')
        for col in ['acceptance_qty', 'po_line_no', 'shipment_no']:
            acceptance_df[col] = pd.to_numeric(acceptance_df[col], errors='coerce').fillna(0)

        # Drop invalid rows
        acceptance_df.dropna(subset=['po_no', 'po_line_no'], inplace=True)

        # 2. Save Raw Data and GET THE IDs
        # We need the IDs to ensure we only process THIS file's data
        new_record_ids = create_raw_acceptances_from_dataframe(db, acceptance_df, user_id)

        # 3. Process Only These Specific Records
        updated_count = process_acceptances_by_ids(db, new_record_ids)

        # 4. Success: Update History & Notify
        history_record = db.query(models.UploadHistory).get(history_id)
        if history_record:
            history_record.status = "SUCCESS"
            history_record.total_rows = len(new_record_ids)
            # Optional: Add details about how many POs were actually updated
            # history_record.error_message = f"Updated {updated_count} POs." 
            db.commit()

        # Create Notification
        create_notification(
            db, 
            recipient_id=user_id,
            type=models.NotificationType.APP,
            title="Acceptance Import Complete",
            message=f"File processed successfully. {updated_count} Merged POs updated.",
            link="/site-dispatcher"
        )
        db.commit()

    except Exception as e:
        # 5. Error Handling
        logger.error(f"Background Task Failed: {e}", exc_info=True)
        db.rollback()
        history = db.query(models.UploadHistory).get(history_id)
        if history:
            history.status = "FAILED"
            history.error_message = str(e)[:500] 
            db.commit()
            
    finally:
        # 6. Cleanup
        db.close()
        if os.path.exists(file_path):
            os.remove(file_path)

def get_all_po_data(db: Session):
    """
    Retourne les MergedPO avec le nom du projet interne dans le champ project_name
    pour que le frontend puisse l'utiliser directement.
    """
    rows = (
        db.query(models.MergedPO)
        .options(
            joinedload(models.MergedPO.internal_project)  # charge la relation
        )
        .all()
    )

    result = []
    for po in rows:
        d = po.__dict__.copy()
        # On enlève l’état SQLAlchemy qui casse la serialisation JSON
        d.pop("_sa_instance_state", None)

        # 👉 ICI : on injecte le nom du projet
        d["project_name"] = (
            po.internal_project.name if po.internal_project else None
        )

        result.append(d)

    return result


def create_po_data_from_dataframe(db: Session, df: pd.DataFrame, user_id: int):
    df['uploader_id'] = user_id

    records = df.to_dict(orient="records")
    db.bulk_insert_mappings(models.RawPurchaseOrder, records)
    db.commit()

def create_raw_acceptances_from_dataframe(db: Session, df: pd.DataFrame, user_id: int) -> List[int]:
    """
    Saves DataFrame to RawAcceptance table and returns the list of new IDs.
    """
    df['uploader_id'] = user_id
    
    # Filter to ensure we only try to save columns that exist in the model
    valid_columns = [c.key for c in models.RawAcceptance.__table__.columns if c.key != 'id']
    df_final = df[[c for c in df.columns if c in valid_columns]]

    records = df_final.to_dict("records")
    
    # Create objects
    new_instances = [models.RawAcceptance(**rec) for rec in records]
    
    # Add and Commit to generate IDs
    db.add_all(new_instances)
    db.commit()
    
    # Return the IDs
    return [instance.id for instance in new_instances]



def create_upload_history_record(
    db: Session,
    filename: str,
    status: str,
    user_id: int,
    total_rows: int = 0,
    error_msg: str = None,
):
    history_record = models.UploadHistory(
        original_filename=filename,
        status=status,
        user_id=user_id,
        total_rows=total_rows,
        error_message=error_msg,
        uploaded_at=datetime.utcnow()
    )
    db.add(history_record)
    db.commit()
    db.refresh(history_record) 

    return history_record


def get_upload_history(db: Session, skip: int = 0, limit: int = 100):
    # Use order_by to get the most recent uploads first
    return (
        db.query(models.UploadHistory)
        .order_by(models.UploadHistory.uploaded_at.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )

def get_eligible_pos_for_bc(
    db: Session, 
    project_id: int, 
    site_codes: Optional[List[str]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    """
    Fetches POs for the project that have REMAINING Quantity > 0.
    IGNORES usage from Rejected BCs.
    """
    
    # --- FIX STARTS HERE ---
    
    # Subquery: Sum of quantities used in VALID (non-rejected) BCs per PO
    used_subquery = db.query(
        models.BCItem.merged_po_id,
        func.sum(models.BCItem.quantity_sbc).label("used_qty")
    ).join(
        models.BonDeCommande, models.BCItem.bc_id == models.BonDeCommande.id
    ).filter(
        # Only count items if the BC is NOT rejected
        models.BonDeCommande.status != models.BCStatus.REJECTED 
    ).group_by(models.BCItem.merged_po_id).subquery()

    # --- FIX ENDS HERE ---

    # Main Query: Left Join MergedPO with the Usage Subquery
    query = db.query(models.MergedPO).outerjoin(
        used_subquery, models.MergedPO.id == used_subquery.c.merged_po_id
    ).filter(
        models.MergedPO.internal_project_id == project_id,
        
        # CRITICAL FILTER: Requested Qty > Valid Used Qty
        # We use a small epsilon (0.0001) for float safety
        models.MergedPO.requested_qty > (func.coalesce(used_subquery.c.used_qty, 0) + 0.0001)
    )

    # Standard Filters
    if site_codes and len(site_codes) > 0:
        clean_codes = [c.strip() for c in site_codes if c.strip()]
        if clean_codes:
            query = query.filter(models.MergedPO.site_code.in_(clean_codes))

    if start_date:
        query = query.filter(func.date(models.MergedPO.publish_date) >= start_date)
    if end_date:
        query = query.filter(func.date(models.MergedPO.publish_date) <= end_date)
        
    return query.all()
def get_raw_po_data_as_dataframe(
    db: Session,
    status: Optional[str] = None,
    project_name: Optional[str] = None,
    search: Optional[str] = None,
    # We are not including 'category' as it's not in the table
) -> pd.DataFrame:
    """
    Queries the raw RawPurchaseOrder table with optional filters and returns a DataFrame.
    """
    # Start with a base query on the RawPurchaseOrder table
    query = db.query(models.RawPurchaseOrder)

    # Apply filters to the query if they are provided
    if status:
        # The 'po_status' column in the table matches this filter
        query = query.filter(models.RawPurchaseOrder.po_status == status)

    if project_name:
        # The 'project_code' column in the table matches this filter
        query = query.filter(models.RawPurchaseOrder.project_code == project_name)

    if search:
        # Create a search filter for PO number
        search_term = f"%{search}%"
        query = query.filter(models.RawPurchaseOrder.po_no.ilike(search_term))

    # Execute the query and read the results directly into a Pandas DataFrame
    df = pd.read_sql(query.statement, db.bind)

    # Return the DataFrame, which includes all columns from the table
    return df


def get_merged_po_data_as_dataframe(
    db: Session,
    status: Optional[str] = None,
    category: Optional[str] = None,
    project_name: Optional[str] = None,
    search: Optional[str] = None
) -> pd.DataFrame:
    """
    Queries the MergedPO table with optional filters and returns the result as a Pandas DataFrame.
    """
    # Start with a base query on the MergedPO table
    query = db.query(models.MergedPO)

    # Apply filters to the query if they are provided.
    # Note: These rely on the 'status' and 'category' columns existing in MergedPO.
    if status:
        query = query.filter(models.MergedPO.status == status)
    if category:
        query = query.filter(models.MergedPO.category == category)
    if project_name:
        query = query.filter(models.MergedPO.project_name == project_name)
    if search:
        search_term = f"%{search}%"
        query = query.filter(
            (models.MergedPO.po_no.ilike(search_term)) |
            (models.MergedPO.item_description.ilike(search_term)) |
            (models.MergedPO.site_code.ilike(search_term))
        )

    # Execute the query and read the results directly into a Pandas DataFrame
    # This efficiently converts the SQLAlchemy query result to a DataFrame.
    df = pd.read_sql(query.statement, db.bind)
    
    return df

import re

def deduce_category(description: str) -> str:
    """
    Robustly deduces the category based on keywords in the item description.
    Priority is given to specific keywords like 'Transportation', 'Survey', etc.
    """
    if not isinstance(description, str) or not description.strip():
        return "TBD"

    desc_lower = description.lower()

    # --- CATEGORY RULES ---
    # The order matters! Put more specific categories first.
    
    # 1. Transportation
    # Keywords: transportation, transport, km<distance, vehicle, tractor, driver
    if any(k in desc_lower for k in ["transport", "distance<=", "vehicle", "tractor", "driver", "per time"]):
        return "Transportation"

    # 2. Site Engineer / FSC
    # Keywords: fsc, site engineer, work order, rigger
    if any(k in desc_lower for k in ["fsc", "site engineer", "work order", "rigger"]):
        return "Site Engineer"

    # 3. Survey
    # Keywords: survey, report(level
    if any(k in desc_lower for k in ["survey", "tssr", "level a", "level b"]):
        return "Survey"
        
    # 4. Civil Work (New Category recommended based on your data)
    # Keywords: civil work, foundation, excavation, concrete, painting, wall opening
    if any(k in desc_lower for k in ["civil work", "foundation", "excavation", "concrete", "paint", "wall opening", "soil"]):
        return "Civil Work"

    # 5. Hardware / Material (New Category recommended)
    # Keywords: supply, cable, antenna, rack, battery, cabinet, pvc, rod, connector
    # But ONLY if it doesn't say "installation" explicitly (which implies service)
    # This is tricky. Often "Supply and Install" is Service. 
    # Pure material: "Connector-Item...", "RF coaxial connector"
    if any(k in desc_lower for k in ["connector", "jumper", "packaging", "box", "screw", "bolt"]):
        if "install" not in desc_lower:
             return "Material"

    # 6. Service (Catch-all for installation/swap/expansion)
    # Keywords: install, swap, expansion, dismantling, commissioning, integration, upgrade, replacement
    service_keywords = [
        "service", "install", "swap", "expansion", "dismantling", 
        "commissioning", "integration", "upgrade", "replacement", 
        "zone", "configuration", "acceptance", "testing"
    ]
    if any(k in desc_lower for k in service_keywords):
        return "Service"

    # 7. Fallback for specific equipment mentions that usually imply service
    if any(k in desc_lower for k in ["rru", "bbu", "aau", "bts", "msan", "olt", "wdm", "microwave"]):
        return "Service"

    return "TBD"

def process_acceptances_by_ids(db: Session, raw_acceptance_ids: List[int]):
    """
    Processes specific RawAcceptance records by ID.
    Replicates exact logic: Aggregate -> Deduce Category -> Calc AC/PAC -> Update.
    """
    if not raw_acceptance_ids:
        return 0

    # 1. Fetch only the requested raw records
    query_raw = db.query(models.RawAcceptance).filter(
        models.RawAcceptance.id.in_(raw_acceptance_ids),
        models.RawAcceptance.is_processed == False
    )
    
    # Read into DataFrame for easy aggregation
    acceptance_df = pd.read_sql(query_raw.statement, db.bind)
    
    if acceptance_df.empty:
        return 0

    # 2. Generate IDs for Aggregation (Exact same logic as before)
    acceptance_df['po_id'] = acceptance_df['po_no'] + '-' + acceptance_df['po_line_no'].astype(int).astype(str)
    acceptance_df['id2'] = acceptance_df['po_id'] + '-' + acceptance_df['shipment_no'].astype(int).astype(str)

    # 3. Aggregate (Sum qty, take max date)
    aggregated_df = acceptance_df.groupby("id2").agg(
        acceptance_qty=("acceptance_qty", "sum"),
        application_processed_date=("application_processed_date", "max"),
        po_id=("po_id", "first"),
        shipment_no=("shipment_no", "first"),
    ).reset_index()

    # 4. Fetch related MergedPOs
    po_ids_to_update = aggregated_df['po_id'].unique().tolist()
    merged_po_records = db.query(models.MergedPO).filter(models.MergedPO.po_id.in_(po_ids_to_update)).all()
    merged_po_map = {mp.po_id: mp for mp in merged_po_records}
    
    updated_count = 0
    updated_po_ids = set()

    # 5. Calculation Loop
    for index, acceptance_row in aggregated_df.iterrows():
        po_id = acceptance_row['po_id']
        
        if po_id in merged_po_map:
            merged_po_to_update = merged_po_map[po_id]
            updated_po_ids.add(po_id)
            
            # 1. Get the date from the file
            raw_processed_date = acceptance_row['application_processed_date']
            
            # --- 🚨 TIMEZONE FIX: 2026-01-01 -> 2025-12-31 🚨 ---
            # If the date is valid and is exactly Jan 1, 2026, shift it back.
            final_processed_date = None
            if pd.notna(raw_processed_date):
                date_obj = raw_processed_date.date()
                if date_obj == date(2026, 1, 1):
                    final_processed_date = date(2025, 12, 31)
                else:
                    final_processed_date = date_obj
            # ----------------------------------------------------

            unit_price = merged_po_to_update.unit_price or 0
            req_qty = merged_po_to_update.requested_qty or 0
            agg_acceptance_qty = acceptance_row['acceptance_qty']
            shipment_no = acceptance_row['shipment_no']

            merged_po_to_update.category = deduce_category(merged_po_to_update.item_description)
            payment_term = merged_po_to_update.payment_term

            # --- Apply the CORRECTED date to AC/PAC fields ---

            if shipment_no == 1:
                merged_po_to_update.total_ac_amount = unit_price * req_qty * 0.80
                merged_po_to_update.accepted_ac_amount = unit_price * agg_acceptance_qty * 0.80
                # Use the fixed date here
                merged_po_to_update.date_ac_ok = final_processed_date 
                
                if payment_term == "AC PAC 100%":
                    merged_po_to_update.total_pac_amount = unit_price * req_qty * 0.20
                    merged_po_to_update.accepted_pac_amount = unit_price * agg_acceptance_qty * 0.20
                    # Use the fixed date here
                    merged_po_to_update.date_pac_ok = final_processed_date 

            elif shipment_no == 2:
                if payment_term == "AC1 80 | PAC 20":
                    merged_po_to_update.total_pac_amount = unit_price * req_qty * 0.20
                    merged_po_to_update.accepted_pac_amount = unit_price * agg_acceptance_qty * 0.20
                    # Use the fixed date here
                    merged_po_to_update.date_pac_ok = final_processed_date


            # --- Logic: Update Total Acceptance Qty (Optional - remove if column doesn't exist) ---
            # Since you got an error here, I am removing this block to respect your current DB schema.
            # If you WANT this, you must run the migration first. 
            # For now, I'm commenting it out to fix the crash.
            # if hasattr(merged_po_to_update, 'total_acceptance_qty'):
            #     if merged_po_to_update.total_acceptance_qty is None:
            #         merged_po_to_update.total_acceptance_qty = 0.0
            #     merged_po_to_update.total_acceptance_qty += agg_acceptance_qty

    # 6. Mark as Processed & Commit
    query_raw.update({"is_processed": True})
    db.commit()

    return len(updated_po_ids)
# --- FINAL REVISED FUNCTION ---
def process_acceptance_dataframe(db: Session, acceptance_df: pd.DataFrame):
    """
    Processes a DataFrame of acceptance data, deduces categories, calculates AC/PAC values,
    and updates the MergedPO table in the database.
    """
    # --- Phase 1: Pre-process and Aggregate the Acceptance Data ---

    # Standardize column names from the Acceptance Excel file
    acceptance_df.rename(
        columns={
            "PONo.": "po_no",
            "POLineNo.": "po_line_no",
            "ShipmentNO.": "shipment_no",
            "AcceptanceQty": "acceptance_qty",
            "ApplicationProcessed": "application_processed_date",  # The new date source
        },
        inplace=True,
        errors="ignore",
    )  # 'ignore' prevents errors if a column is missing
    if 'application_processed_date' in acceptance_df.columns:
        # Ensure datetime
        acceptance_df['application_processed_date'] = pd.to_datetime(
            acceptance_df['application_processed_date'], errors='coerce'
        )
        
        target_date = pd.Timestamp("2026-01-01") 
        mask = acceptance_df['application_processed_date'].dt.date == target_date.date()
        
        # Shift back 1 day
        acceptance_df.loc[mask, 'application_processed_date'] -= pd.Timedelta(days=1)
    unprocessed_acceptances_query = db.query(models.RawAcceptance).filter(models.RawAcceptance.is_processed == False)
    unprocessed_acceptances = unprocessed_acceptances_query.all()
    
    if not unprocessed_acceptances:
        return 0

    # Convert the raw data to a DataFrame to use our existing Pandas logic
    acceptance_df = pd.read_sql(unprocessed_acceptances_query.statement, db.bind)
    # Ensure correct data types
    acceptance_df["po_no"] = acceptance_df["po_no"].astype(str)
    acceptance_df["po_line_no"] = pd.to_numeric(
        acceptance_df["po_line_no"], errors="coerce"
    )
    acceptance_df["shipment_no"] = pd.to_numeric(
        acceptance_df["shipment_no"], errors="coerce"
    )
    acceptance_df["acceptance_qty"] = pd.to_numeric(
        acceptance_df["acceptance_qty"], errors="coerce"
    )
    acceptance_df["application_processed_date"] = pd.to_datetime(
        acceptance_df["application_processed_date"], errors="coerce"
    )

    # Drop rows where essential data is missing
    acceptance_df.dropna(
        subset=[
            "po_no",
            "po_line_no",
            "shipment_no",
            "acceptance_qty",
            "application_processed_date",
        ],
        inplace=True,
    )

    # Generate the 'po_id' and 'id2' for aggregation
    acceptance_df["po_id"] = (
        acceptance_df["po_no"]
        + "-"
        + acceptance_df["po_line_no"].astype(int).astype(str)
    )
    acceptance_df["id2"] = (
        acceptance_df["po_id"]
        + "-"
        + acceptance_df["shipment_no"].astype(int).astype(str)
    )

    # Aggregate duplicate id2 rows
    aggregated_df = (
        acceptance_df.groupby("id2")
        .agg(
            acceptance_qty=("acceptance_qty", "sum"),
            application_processed_date=("application_processed_date", "max"),
            po_id=("po_id", "first"),
            shipment_no=("shipment_no", "first"),
        )
        .reset_index()
    )
    print("--- DEBUG: Acceptance Data ---")
    print(f"Found {len(aggregated_df)} unique acceptance records to process.")
    if not aggregated_df.empty:
        print(
            "First 5 po_ids from Acceptance file:",
            aggregated_df["po_id"].head().tolist(),
        )
    # --- END DEBUGGING ---
    # --- Phase 2 & 3: Calculate and Update ---

    po_ids_to_update = aggregated_df['po_id'].unique().tolist()
    if not po_ids_to_update:
        return 0

    merged_po_records = db.query(models.MergedPO).filter(models.MergedPO.po_id.in_(po_ids_to_update)).all()
    merged_po_map = {mp.po_id: mp for mp in merged_po_records}
    
    updated_records = []

    for index, acceptance_row in aggregated_df.iterrows():
        po_id = acceptance_row['po_id']
        
        if po_id in merged_po_map:
            merged_po_to_update = merged_po_map[po_id]
            updated_records.append(po_id)
            
            unit_price = merged_po_to_update.unit_price or 0
            req_qty = merged_po_to_update.requested_qty or 0
            agg_acceptance_qty = acceptance_row['acceptance_qty']
            latest_processed_date = acceptance_row['application_processed_date'].date() if pd.notna(acceptance_row['application_processed_date']) else None
            shipment_no = acceptance_row['shipment_no']

            # Deduce and Update Category
            merged_po_to_update.category = deduce_category(merged_po_to_update.item_description)

            # --- REVISED AC/PAC CALCULATION LOGIC ---
            payment_term = merged_po_to_update.payment_term

            # Case 1: Processing a Shipment 1 record
            if shipment_no == 1:
                # Always calculate AC for Shipment 1
                merged_po_to_update.total_ac_amount = unit_price * req_qty * 0.80
                merged_po_to_update.accepted_ac_amount = unit_price * agg_acceptance_qty * 0.80
                merged_po_to_update.date_ac_ok = latest_processed_date
                
                # ONLY calculate PAC if the payment term is "ACPAC 100%"
                if payment_term == "AC PAC 100%":
                    merged_po_to_update.total_pac_amount = unit_price * req_qty * 0.20
                    merged_po_to_update.accepted_pac_amount = unit_price * agg_acceptance_qty * 0.20
                    merged_po_to_update.date_pac_ok = latest_processed_date # Same date as AC

            # Case 2: Processing a Shipment 2 record
            elif shipment_no == 2:
                # ONLY calculate PAC, and ONLY if the payment term is "AC1 80 | PAC 20"
                if payment_term == "AC1 80 | PAC 20":
                    merged_po_to_update.total_pac_amount = unit_price * req_qty * 0.20
                    merged_po_to_update.accepted_pac_amount = unit_price * agg_acceptance_qty * 0.20
                    merged_po_to_update.date_pac_ok = latest_processed_date
            
            # For any other shipment number (3, 4, etc.), no calculations are performed.
    
    unprocessed_acceptances_query.update({"is_processed": True})
    
    db.commit()

    return len(set(updated_records))

def get_filtered_merged_pos(
    db: Session,
    internal_project_id: Optional[int] = None,
    customer_project_id: Optional[int] = None,
    site_code: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    search: Optional[str] = None
) -> Query:
    """
    Builds a SQLAlchemy Query for the MergedPO table with multiple optional filters.
    """
    # Start with a base query and eagerly load relationships to prevent N+1 query problem.
    # This makes the API faster.
    query = db.query(models.MergedPO).options(
        joinedload(models.MergedPO.customer_project),joinedload(models.MergedPO.internal_project),
        joinedload(models.MergedPO.site)
    )

   
    if internal_project_id:
        # FIX: Filter directly on MergedPO, not via CustomerProject join
        query = query.filter(models.MergedPO.internal_project_id == internal_project_id)
    
    if customer_project_id:
        query = query.filter(models.MergedPO.customer_project_id == customer_project_id)
    
    if site_code:
        query = query.filter(models.MergedPO.site_code == site_code)
    if category:
        query = query.filter(models.MergedPO.category == category)
    if start_date:
        query = query.filter(sa.func.date(models.MergedPO.publish_date) >= start_date)

    if end_date:
        query = query.filter(sa.func.date(models.MergedPO.publish_date) <= end_date)
        
    if search:
        search_term = f"%{search}%"
        # Search on MergedPO fields AND related fields
        # We join explicitly for the search columns
        query = query.join(models.CustomerProject, isouter=True)\
                     .join(models.InternalProject, isouter=True) # Direct join
                     
        query = query.filter(
            (models.MergedPO.po_id.ilike(search_term)) |
            (models.MergedPO.item_description.ilike(search_term)) |
            (models.InternalProject.name.ilike(search_term)) | 
            (models.CustomerProject.name.ilike(search_term))
        )
        
    return query
def get_total_financial_summary(db: Session, user: models.User = None) -> dict:
    # base_query = db.query(models.MergedPO)
    query = db.query(
        func.sum(models.MergedPO.line_amount_hw).label("total_po_value"),
        func.sum(models.MergedPO.accepted_ac_amount).label("total_accepted_ac"),
        func.sum(models.MergedPO.accepted_pac_amount).label("total_accepted_pac")
    )
    canceled_query = db.query(
        func.sum(models.MergedPO.line_amount_hw).label("total_canceled")
    ).filter(models.MergedPO.internal_control == 0)


    # --- THIS IS THE FIX ---
    # If a user is provided and their role is PM, filter the data
    if user and user.role in [UserRole.PM]:
        # Join with InternalProject to access the project_manager_id
        query = query.join(
            models.InternalProject, 
            models.MergedPO.internal_project_id == models.InternalProject.id
        )
        # Add the WHERE clause
        query = query.filter(models.InternalProject.project_manager_id == user.id)
    # -----------------------
    
    # Execute the (now possibly filtered) query
    result = query.one()
    remaining_gap = result.total_po_value - (result.total_accepted_ac + result.total_accepted_pac)

    total_po_value = result.total_po_value or 0.0
    total_accepted_ac = result.total_accepted_ac or 0.0
    total_accepted_pac = result.total_accepted_pac or 0.0
    total_canceled = canceled_query.scalar() or 0.0


    return {
        "total_po_value": total_po_value,
        "total_accepted_ac": total_accepted_ac,
        "total_accepted_pac": total_accepted_pac,
        "remaining_gap": remaining_gap,
        "total_canceled": total_canceled 

    }
def get_internal_projects_financial_summary(db: Session, user: models.User = None):
    # 1. Fetch APPROVED data grouped by Internal Project AND Project Manager
    results = db.query(
        models.InternalProject.id.label("project_id"),
        models.InternalProject.name.label("project_name"),
        models.InternalProject.project_manager_id.label("project_manager_id"),
        # --- NEW: Select PM Name Fields ---
        models.User.first_name.label("pm_first_name"),
        models.User.last_name.label("pm_last_name"),
        # ----------------------------------
        func.coalesce(func.sum(models.MergedPO.line_amount_hw), 0).label("total_po_value"),
        (
            func.coalesce(func.sum(models.MergedPO.accepted_ac_amount), 0) + 
            func.coalesce(func.sum(models.MergedPO.accepted_pac_amount), 0)
        ).label("total_accepted")
    ).outerjoin(
        models.MergedPO, 
        and_(
            models.InternalProject.id == models.MergedPO.internal_project_id,
            models.MergedPO.assignment_status == models.AssignmentStatus.APPROVED
        )
    ).outerjoin( # --- NEW: Join User Table to get PM details ---
        models.User,
        models.InternalProject.project_manager_id == models.User.id
    ).group_by(
        models.InternalProject.id, 
        models.InternalProject.name,
        # --- NEW: Group by PM fields ---
        models.User.first_name,
        models.User.last_name
    ).all()

    # 2. Fetch "Pending Approval" data (The Limbo Money)
    pending_stats = db.query(
        func.coalesce(func.sum(models.MergedPO.line_amount_hw), 0).label("po_value"),
        func.coalesce(func.sum(models.MergedPO.accepted_ac_amount), 0).label("ac_value"),
        func.coalesce(func.sum(models.MergedPO.accepted_pac_amount), 0).label("pac_value")
    ).filter(
        models.MergedPO.assignment_status == models.AssignmentStatus.PENDING_APPROVAL
    ).first()
    
    pending_po = float(pending_stats.po_value) if pending_stats else 0.0
    pending_accepted = (float(pending_stats.ac_value) + float(pending_stats.pac_value)) if pending_stats else 0.0

    # 3. Identify TBD Project
    tbd_project = db.query(models.InternalProject).filter(models.InternalProject.name == "To Be Determined").first()
    tbd_id = tbd_project.id if tbd_project else -1

    summary_list = []
    
    for row in results:
        # Role Filtering
        if user and user.role in [models.UserRole.PM, models.UserRole.PD]:
             if row.project_manager_id != user.id:
                 continue

        po_value = float(row.total_po_value)
        accepted = float(row.total_accepted)

        # CRITICAL: If this is TBD, add the Pending money to it
        if row.project_id == tbd_id:
            po_value += pending_po
            accepted += pending_accepted

        gap = po_value - accepted
        completion = (accepted / po_value * 100) if po_value > 0 else 0.0
        
        # --- NEW: Construct PM Object ---
        pm_info = None
        if row.pm_first_name or row.pm_last_name:
            pm_info = {
                "first_name": row.pm_first_name,
                "last_name": row.pm_last_name
            }
        
        summary_list.append({
            "project_id": row.project_id,
            "project_name": row.project_name,
            "project_manager": pm_info, # Pass the object or None
            "total_po_value": po_value,
            "total_accepted": accepted,
            "remaining_gap": gap,
            "completion_percentage": completion
        })
        
    return summary_list

def get_customer_projects_financial_summary(db: Session):
    results = db.query(
        models.CustomerProject.id.label("project_id"),
        models.CustomerProject.name.label("project_name"),
        func.coalesce(func.sum(models.MergedPO.line_amount_hw), 0).label("total_po_value"),
        (
            func.coalesce(func.sum(models.MergedPO.accepted_ac_amount), 0) + 
            func.coalesce(func.sum(models.MergedPO.accepted_pac_amount), 0)
        ).label("total_accepted")
    ).outerjoin( 
        models.MergedPO, 
        and_(
            models.CustomerProject.id == models.MergedPO.customer_project_id,
            # --- UPDATE: Filter inside the JOIN ---
            models.MergedPO.assignment_status == models.AssignmentStatus.APPROVED
        )
    ).group_by(models.CustomerProject.id, models.CustomerProject.name).all()
    
    summary_list = []
    for row in results:
        po_value = float(row.total_po_value)
        accepted = float(row.total_accepted)
        
        gap = po_value - accepted
        completion = (accepted / po_value * 100) if po_value > 0 else 0.0
        
        summary_list.append({
            "project_id": row.project_id,
            "project_name": row.project_name,
            "total_po_value": po_value,
            "total_accepted": accepted,
            "remaining_gap": gap,
            "completion_percentage": completion
        })
    return summary_list

def get_po_value_by_category(db: Session, user: models.User = None):
    """
    Calculates the total PO value for each category, correctly filtered by user role.
    """
    # Define the category label once for reuse
    category_label = coalesce(models.MergedPO.category, "TBD").label("category_name")

    # --- THIS IS THE FIX ---

    # 1. Start the query from the main table, MergedPO.
    base_query = db.query(models.MergedPO)

    # 2. Apply the role-based filter by joining to InternalProject.
    #    This must be done BEFORE the final select and group_by.
    if user and user.role in [UserRole.PM]:
        base_query = base_query.join(
            models.InternalProject, 
            models.MergedPO.internal_project_id == models.InternalProject.id
        ).filter(
            models.InternalProject.project_manager_id == user.id
        )

    # 3. Now, with the correct FROM and JOIN clauses established,
    #    select the final columns and apply the GROUP BY.
    final_query = base_query.with_entities(
        category_label,
        func.sum(models.MergedPO.line_amount_hw).label("total_value")
    ).group_by(category_label)

    # -----------------------

    results = final_query.all()
    
    return [{"category": row.category_name, "value": row.total_value or 0} for row in results]



def get_remaining_to_accept_paginated(
    db: Session, 
    page: int = 1, 
    size: int = 20, 
    filter_stage: str = "ALL",
    # --- NEW FILTERS ---
    search: Optional[str] = None,
    internal_project_id: Optional[int] = None,
    customer_project_id: Optional[int] = None,
    project_manager_id: Optional[int] = None,
        user: models.User = None  # <-- Add user parameter

):
    # 1. Define SQL Expressions (Same as before)
    remaining_expr = models.MergedPO.line_amount_hw - (
        func.coalesce(models.MergedPO.accepted_ac_amount, 0) + 
        func.coalesce(models.MergedPO.accepted_pac_amount, 0)
    )
    
    stage_expr = case(
        (models.MergedPO.date_ac_ok.is_(None), "WAITING_AC"),
        (and_(models.MergedPO.date_ac_ok.isnot(None), models.MergedPO.date_pac_ok.is_(None)), "WAITING_PAC"),
        else_="PARTIAL_GAP"
    )

    # 2. Build Base Query (Only items with remaining money)
    query = db.query(
        models.MergedPO,
        remaining_expr.label("remaining_amount"),
        stage_expr.label("remaining_stage")
    ).filter(
        func.abs(remaining_expr) > 0.01
    )

    if user and user.role in [UserRole.PM]:
        query = query.join(
            models.InternalProject,
            models.MergedPO.internal_project_id == models.InternalProject.id
        ).filter(models.InternalProject.project_manager_id == user.id)

    # 3. Apply Filters
    if filter_stage != "ALL":
        query = query.filter(stage_expr == filter_stage)
        
    if internal_project_id:
        query = query.filter(models.MergedPO.internal_project_id == internal_project_id)
        
    if customer_project_id:
        query = query.filter(models.MergedPO.customer_project_id == customer_project_id)
    if project_manager_id:
        query = query.join(
            models.InternalProject,
            models.MergedPO.internal_project_id == models.InternalProject.id
        ).filter(models.InternalProject.project_manager_id == project_manager_id)
    if search:
        term = f"%{search}%"
        query = query.filter(
            (models.MergedPO.po_no.ilike(term)) | 
            (models.MergedPO.site_code.ilike(term)) |
            (models.MergedPO.item_description.ilike(term))
        )

    # 4. Pagination
    total_items = query.count()
    results = query.order_by(models.MergedPO.publish_date.desc())\
                   .offset((page - 1) * size).limit(size).all()

    # 5. Format Output
    items = []
    for po, rem_amount, stage in results:
        po_dict = po.__dict__
        po_dict['remaining_amount'] = rem_amount
        po_dict['remaining_stage'] = stage
        # Eager loading might put objects here, ensure we return serializable data
        if po.internal_project: po_dict['internal_project_name'] = po.internal_project.name
        items.append(po_dict)

    return {
        "items": items,
        "total_items": total_items,
        "page": page,
        "size": size,
        "total_pages": (total_items + size - 1) // size
    }

# NEW HELPER: Get Stats efficiently without fetching all rows
def get_remaining_stats(db: Session,user: models.User = None) -> dict:
    remaining_expr = models.MergedPO.line_amount_hw - (
        func.coalesce(models.MergedPO.accepted_ac_amount, 0) + func.coalesce(models.MergedPO.accepted_pac_amount, 0)
    )
    stage_expr = case(
        (models.MergedPO.date_ac_ok.is_(None), "WAITING_AC"),
        (and_(models.MergedPO.date_ac_ok.isnot(None), models.MergedPO.date_pac_ok.is_(None)), "WAITING_PAC"),
        else_="PARTIAL_GAP"
    )
    
    base_query = db.query(
    stage_expr.label("stage"),
    func.count(models.MergedPO.id).label("count"),
    func.sum(remaining_expr).label("total_gap")
    ).filter(
        func.abs(remaining_expr) > 0.01
    )

    # --- THIS IS THE FIX ---
    # Apply the same role-based filter
    if user and user.role in [UserRole.PM]:
        base_query = base_query.join(
            models.InternalProject,
            models.MergedPO.internal_project_id == models.InternalProject.id
        ).filter(models.InternalProject.project_manager_id == user.id)
    # -----------------------

    # Now group by and execute on the (potentially filtered) query
    stats = base_query.group_by(stage_expr).all()

    # Initialize all buckets to 0
    all_stages = ["WAITING_AC", "WAITING_PAC", "PARTIAL_GAP"]
    result_dict = {stage: {"count": 0, "gap": 0.0} for stage in all_stages}

    # Populate the dictionary with actual results
    for row in stats:
        if row.stage in result_dict:
            result_dict[row.stage] = {"count": row.count, "gap": row.total_gap or 0}

    return result_dict


def get_financial_summary_by_period(
    db: Session, 
    year: int, 
    month: Optional[int] = None, 
    week: Optional[int] = None,
    user: Optional[models.User] = None  # <-- Added user parameter
) -> dict:
    """
    Calculates the financial summary for a specific period, filtered by user role if applicable.
    """
    
    # --- Define base query and role-based filtering ---
    base_query = db.query(models.MergedPO)

    # If a user is provided and they are a PM or PD, join and filter by their projects.
    if user and user.role in [UserRole.PM]:
        base_query = base_query.join(
            models.InternalProject, 
            models.MergedPO.internal_project_id == models.InternalProject.id
        ).filter(models.InternalProject.project_manager_id == user.id)

    # --- Define date filters for each metric ---
    po_date_filters = [extract('year', models.MergedPO.publish_date) == year]
    ac_date_filters = [extract('year', models.MergedPO.date_ac_ok) == year]
    pac_date_filters = [extract('year', models.MergedPO.date_pac_ok) == year]

    if month:
        po_date_filters.append(extract('month', models.MergedPO.publish_date) == month)
        ac_date_filters.append(extract('month', models.MergedPO.date_ac_ok) == month)
        pac_date_filters.append(extract('month', models.MergedPO.date_pac_ok) == month)

    if week:
        # Note: func.week(..., 3) is MySQL specific for Monday-start weeks.
        po_date_filters.append(func.week(models.MergedPO.publish_date, 3) == week)
        ac_date_filters.append(func.week(models.MergedPO.date_ac_ok, 3) == week)
        pac_date_filters.append(func.week(models.MergedPO.date_pac_ok, 3) == week)
        # Only count POs that have been formally approved for their project
    # status_filter = (models.MergedPO.assignment_status == models.AssignmentStatus.APPROVED)

    # --- Perform conditional aggregation on the (potentially filtered) base_query ---
    summary = base_query.with_entities(
        # Add the status filter to every SUM condition using AND
        func.sum(case((and_(*po_date_filters), models.MergedPO.line_amount_hw), else_=0)).label("total_po_value"),
        func.sum(case((and_(*ac_date_filters), models.MergedPO.accepted_ac_amount), else_=0)).label("total_accepted_ac"),
        func.sum(case((and_(*pac_date_filters), models.MergedPO.accepted_pac_amount), else_=0)).label("total_accepted_pac")
    ).one()


    # Process results (no change here)
    total_po_value = summary.total_po_value or 0.0
    total_accepted_ac = summary.total_accepted_ac or 0.0
    total_accepted_pac = summary.total_accepted_pac or 0.0
    remaining_gap = total_po_value - (total_accepted_ac + total_accepted_pac)

    return {
        "total_po_value": total_po_value,
        "total_accepted_ac": total_accepted_ac,
        "total_accepted_pac": total_accepted_pac,
        "remaining_gap": remaining_gap,
    }

# --- Also, let's fix the get_yearly_chart_data function ---

def get_yearly_chart_data(db: Session, year: int, user: models.User = None):
    # Base query for MergedPO
    base_query = db.query(models.MergedPO)
    
    # Filter: Only Approved POs
    base_query = base_query.filter(models.MergedPO.assignment_status == models.AssignmentStatus.APPROVED)

    # Filter: User Role (PM/PD)
    if user and user.role in [models.UserRole.PM, models.UserRole.PD]:
        # Since MergedPO doesn't link directly to InternalProject anymore,
        # we must join through CustomerProject -> InternalProject
        base_query = base_query.join(models.CustomerProject).join(models.InternalProject).filter(
            models.InternalProject.project_manager_id == user.id
        )

    # --- Identify Active Months ---
    month_col = extract('month', models.MergedPO.publish_date).label("month_num")
    
    po_months = base_query.with_entities(month_col).filter(extract('year', models.MergedPO.publish_date) == year)
    
    ac_months = base_query.with_entities(
        extract('month', models.MergedPO.date_ac_ok).label("month_num")
    ).filter(extract('year', models.MergedPO.date_ac_ok) == year)
    
    pac_months = base_query.with_entities(
        extract('month', models.MergedPO.date_pac_ok).label("month_num")
    ).filter(extract('year', models.MergedPO.date_pac_ok) == year)
    
    all_months_query = union_all(po_months, ac_months, pac_months).subquery()
    active_months_query = db.query(distinct(all_months_query.c.month_num))
    active_months = [row[0] for row in active_months_query.all()]

    # --- Fetch Data ---
    monthly_data = []
    for month in active_months:
        if not month: continue

        # The helper function 'get_financial_summary_by_period' MUST also be updated 
        # to filter by APPROVED status (as provided in the previous response).
        summary = get_financial_summary_by_period(db=db, year=year, month=month, user=user)
        
        total_paid = (summary.get("total_accepted_ac", 0) or 0) + (summary.get("total_accepted_pac", 0) or 0)
        monthly_data.append({
            "month": month,
            "total_po_value": summary.get("total_po_value", 0) or 0,
            "total_paid": total_paid
        })
        
    return sorted(monthly_data, key=lambda x: x['month'])



def get_export_dataframe(
    db: Session,
    internal_project_id: Optional[int] = None,
    customer_project_id: Optional[int] = None,
    site_code: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    search: Optional[str] = None
) -> pd.DataFrame:
    
    CustProj = aliased(models.CustomerProject)
    IntProj = aliased(models.InternalProject)
    remaining_expr = (models.MergedPO.line_amount_hw - (
        func.coalesce(models.MergedPO.accepted_ac_amount, 0) + 
        func.coalesce(models.MergedPO.accepted_pac_amount, 0)
    ))

    # --- CORRECTED & COMPLETE SELECT STATEMENT ---
    query = db.query(
        # 1. Start with the required new fields, PO ID first
        models.MergedPO.po_id.label("PO ID"),
        func.concat(models.User.first_name, " ", models.User.last_name).label("PM"),
        models.MergedPO.unit_price.label("Unit Price"),
        models.MergedPO.requested_qty.label("Requested Qty"),
        models.MergedPO.internal_control.label("Internal Check"),
        models.MergedPO.payment_term.label("Payment Term"),
        
        # 2. Now add all the original fields back
        IntProj.name.label("Internal Project"),
        CustProj.name.label("Customer Project"),
        models.MergedPO.site_code.label("Site Code"),
        models.MergedPO.po_no.label("PO No."),
        models.MergedPO.po_line_no.label("PO Line No."),
        models.MergedPO.item_description.label("Item Description"),
        models.MergedPO.category.label("Category"),
        models.MergedPO.publish_date.label("Publish Date"),
        models.MergedPO.line_amount_hw.label("Line Amount"),
        
        # 3. AC/PAC and Remaining columns
        models.MergedPO.total_ac_amount.label("Total AC (80%)"),
        models.MergedPO.accepted_ac_amount.label("Accepted AC Amount"),
        models.MergedPO.date_ac_ok.label("Date AC OK"),
        models.MergedPO.total_pac_amount.label("Total PAC (20%)"),
        models.MergedPO.accepted_pac_amount.label("Accepted PAC Amount"),
        models.MergedPO.date_pac_ok.label("Date PAC OK"),
        remaining_expr.label("Remaining Amount"),
        (remaining_expr * models.MergedPO.internal_control).label("Real Backlog")


    ).select_from(models.MergedPO)

    # --- JOINS ---
    # Join everything needed for the selected columns and filters
    query = query.join(IntProj, models.MergedPO.internal_project_id == IntProj.id, isouter=True)
    query = query.join(models.User, IntProj.project_manager_id == models.User.id, isouter=True)
    query = query.join(CustProj, models.MergedPO.customer_project_id == CustProj.id, isouter=True)

    # --- FILTERS (No change) ---
    if internal_project_id:
        query = query.filter(IntProj.id == internal_project_id)
    # ... (rest of filters) ...
    if customer_project_id:
        query = query.filter(CustProj.id == customer_project_id)
    if site_code:
        query = query.filter(models.MergedPO.site_code == site_code)
    if category:
        query= query.filter(models.MergedPO.category == category)
    if start_date:
        query = query.filter(sa.func.date(models.MergedPO.publish_date) >= start_date)
    if end_date:
        query = query.filter(sa.func.date(models.MergedPO.publish_date) <= end_date)
    if search:
        search_term = f"%{search}%"
        query = query.filter(
            (models.MergedPO.po_id.ilike(search_term)) |
            (models.MergedPO.item_description.ilike(search_term)) |
            (IntProj.name.ilike(search_term)) |
            (CustProj.name.ilike(search_term))
        )

    df = pd.read_sql(query.statement, db.bind)
    
    # --- DATA CLEANING (No change) ---
    if "Remaining Amount" in df.columns:
        df["Remaining Amount"] = df["Remaining Amount"].round(5)
        df.loc[df["Remaining Amount"].abs() < 1, "Remaining Amount"] = 0
    if "Real Backlog" in df.columns:
        df["Real Backlog"] = df["Real Backlog"].round(5)
        df.loc[df["Real Backlog"].abs() < 1, "Real Backlog"] = 0
       
    return df


def get_internal_project_by_name(db: Session, name: str):
    return db.query(models.InternalProject).filter(models.InternalProject.name == name).first()

def get_internal_project(db: Session, project_id: int):
    return db.query(models.InternalProject).filter(models.InternalProject.id == project_id).first()

def create_internal_project(db: Session, project: schemas.InternalProjectCreate):
    # Convert Pydantic model to dictionary
    db_project = models.InternalProject(**project.model_dump())
    db.add(db_project)
    db.commit()
    db.refresh(db_project)
    return db_project

def get_sites_for_internal_project(db: Session, project_id: int):
    """
    Returns a list of all unique Sites currently assigned to this Internal Project.
    We derive this from the MergedPO table to see what is *actually* active.
    """
    # Query distinct sites associated with this project in the MergedPO table
    sites = db.query(models.Site).join(models.MergedPO).filter(
        models.MergedPO.internal_project_id == project_id
    ).distinct().all()
    
    return sites
def get_sites_for_internal_project_paginated(
    db: Session, 
    project_id: int, 
    page: int = 1, 
    size: int = 50,
    search: Optional[str] = None
):
    """
    Returns paginated MergedPO records for a specific project (e.g., TBD),
    ensuring one record per site_code, compatible with ONLY_FULL_GROUP_BY.
    """
    # 1. Build the base query with filters
    base_query = db.query(models.MergedPO).filter(
        models.MergedPO.internal_project_id == project_id
    )

    if search:
        base_query = base_query.filter(models.MergedPO.site_code.ilike(f"%{search}%") | (models.MergedPO.customer_project.has(models.CustomerProject.name.ilike(f"%{search}%")))   )

    # 2. Calculate total items by counting DISTINCT site_codes
    count_query = base_query.with_entities(func.count(distinct(models.MergedPO.site_code)))
    total_items = count_query.scalar()

    # --- THIS IS THE FIX ---

    # 3. Create a subquery that finds the minimum ID for each site_code group.
    #    This gives us a unique identifier for one representative row per site.
    subq = base_query.with_entities(
        func.min(models.MergedPO.id).label("min_id")
    ).group_by(models.MergedPO.site_code).subquery()

    # 4. Now, build the main query to select MergedPO objects whose ID is in our subquery's list of min_ids.
    #    This effectively selects just one row for each site_code.
    main_query = db.query(models.MergedPO).options(
        joinedload(models.MergedPO.internal_project),
        joinedload(models.MergedPO.customer_project)
    ).join(
        subq, models.MergedPO.id == subq.c.min_id
    )

    # 5. Apply ordering, pagination, and execute.
    #    The GROUP BY is no longer needed in the final query.
    merged_po_items = main_query.order_by(models.MergedPO.site_code)\
                                .offset((page - 1) * size)\
                                .limit(size).all()
    
    # -----------------------

    return {
        "items": merged_po_items,
        "total_items": total_items,
        "page": page,
        "per_page": size,
        "total_pages": (total_items + size - 1) // size if total_items > 0 else 1
    }
def update_internal_project(db: Session, project_id: int, updates: schemas.InternalProjectUpdate):
    db_project = db.query(models.InternalProject).filter(models.InternalProject.id == project_id).first()
    if not db_project:
        return None
    
    update_data = updates.model_dump(exclude_unset=True)
    
    for key, value in update_data.items():
        setattr(db_project, key, value)
    
    db.commit()
    db.refresh(db_project)
    return db_project
def get_user_performance_stats(
    db: Session, 
    user_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> dict:
    """
    Calculates financial performance for a specific PM over a flexible Date Range.
    """
    
    # Base filter: Only projects managed by this user
    base_filters = [models.InternalProject.project_manager_id == user_id]

    # Initialize filters for each metric
    po_filters = base_filters.copy()
    ac_filters = base_filters.copy()
    pac_filters = base_filters.copy()

    # Apply Date Range Logic
    if start_date:
        po_filters.append(func.date(models.MergedPO.publish_date) >= start_date)
        ac_filters.append(models.MergedPO.date_ac_ok >= start_date)
        pac_filters.append(models.MergedPO.date_pac_ok >= start_date)

    if end_date:
        po_filters.append(func.date(models.MergedPO.publish_date) <= end_date)
        ac_filters.append(models.MergedPO.date_ac_ok <= end_date)
        pac_filters.append(models.MergedPO.date_pac_ok <= end_date)

    # Execute Conditional Aggregation
    summary = db.query(
        # 1. PO Value (Based on Publish Date)
        func.sum(case((and_(*po_filters), models.MergedPO.line_amount_hw), else_=0)).label("total_po_value"),
        
        # 2. AC Value (Based on AC Date)
        func.sum(case((and_(*ac_filters), models.MergedPO.accepted_ac_amount), else_=0)).label("total_accepted_ac"),
        
        # 3. PAC Value (Based on PAC Date)
        func.sum(case((and_(*pac_filters), models.MergedPO.accepted_pac_amount), else_=0)).label("total_accepted_pac")
        
    ).join(
        models.InternalProject, 
        models.MergedPO.internal_project_id == models.InternalProject.id
    ).one()

    # Calculate Totals
    total_po = summary.total_po_value or 0.0
    total_accepted = (summary.total_accepted_ac or 0.0) + (summary.total_accepted_pac or 0.0)
    remaining = total_po - total_accepted
    
    completion = (total_accepted / total_po * 100) if total_po > 0 else 0.0

    return {
        "total_po_value": total_po,
        "total_accepted": total_accepted,
        "remaining_gap": remaining,
        "completion_percentage": completion
    }
def set_user_target(db: Session, target: schemas.UserTargetCreate):
    """
    Creates or Updates a target (Upsert).
    """
    db_target = db.query(models.UserPerformanceTarget).filter(
        models.UserPerformanceTarget.user_id == target.user_id,
        models.UserPerformanceTarget.year == target.year,
        models.UserPerformanceTarget.month == target.month
    ).first()

    if db_target:
        # Update existing
        if target.target_po_amount is not None:
            db_target.po_monthly_update = target.target_po_amount
        if target.target_invoice_amount is not None:
            db_target.acceptance_monthly_update = target.target_invoice_amount
    else:
        # Create new
        db_target = models.UserPerformanceTarget(**target.model_dump())
        db.add(db_target)
    
    db.commit()
    return db_target

# In crud.py

def get_performance_matrix(
    db: Session, 
    year: int, 
    month: Optional[int] = None, 
    filter_user_id: Optional[int] = None,
    current_user: models.User = None
):
    # 1. Get eligible users (Standard logic)
    pms_to_process = []
    if current_user and current_user.role in [UserRole.PM]:
        pms_to_process = [current_user]
    else: 
        pms_to_process = db.query(models.User).filter(
            models.User.role.in_(['PM', 'ADMIN', 'PD'])
        ).all()
        
    results = []

    for pm in pms_to_process:
        # A. Fetch Targets (Plan) - Remains the same
        target_query = db.query(
            func.sum(models.UserPerformanceTarget.po_monthly_update),
            func.sum(models.UserPerformanceTarget.acceptance_monthly_update)
        ).filter(
            models.UserPerformanceTarget.user_id == pm.id,
            models.UserPerformanceTarget.year == year
        )
        if month:
            target_query = target_query.filter(models.UserPerformanceTarget.month == month)
            
        plan_po, plan_invoice = target_query.first()
        plan_po = plan_po or 0.0
        plan_invoice = plan_invoice or 0.0

        # --- B. Fetch Actuals (UPDATED LOGIC) ---
        # Include ALL items assigned to this PM (Approved OR Pending)
        # We filter by the PM ID on the InternalProject table.
        base_filters = [
            models.InternalProject.project_manager_id == pm.id,
            # CRITICAL CHANGE: We DO NOT filter by assignment_status here. 
            # If it is assigned to this PM's project, it counts for them.
            # This covers both APPROVED and PENDING_APPROVAL.
        ]
        
        # Period Filters
        po_date_filters = base_filters + [extract('year', models.MergedPO.publish_date) == year]
        ac_date_filters = base_filters + [extract('year', models.MergedPO.date_ac_ok) == year]
        pac_date_filters = base_filters + [extract('year', models.MergedPO.date_pac_ok) == year]
        
        if month:
            po_date_filters.append(extract('month', models.MergedPO.publish_date) == month)
            ac_date_filters.append(extract('month', models.MergedPO.date_ac_ok) == month)
            pac_date_filters.append(extract('month', models.MergedPO.date_pac_ok) == month)

        summary = db.query(
            func.sum(case((and_(*po_date_filters), models.MergedPO.line_amount_hw), else_=0)),
            func.sum(case((and_(*ac_date_filters), models.MergedPO.accepted_ac_amount), else_=0)),
            func.sum(case((and_(*pac_date_filters), models.MergedPO.accepted_pac_amount), else_=0))
        ).join(
            models.CustomerProject, models.MergedPO.customer_project_id == models.CustomerProject.id
        ).join(
            models.InternalProject, models.MergedPO.internal_project_id == models.InternalProject.id
        ).first()

        actual_po_period = summary[0] or 0.0
        actual_paid_period = (summary[1] or 0.0) + (summary[2] or 0.0)

        # --- C. Fetch LIFETIME GAP (UPDATED LOGIC) ---
        # Same logic: Include everything assigned to this PM's projects.
        lifetime_summary = db.query(
            func.sum(models.MergedPO.line_amount_hw),
            func.sum(models.MergedPO.accepted_ac_amount),
            func.sum(models.MergedPO.accepted_pac_amount)
        ).join(
            models.InternalProject, models.MergedPO.internal_project_id == models.InternalProject.id
        ).filter(
            models.InternalProject.project_manager_id == pm.id
            # Again, NO status filter. Counts pending and approved.
        ).first()
        
        lifetime_po = lifetime_summary[0] or 0.0
        lifetime_paid = (lifetime_summary[1] or 0.0) + (lifetime_summary[2] or 0.0)
        total_lifetime_gap = lifetime_po - lifetime_paid

        # --- E. Final Result Construction ---
        results.append({
            "user_id": pm.id,
            "user_name": f"{pm.first_name} {pm.last_name}",
            "total_gap": total_lifetime_gap, 
            "plan_po": plan_po,
            "actual_po": actual_po_period,
            "percent_po": (actual_po_period / plan_po * 100) if plan_po > 0 else 0,
            "plan_invoice": plan_invoice,
            "actual_invoice": actual_paid_period,
            "percent_invoice": (actual_paid_period / plan_invoice * 100) if plan_invoice > 0 else 0,
        })
        
    return results
# def get_yearly_matrix_data(db: Session, year: int):
#     # 1. Get all PMs
#     pms = db.query(models.User).filter(models.User.role.in_(['PM', 'ADMIN', 'PD'])).all()
    
#     matrix_data = []

#     for pm in pms:
#         # Initialize arrays for 12 months (0.0)
#         target_po_monthly = [0.0] * 12
#         actual_po_monthly = [0.0] * 12
#         target_inv_monthly = [0.0] * 12
#         actual_inv_monthly = [0.0] * 12

#         # 2. Fetch ALL Targets for this year for this PM
#         targets = db.query(models.UserPerformanceTarget).filter(
#             models.UserPerformanceTarget.user_id == pm.id,
#             models.UserPerformanceTarget.year == year
#         ).all()

#         for t in targets:
#             # Month is 1-based, array is 0-based
#             if 1 <= t.month <= 12:
#                 target_po_monthly[t.month - 1] = t.po_monthly_update
#                 target_inv_monthly[t.month - 1] = t.acceptance_monthly_update

#         # 3. Fetch ALL Actuals (Grouped by Month)
#         # We do 2 queries: one for PO (publish_date), one for Invoice (AC/PAC dates)
        
#         # A. Actual POs
#         po_results = db.query(
#             extract('month', models.MergedPO.publish_date).label('month'),
#             func.sum(models.MergedPO.line_amount_hw)
#         ).join(models.InternalProject).filter(
#             models.InternalProject.project_manager_id == pm.id,
#             extract('year', models.MergedPO.publish_date) == year
#         ).group_by('month').all()

#         for m, val in po_results:
#             if m: actual_po_monthly[int(m) - 1] = val or 0

#         # B. Actual Invoices (Paid) - This is trickier because AC and PAC have different dates.
#         # We iterate 1-12 and query efficiently or fetch all and aggregate in python.
#         # Let's fetch all accepted items for this PM and year and bucket them in Python.
        
#         # (Simplified logic for performance: Fetch items where EITHER date is in year)
#         paid_items = db.query(models.MergedPO).join(models.InternalProject).filter(
#             models.InternalProject.project_manager_id == pm.id,
#             (extract('year', models.MergedPO.date_ac_ok) == year) | (extract('year', models.MergedPO.date_pac_ok) == year)
#         ).all()

#         for item in paid_items:
#             # Add AC amount to the AC month
#             if item.date_ac_ok and item.date_ac_ok.year == year:
#                 actual_inv_monthly[item.date_ac_ok.month - 1] += (item.accepted_ac_amount or 0)
            
#             # Add PAC amount to the PAC month
#             if item.date_pac_ok and item.date_pac_ok.year == year:
#                 actual_inv_monthly[item.date_pac_ok.month - 1] += (item.accepted_pac_amount or 0)

#         # 4. Construct the Rows
#         rows = [
#             { "name": "Target PO Received", "values": target_po_monthly, "total": sum(target_po_monthly) },
#             { "name": "Actual PO Received", "values": actual_po_monthly, "total": sum(actual_po_monthly) },
#             { "name": "Target Invoice", "values": target_inv_monthly, "total": sum(target_inv_monthly) },
#             { "name": "Actual Invoice", "values": actual_inv_monthly, "total": sum(actual_inv_monthly) }
#         ]

#         matrix_data.append({
#             "pm_name": f"{pm.first_name} {pm.last_name}",
#             "milestones": rows
#         })

#     return matrix_data
def get_planning_matrix(db: Session, year: int, user: Optional[models.User] = None):
    # 1. Get all PMs
    # Using your role logic
    pms_to_process = []
    if user and user.role in [UserRole.PM]:
        # If the user is a PM or PD, they only see their own data
        pms_to_process = [user]
    else: # This covers ADMIN or cases where no user is passed
        # Admins see everyone
        pms_to_process = db.query(models.User).filter(
            models.User.role.in_(['PM', 'ADMIN', 'PD'])
        ).all()
    
    matrix_data = []

    for pm in pms_to_process:
        # --- Initialize 12-month arrays for ALL 6 data rows ---
        po_master = [0.0] * 12
        po_update = [0.0] * 12
        po_actual = [0.0] * 12
        
        acc_master = [0.0] * 12
        acc_update = [0.0] * 12
        acc_actual = [0.0] * 12

        # 2. Fetch Targets (Database)
        # Assuming you renamed the model to UserPerformanceTarget or kept MonthlyTarget
        # AND you ran the migration to add the 'master' columns.
        targets = db.query(models.UserPerformanceTarget).filter(
            models.UserPerformanceTarget.user_id == pm.id,
            models.UserPerformanceTarget.year == year
        ).all()

        for t in targets:
            if 1 <= t.month <= 12:
                idx = t.month - 1
                # Map the database columns to our arrays
                po_master[idx] = t.po_master_plan or 0
                po_update[idx] = t.po_monthly_update or 0
                acc_master[idx] = t.acceptance_master_plan or 0
                acc_update[idx] = t.acceptance_monthly_update or 0

        # 3. Calculate Actuals (Logic from your old function, slightly optimized)
        
        # A. Actual POs (Based on Publish Date)
        po_results = db.query(
            extract('month', models.MergedPO.publish_date).label('month'),
            func.sum(models.MergedPO.line_amount_hw)
        ).join(models.CustomerProject).join(models.InternalProject).filter(
            models.InternalProject.project_manager_id == pm.id,
            extract('year', models.MergedPO.publish_date) == year,
            models.MergedPO.assignment_status == models.AssignmentStatus.APPROVED
        ).group_by('month').all()

        for m, val in po_results:
            if m: po_actual[int(m) - 1] = val or 0

        # B. Actual Acceptance (Based on AC/PAC Dates)
        # Using the same logic as before: fetch items where either date is in year
        paid_items = db.query(models.MergedPO).join(models.CustomerProject).join(models.InternalProject).filter(
            models.InternalProject.project_manager_id == pm.id,
            (extract('year', models.MergedPO.date_ac_ok) == year) | 
            (extract('year', models.MergedPO.date_pac_ok) == year),
                    models.MergedPO.assignment_status == models.AssignmentStatus.APPROVED

        ).all()

        for item in paid_items:
            # AC Logic
            if item.date_ac_ok and item.date_ac_ok.year == year:
                acc_actual[item.date_ac_ok.month - 1] += (item.accepted_ac_amount or 0)
            
            # PAC Logic
            if item.date_pac_ok and item.date_pac_ok.year == year:
                acc_actual[item.date_pac_ok.month - 1] += (item.accepted_pac_amount or 0)

        # 4. Construct the Data Structure for the Frontend
        # We return an object that matches the structure expected by the React component I gave you earlier.
        
        # Note: My React component expected a dictionary 'months': { 1: { po: {...}, acc: {...} } }
        # Let's reshape these arrays into that dictionary format to be clean.
        
        months_data = {}
        for i in range(12):
            m = i + 1
            months_data[m] = {
                "po": {
                    "master": po_master[i],
                    "update": po_update[i],
                    "actual": po_actual[i]
                },
                "acceptance": {
                    "master": acc_master[i],
                    "update": acc_update[i],
                    "actual": acc_actual[i]
                }
            }

        matrix_data.append({
            "pm_id": pm.id,
            "pm_name": f"{pm.first_name} {pm.last_name}",
            "months": months_data
        })

    return matrix_data

def get_internal_projects_for_user(db: Session, user: models.User):
    """
    Returns projects based on role:
    - Admin: All projects
    - PM/PD: Only projects where they are the manager
    - Others: Empty list (or all, depending on your needs)
    """
    query = db.query(models.InternalProject)
    
    if user.role == UserRole.ADMIN:
        return query.order_by(models.InternalProject.name).all()
    
    elif user.role in [UserRole.PM]:
        return query.filter(models.InternalProject.project_manager_id == user.id).order_by(models.InternalProject.name).all()
    
    else:
        return [] # Or raise error

# Update the selector too
def get_internal_project_selector_for_user(db: Session, user: models.User, search: str = None):
    query = db.query(models.InternalProject)
    
    # 1. Apply Security Filter
    if user.role != UserRole.ADMIN:
        # If not admin, restrict to own projects
        # (Assuming only PMs/PDs use this selector to see their work)
        query = query.filter(models.InternalProject.project_manager_id == user.id)

    # 2. Apply Search Filter
    if search:
        query = query.filter(models.InternalProject.name.ilike(f"%{search}%"))
        
    return query.limit(20).all()

def get_remaining_to_accept_dataframe(
    db: Session,
    filter_stage: str = "ALL",
    search: Optional[str] = None,
    internal_project_id: Optional[int] = None,
    customer_project_id: Optional[int] = None,
    user: models.User = None
) -> pd.DataFrame:
    """
    Builds a query for the "Remaining To Accept" export based on filters,
    and returns a Pandas DataFrame ready for export.
    """
    # Define SQL expressions
    remaining_expr = models.MergedPO.line_amount_hw - (
        func.coalesce(models.MergedPO.accepted_ac_amount, 0) + 
        func.coalesce(models.MergedPO.accepted_pac_amount, 0)
    )
    stage_expr = case(
        (models.MergedPO.date_ac_ok.is_(None), "WAITING_AC"),
        (and_(models.MergedPO.date_ac_ok.isnot(None), models.MergedPO.date_pac_ok.is_(None)), "WAITING_PAC"),
        else_="PARTIAL_GAP"
    )

    # --- THIS IS THE UPDATED QUERY ---
    query = db.query(
        # 1. Add the new requested columns
        models.MergedPO.po_id,
        models.MergedPO.po_no,
        models.MergedPO.po_line_no,
        func.concat(models.User.first_name, " ", models.User.last_name).label("pm_name"),
        models.InternalProject.name.label("internal_project_name"),

        # 2. Keep the original columns
        models.MergedPO.site_code,
        models.MergedPO.item_description,
        models.CustomerProject.name.label("customer_project_name"),
        models.MergedPO.line_amount_hw,
        models.MergedPO.accepted_ac_amount,
        models.MergedPO.accepted_pac_amount,
        remaining_expr.label("remaining_amount"),
        stage_expr.label("remaining_stage"),
        models.MergedPO.publish_date
    ).select_from(models.MergedPO).outerjoin(
        models.InternalProject, models.MergedPO.internal_project_id == models.InternalProject.id
    ).outerjoin(
        # Add a join to the User table to get the PM name
        models.User, models.InternalProject.project_manager_id == models.User.id
    ).outerjoin(
        models.CustomerProject, models.MergedPO.customer_project_id == models.CustomerProject.id
    ).filter(
        func.abs(remaining_expr) > 0.01
    )
    # --------------------------------
    if user and user.role in [UserRole.PM]:
        query = query.filter(models.InternalProject.project_manager_id == user.id)
    # Apply filters (no change here)
    if filter_stage != "ALL":
        query = query.filter(stage_expr == filter_stage)
    if internal_project_id:
        query = query.filter(models.MergedPO.internal_project_id == internal_project_id)
    if customer_project_id:
        query = query.filter(models.MergedPO.customer_project_id == customer_project_id)
    if search:
        term = f"%{search}%"
        query = query.filter(
            (models.MergedPO.po_no.ilike(term)) | 
            (models.MergedPO.site_code.ilike(term)) |
            (models.MergedPO.item_description.ilike(term))
        )
    
    # Execute query and load into DataFrame (no change here)
    df = pd.read_sql(query.statement, db.bind)

    if df.empty:
        return pd.DataFrame()

    # --- THIS IS THE UPDATED RENAMING ---
    # Rename the columns for the final Excel file output
    df.rename(columns={
        'po_id': 'PO ID',
        'po_no': 'PO Number',
        'po_line_no': 'PO Line',
        'pm_name': 'PM',
        'internal_project_name': 'Internal Project',
        'customer_project_name': 'Customer Project',
        'site_code': 'Site Code',
        'item_description': 'Item Description',
        'line_amount_hw': 'Total PO Value',
        'accepted_ac_amount': 'Accepted AC',
        'accepted_pac_amount': 'Accepted PAC',
        'remaining_amount': 'Remaining to Accept',
        'remaining_stage': 'Stage',
        'publish_date': 'Publish Date'
    }, inplace=True)
    # ------------------------------------
    
    return df   

def generate_bc_number(db: Session):
    """
    Generates ID based on Date: BC + YYYYMMDD + XX (Daily Sequence)
    Example: BC2025120701
    """
    now = datetime.now()
    
    # 1. Build the prefix: BC20251207
    date_str = now.strftime("%Y%m%d")
    prefix = f"BC{date_str}"
    
    # 2. Find the last BC created TODAY (using the prefix)
    # We use a LIKE query to find IDs starting with today's prefix
    last_bc_today = db.query(models.BonDeCommande).filter(
        models.BonDeCommande.bc_number.like(f"{prefix}%")
    ).order_by(models.BonDeCommande.bc_number.desc()).first()
    
    if last_bc_today:
        # Extract the last 2 digits (XX)
        # ID is "BC2025120701", length is 12. Slicing last 2 chars.
        last_seq_str = last_bc_today.bc_number[-2:] 
        try:
            last_seq = int(last_seq_str)
            new_seq = last_seq + 1
        except ValueError:
            # Fallback if parsing fails
            new_seq = 1
    else:
        # First one today
        new_seq = 1
        
    # 3. Format with 2-digit padding (01, 02... 99)
    # If you expect >99 BCs per day, increase padding to 3 (:03d)
    return f"{prefix}{new_seq:02d}"
def get_tax_rate(db: Session, category: str, year: int):
    """
    Finds the tax rate for a specific Category and Year.
    Defaults to 0.20 (20%) if no rule found.
    """
    rule = db.query(models.TaxRule).filter(
        models.TaxRule.category == category,
        models.TaxRule.year == year
    ).first()
    
    if rule:
        return rule.tax_rate
    return 0.20 # Default fallback


# --- MAIN ACTION: Create BC ---
def create_bon_de_commande(db: Session, bc_data: schemas.BCCreate, creator_id: int):
    
    sbc = db.query(models.SBC).get(bc_data.sbc_id)
    if not sbc:
        raise ValueError("SBC not found")
        
    # Map SBC type to BC type
    bc_type_to_set = BCType.PERSONNE_PHYSIQUE if sbc.sbc_type == SBCType.PP else BCType.STANDARD

    
    # 1. Generate ID (Using the new Date-based format)
    bc_number = generate_bc_number(db)
    
    new_bc = models.BonDeCommande(
        bc_number=bc_number,
        project_id=bc_data.internal_project_id,
        sbc_id=bc_data.sbc_id,
        status=models.BCStatus.DRAFT,
        bc_type=bc_type_to_set, # Set the BC type automatically
        created_at=datetime.now(),
        creator_id=creator_id,
        year=datetime.now().year
    )
    
    db.add(new_bc)
    db.flush()
    
    total_ht = 0.0
    total_tax = 0.0
    
    for item_data in bc_data.items:
        po = db.query(models.MergedPO).get(item_data.merged_po_id)
        if not po:        
            raise ValueError(f"PO Line ID {item_data.merged_po_id} not found.")
        
        # 1. Calculate how much has ALREADY been assigned to other BCs
        consumed_qty = db.query(func.sum(models.BCItem.quantity_sbc)).filter(
            models.BCItem.merged_po_id == po.id
        ).scalar() or 0.0
        
        # 2. Calculate what is actually available
        available_qty = po.requested_qty - consumed_qty
        
        # 3. Check if the new request fits
        # We use a small epsilon (0.0001) to handle floating point variations
        if item_data.quantity_sbc > (available_qty + 0.0001):
            raise ValueError(
                f"Error on PO {po.po_id}: "
                f"Requested {item_data.quantity_sbc}, but only {available_qty} remains "
                f"(Total: {po.requested_qty}, Used: {consumed_qty})."
            )
            
        # 4. Project Security Check (Existing)
        if po.internal_project_id != bc_data.internal_project_id:
             raise ValueError(f"PO Line {po.po_id} does not belong to the selected Internal Project.")
        # 1. Calc HT Amounts
        unit_price_sbc = (po.unit_price or 0) * item_data.rate_sbc
        line_amount_sbc = unit_price_sbc * item_data.quantity_sbc
        
        # 2. AUTOMATIC TAX LOOKUP
        # Logic: Find tax based on PO Category + Current Year
        # (Assuming the tax applies to the year the BC is created)
        current_year = datetime.now().year
        if bc_data.bc_type == models.BCType.PERSONNE_PHYSIQUE:
            tax_rate_val = 0.0 # No Tax for individuals
        else:
            # Standard logic
            tax_rate_val = get_tax_rate(db, category=po.category, year=current_year)

        line_tax = line_amount_sbc * tax_rate_val
        
        # 3. Create Item
        bc_item = models.BCItem(
            bc_id=new_bc.id,
            merged_po_id=po.id,
            rate_sbc=item_data.rate_sbc,
            quantity_sbc=item_data.quantity_sbc,
            unit_price_sbc=unit_price_sbc,
            line_amount_sbc=line_amount_sbc,
            applied_tax_rate=tax_rate_val # Store the rate used
        )
        db.add(bc_item)
        
        total_ht += line_amount_sbc
        total_tax += line_tax

    # 4. Finalize
    new_bc.total_amount_ht = total_ht
    new_bc.total_tax_amount = total_tax
    new_bc.total_amount_ttc = total_ht + total_tax
    
    db.commit()
    db.refresh(new_bc)
    return new_bc
def generate_sbc_code(db: Session):
    """Auto-generates SBC-001, SBC-002..."""
    last = db.query(models.SBC).order_by(models.SBC.id.desc()).first()
    next_id = (last.id + 1) if last else 1
    return f"SBC-{str(next_id).zfill(3)}"

def save_upload_file(upload_file, sbc_code, doc_type):
    """Saves file to disk and returns path"""
    if not upload_file: return None
    
    # Create dir if not exists
    Path(UPLOAD_DIR).mkdir(parents=True, exist_ok=True)
    
    # Filename: SBC-001_Contract.pdf
    ext = upload_file.filename.split('.')[-1]
    filename = f"{sbc_code}_{doc_type}.{ext}"
    file_path = os.path.join(UPLOAD_DIR, filename)
    
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(upload_file.file, buffer)
        
    return filename

def create_sbc(db: Session, form_data: dict, contract_file, tax_file, creator_id: int):
    # 1. Handle Code
    email = form_data.get('email')
    phone = form_data.get('phone_1')
    if email and db.query(models.SBC).filter(models.SBC.email == email).first():
        raise ValueError(f"An SBC with the email '{email}' already exists.")
    if phone and db.query(models.SBC).filter(models.SBC.phone_1 == phone).first():
        raise ValueError(f"An SBC with the phone number '{phone}' already exists.")

    code = form_data.get('sbc_code')
    if not code:
        code = generate_sbc_code(db)
        
    # 2. Handle Files
    
    contract_fname = save_upload_file(contract_file, code, "Contract")
    tax_fname = save_upload_file(tax_file, code, "TaxReg")
    # 3. Create Entity
    new_sbc = models.SBC(
        sbc_code=code,
        creator_id=creator_id,
        status=models.SBCStatus.UNDER_APPROVAL, # Always start here
        sbc_type=form_data.get('sbc_type'), # Get from form data

        # Identity
        short_name=form_data.get('short_name'),
        name=form_data.get('name'),
        start_date=form_data.get('start_date'),
        
        # Contact
        ceo_name=form_data.get('ceo_name'),
        phone_1=form_data.get('phone_1'),
        email=form_data.get('email'),
        
        # Financial
        rib=form_data.get('rib'),
        bank_name=form_data.get('bank_name'),
        ice=form_data.get('ice'),
        rc=form_data.get('rc'),
        # Contract
        contract_ref=form_data.get('contract_ref'),
        # We store the DATE of upload if file exists
        contract_upload_date=datetime.now() if contract_fname else None,
        has_contract_attachment=True if contract_fname else False,
                contract_filename=contract_fname, # Save it!

        # Tax
        tax_reg_upload_date=datetime.now() if tax_fname else None,
        has_tax_regularization=True if tax_fname else False,
        tax_reg_end_date=form_data.get('tax_reg_end_date'),
                tax_reg_filename=tax_fname # Save it!

    )
    
    db.add(new_sbc)
    db.commit()
    db.refresh(new_sbc)
    return new_sbc
def get_pending_sbcs(db: Session):
    return db.query(models.SBC).filter(
        models.SBC.status == models.SBCStatus.UNDER_APPROVAL
    ).all()
def get_active_sbcs(db: Session):
    return db.query(models.SBC).filter(models.SBC.status == models.SBCStatus.ACTIVE).all()
def get_all_sbcs(db: Session, search: Optional[str] = None):
    return_query = db.query(models.SBC)
    if search:
        term = f"%{search}%"
        return_query = return_query.filter(
            (models.SBC.sbc_code.ilike(term)) |
            (models.SBC.short_name.ilike(term)) |
            (models.SBC.name.ilike(term)) |
            (models.SBC.email.ilike(term)) |
            (models.SBC.phone_1.ilike(term))
        )
    return return_query.order_by(models.SBC.created_at.desc()).all()
def approve_sbc(db: Session, sbc_id: int, approver_id: int):
    # 1. Get the SBC
    sbc = db.query(models.SBC).get(sbc_id)
    if not sbc:
        raise ValueError("SBC not found")
    if not sbc.email:
        raise ValueError("Cannot approve SBC: The SBC must have an email address to create a user account.")
    
    # 2. Update SBC Status
    sbc.status = models.SBCStatus.ACTIVE
    sbc.approver_id = approver_id
    
    # 3. Check if a User already exists for this email
    existing_user = get_user_by_email(db, sbc.email)
    
    new_user = None
    if not existing_user:
        # 4. Create the User Account with SBC Role
        temp_password = secrets.token_urlsafe(10)
        reset_token = secrets.token_urlsafe(32)
        
        # Split name for First/Last
        # Assuming sbc.ceo_name is "John Doe"
        name_parts = (sbc.ceo_name or "SBC User").split(" ")
        first_name = name_parts[0]
        last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

        new_user = models.User(
            email=sbc.email,
            username=sbc.email, # Use email as username for external users
            first_name=first_name,
            last_name=last_name,
            role="SBC", # <--- FORCE THE SBC ROLE HERE
            hashed_password=auth.get_password_hash(temp_password), # Use SBC password or temp
            phone_number=sbc.phone_1,
            sbc_id=sbc.id, # Link this new user to the SBC being approved
            
            is_active=True,
            reset_token=reset_token # Save token for the email
        )
        db.add(new_user)
        # Flush to get the ID, but don't commit yet to keep transaction atomic
        db.flush() 
    
    db.commit()
    db.refresh(sbc)
    
    # Return both the SBC and the New User (if created) so the router can send the email
    return sbc, new_user

def reject_sbc(db: Session, sbc_id: int):
    sbc = db.query(models.SBC).get(sbc_id)
    if not sbc:
        raise ValueError("SBC not found")
        
    sbc.status = models.SBCStatus.BLACKLISTED # Or return to Draft logic if you prefer
    db.commit()
    return sbc
def cancel_bc(db: Session, bc_id: int, user_id: int):
    """Deletes a BC if it's in DRAFT and owned by the user."""
    bc = db.query(models.BonDeCommande).get(bc_id)

    if not bc:
        raise ValueError("BC not found.")
    
    # Security Check: Only the creator can cancel it
    if bc.creator_id != user_id:
        raise ValueError("Forbidden: You are not the creator of this BC.")

    # Business Logic Check: Can only cancel drafts
    if bc.status != models.BCStatus.DRAFT:
        raise ValueError("Forbidden: Only BCs in Draft status can be cancelled.")

    # Perform the deletion
    db.delete(bc)
    db.commit()

def submit_bc(db: Session, bc_id: int):
    """Moves BC from DRAFT to SUBMITTED (Ready for L1)"""
    bc = db.query(models.BonDeCommande).get(bc_id)
    if not bc or bc.status != models.BCStatus.DRAFT:
        raise ValueError("BC not found or not in Draft status.")
    bc.status = models.BCStatus.SUBMITTED
    bc.submitted_at = datetime.now()
    db.commit()
    return bc


def approve_bc_l1(db: Session, bc_id: int, approver_id: int):
    # 1. Fetch the Approver User to check permissions
    approver = db.query(models.User).get(approver_id)
    if not approver:
        raise ValueError("Approver user not found.")
        
    # --- SECURITY CHECK: Must be PD or Admin ---
    # (Admins usually have super-powers, so including them is safe, but strict PD is fine too)
    if approver.role not in [models.UserRole.PD, models.UserRole.ADMIN]:
        raise ValueError("Permission Denied: Only a Project Director (PD) can perform L1 validation.")
    # -------------------------------------------

    bc = db.query(models.BonDeCommande).get(bc_id)
    # Check if it is SUBMITTED
    if not bc or bc.status != models.BCStatus.SUBMITTED:
        raise ValueError("BC must be in SUBMITTED status for L1 Validation.")
    
    bc.status = models.BCStatus.PENDING_L2
    bc.approver_l1_id = approver_id
    bc.approved_l1_at = datetime.now()
    db.commit()
    return bc


def approve_bc_l2(db: Session, bc_id: int, approver_id: int):
    # 1. Fetch the Approver User
    approver = db.query(models.User).get(approver_id)
    if not approver:
        raise ValueError("Approver user not found.")

    # --- SECURITY CHECK: Must be Admin ---
    if approver.role != models.UserRole.ADMIN:
        raise ValueError("Permission Denied: Only an Administrator can perform Final Approval (L2).")
    # -------------------------------------

    bc = db.query(models.BonDeCommande).get(bc_id)
    if not bc or bc.status != models.BCStatus.PENDING_L2:
        raise ValueError("BC must be in PENDING_L2 status for Final Approval.")
    
    bc.status = models.BCStatus.APPROVED # Final
    bc.approver_l2_id = approver_id
    bc.approved_l2_at = datetime.now()
    db.commit()
    return bc
def get_bcs_by_status(db: Session, status: models.BCStatus, search_term: Optional[str] = None):
    query = db.query(models.BonDeCommande).filter(models.BonDeCommande.status == status)

    if search_term:
        query = query.join(models.SBC).join(models.InternalProject).join(models.MergedPO, models.BonDeCommande.items)
        search = f"%{search_term}%"
        query = query.filter(
            (models.BonDeCommande.bc_number.ilike(search)) |
            (models.SBC.short_name.ilike(search)) |
            (models.InternalProject.name.ilike(search)) | (models.MergedPO.po_id.ilike(search))

        )
    return query.all()
def get_all_bcs(db: Session, current_user: models.User, search: Optional[str] = None, status_filter: Optional[str] = None): # <-- NEW ARGUMENT:
    query = db.query(models.BonDeCommande).options(
        joinedload(models.BonDeCommande.sbc),
        joinedload(models.BonDeCommande.internal_project),
        joinedload(models.BonDeCommande.creator) 
    )
    query = query.join(models.InternalProject)

    # Apply role-based filtering
    if current_user.role == models.UserRole.PM: 
        query = query.filter(
            or_(
                models.BonDeCommande.creator_id == current_user.id,
                models.InternalProject.project_manager_id == current_user.id
            )
        )
    elif current_user.role == models.UserRole.SBC:
        # SBCs see BCs where they are the subcontractor
        if not current_user.sbc_id:
            return [] # This SBC user is not linked to an SBC profile, return nothing
        query = query.filter(models.BonDeCommande.sbc_id == current_user.sbc_id)

    # Apply search filter
    if search:
        search_term = f"%{search}%"
        # The joins are already handled by the options, but we can add them here for clarity if needed.
        query = query.filter(
            (models.BonDeCommande.bc_number.ilike(search_term)) |
            (models.SBC.short_name.ilike(search_term)) |
            (models.InternalProject.name.ilike(search_term))
        )
    if status_filter:
        query = query.filter(models.BonDeCommande.status == status_filter)

    # Order by newest first and execute
    return query.order_by(models.BonDeCommande.created_at.desc()).all()

    
def reject_bc(db: Session, bc_id: int, reason: str, rejector_id: int):
    bc = db.query(models.BonDeCommande).get(bc_id)
    if not bc or bc.status not in [models.BCStatus.SUBMITTED, models.BCStatus.PENDING_L2]:
        raise ValueError("BC not found or cannot be rejected in its current state.")
    
    bc.status = models.BCStatus.REJECTED
    bc.rejection_reason = reason
    # You could also add a 'rejected_by_id' foreign key if you want to track this
    
    db.commit()
    return bc
def get_bc_by_id(db: Session, bc_id: int):

    check_rejections_and_notify(db, bc_id)

    return db.query(models.BonDeCommande).options(
        # 1. Load items, and for each item, load the associated MergedPO
        joinedload(models.BonDeCommande.items).joinedload(models.BCItem.merged_po),
        # 2. Load other relationships
        joinedload(models.BonDeCommande.sbc),
        joinedload(models.BonDeCommande.internal_project),
        joinedload(models.BonDeCommande.creator),
        joinedload(models.BonDeCommande.items)
            .joinedload(models.BCItem.rejection_history)
            .joinedload(models.ItemRejectionHistory.rejected_by)

    ).filter(models.BonDeCommande.id == bc_id).first()

def assign_site_to_internal_project_by_code(
    db: Session,
    site_code: str,
    internal_project_name: str,
) -> int:
    
    """
    Assigne UN site (via site_code) à un projet interne (via nom du projet).
    - Crée/Maj l'entrée SiteProjectAllocation
    - Met à jour tous les MergedPO de ce site pour pointer vers ce projet interne
    Retourne le nombre de lignes MergedPO mises à jour.
    """

    # 1. Récupérer le site
    site = (
        db.query(models.Site)
        .filter(models.Site.site_code == site_code)
        .first()
    )
    if not site:
        # Pas d'erreur ici, on renvoie 0, le router décidera quoi faire
        return 0

    # 2. Récupérer le projet interne par son nom
    internal_project = (
        db.query(models.InternalProject)
        .filter(models.InternalProject.name == internal_project_name)
        .first()
    )
    if not internal_project:
        return 0

    # 3. Créer ou mettre à jour l’allocation manuelle site ↔ projet
    allocation = (
        db.query(models.SiteProjectAllocation)
        .filter(models.SiteProjectAllocation.site_id == site.id)
        .first()
    )
    if allocation:
        allocation.internal_project_id = internal_project.id
    else:
        allocation = models.SiteProjectAllocation(
            site_id=site.id,
            internal_project_id=internal_project.id,
        )
        db.add(allocation)

    # 4. Mettre à jour tous les MergedPO pour ce site
    updated_rows = (
        db.query(models.MergedPO)
        .filter(models.MergedPO.site_id == site.id)
        .update(
            {models.MergedPO.internal_project_id: internal_project.id},
            synchronize_session=False,
        )
    )

    db.commit()
    return updated_rows

def bulk_assign_sites(db: Session, site_ids: List[int], target_project_id: int, admin_user: models.User):
    # 1. Get Target Project Details
    target_project = db.query(models.InternalProject).get(target_project_id)
    if not target_project: 
        return {"updated": 0, "error": "Target project not found"}

    # 2. Simplified Validation: We assume the Admin knows what they are doing.
    # If the site ID exists, we will update it. This matches the single-assign logic.
    # We just filter out any IDs that aren't actually in the Sites table to be safe.
    valid_site_ids = [
        sid for sid in site_ids 
        if db.query(models.Site.id).filter_by(id=sid).scalar() is not None
    ]
    
    if not valid_site_ids: 
        return {"updated": 0, "skipped": len(site_ids)}

    # 3. Update Allocations (Upsert Logic)
    # Get a valid customer_project_id default
    sample_cp = db.query(models.CustomerProject.id).first()
    default_cp_id = sample_cp.id if sample_cp else 1

    # Fetch existing allocations for these sites to decide between UPDATE or INSERT
    existing_allocs = db.query(models.SiteProjectAllocation).filter(
        models.SiteProjectAllocation.site_id.in_(valid_site_ids)
    ).all()
    
    existing_site_ids = {alloc.site_id for alloc in existing_allocs}

    # A. Update existing allocations
    if existing_site_ids:
        db.query(models.SiteProjectAllocation).filter(
            models.SiteProjectAllocation.site_id.in_(existing_site_ids)
        ).update(
            {models.SiteProjectAllocation.internal_project_id: target_project_id},
            synchronize_session=False
        )

    # B. Insert new allocations for sites that didn't have one
    new_allocs = []
    for sid in valid_site_ids:
        if sid not in existing_site_ids:
            new_allocs.append({
                "site_id": sid,
                "internal_project_id": target_project_id,
                "customer_project_id": default_cp_id
            })
    
    if new_allocs:
        db.bulk_insert_mappings(models.SiteProjectAllocation, new_allocs)
            
    # 4. Update MergedPO Records (The most important part)
    # This moves the POs to the new project and sets them to PENDING_APPROVAL
    result_count = db.query(models.MergedPO).filter(
        models.MergedPO.site_id.in_(valid_site_ids)
    ).update({
        "internal_project_id": target_project_id,
        "assignment_status": models.AssignmentStatus.PENDING_APPROVAL,
        "assignment_date": datetime.now()
    }, synchronize_session=False)
    
    db.commit()

    # 5. Send Notification
    if target_project.project_manager_id:
        try:
            create_notification(
                db, 
                recipient_id=target_project.project_manager_id,
                type=models.NotificationType.TODO,
                title="Bulk Site Assignment",
                message=f"{admin_user.first_name} has assigned {result_count} PO lines (across {len(valid_site_ids)} sites) to project '{target_project.name}'. Please review.",
                link="/projects/approvals"
            )
            db.commit()
        except Exception as e:
            print(f"Failed to send notification: {e}")

    return {"updated": result_count, "skipped": len(site_ids) - len(valid_site_ids)}

def auto_approve_old_assignments(db: Session):
    """
    Finds sites pending for > 7 days, auto-approves them, and notifies PMs/Admins.
    """
    seven_days_ago = datetime.now() - timedelta(days=7)

    # 1. Identify which PMs are affected
    # We query for PENDING items older than 7 days, joined with their target Project and PM.
    # We group by PM so we can send one notification per PM.
    pending_groups = db.query(
        models.User.id.label("pm_id"),
        models.InternalProject.name.label("project_name"),
        func.count(models.MergedPO.id).label("count")
    ).join(
        models.InternalProject, models.MergedPO.internal_project_id == models.InternalProject.id
    ).join(
        models.User, models.InternalProject.project_manager_id == models.User.id
    ).filter(
        models.MergedPO.assignment_status == models.AssignmentStatus.PENDING_APPROVAL,
        models.MergedPO.assignment_date <= seven_days_ago
    ).group_by(models.User.id, models.InternalProject.name).all()

    if not pending_groups:
        return 0

    total_approved = 0

    # 2. Iterate through groups and notify PMs
    for group in pending_groups:
        count = group.count
        pm_id = group.pm_id
        project_name = group.project_name
        
        # Notify the PM
        create_notification(
            db,
            recipient_id=pm_id,
            type=models.NotificationType.APP, # 'APP' type for status updates
            title="Auto-Approval Notice",
            message=f"{count} sites assigned to '{project_name}' were auto-approved due to inactivity (>7 days).",
            link="/projects/list" # Link to their project list to see the new sites
        )
        total_approved += count

    # 3. Perform the Bulk Update
    # We update ALL eligible records in one go for efficiency
    db.query(models.MergedPO).filter(
        models.MergedPO.assignment_status == models.AssignmentStatus.PENDING_APPROVAL,
        models.MergedPO.assignment_date <= seven_days_ago
    ).update({
        "assignment_status": models.AssignmentStatus.APPROVED,
        "assignment_date": None 
    }, synchronize_session=False)

    # 4. Notify Admins
    # We send a summary to all admins
    admins = db.query(models.User).filter(models.User.role == "ADMIN").all()
    for admin in admins:
        create_notification(
            db,
            recipient_id=admin.id,
            type=models.NotificationType.SYSTEM,
            title="System Auto-Approval",
            message=f"System auto-approved {total_approved} overdue site assignments.",
            link=None
        )

    db.commit()
    return total_approved
def get_pending_sites_for_pm(db: Session, pm_id: int):
    # Find projects managed by this PM
    return db.query(models.MergedPO).join(models.InternalProject).filter(
        models.InternalProject.project_manager_id == pm_id,
        models.MergedPO.assignment_status == models.AssignmentStatus.PENDING_APPROVAL
    ).all()

# 3. NEW: Process PM Decision (Approve/Reject)
# In crud.py

def process_assignment_review(db: Session, merged_po_ids: List[int], action: str, pm_user: models.User):
    """
    Handles the PM's decision (APPROVE or REJECT) and notifies Admins.
    """
    
    # 1. Identify the records involved
    # Only process items that are actually PENDING for safety
    query = db.query(models.MergedPO).filter(
        models.MergedPO.id.in_(merged_po_ids), 
        models.MergedPO.assignment_status == models.AssignmentStatus.PENDING_APPROVAL
    )
    
    count = query.count()
    if count == 0:
        return 0

    # 2. Apply Logic
    if action == "APPROVE":
        # Change status to APPROVED. Keep project assignment.
        query.update({
            "assignment_status": models.AssignmentStatus.APPROVED
        }, synchronize_session=False)

    elif action == "REJECT":
        # Revert to TBD project and set status to APPROVED (as TBD is auto-approved)
        tbd_project = db.query(models.InternalProject).filter_by(name="To Be Determined").first()
        tbd_id = tbd_project.id if tbd_project else None
        
        if tbd_id:
            query.update({
                "internal_project_id": tbd_id,
                "assignment_status": models.AssignmentStatus.APPROVED
            }, synchronize_session=False)
            
            # Also revert the Allocation table for these sites
            # We need the site_ids from the POs first
            site_ids = [po.site_id for po in query.all()]
            if site_ids:
                db.query(models.SiteProjectAllocation).filter(
                    models.SiteProjectAllocation.site_id.in_(site_ids)
                ).update({"internal_project_id": tbd_id}, synchronize_session=False)

    db.commit()

    # --- 3. SEND NOTIFICATIONS TO ADMINS (FIXED) ---
    try:
        # Get all admins
        admins = db.query(models.User).filter(models.User.role == "ADMIN").all()
        
        notif_type = models.NotificationType.APP if action == "APPROVE" else models.NotificationType.ALERT
        action_text = "Approved" if action == "APPROVE" else "Rejected"
        
        for admin in admins:
            create_notification(
                db,
                recipient_id=admin.id,
                type=notif_type,
                title=f"Site Assignment {action_text}",
                message=f"PM {pm_user.first_name} {pm_user.last_name} has {action.lower()}ed {count} sites.",
                link="/site-dispatcher" # Link them back to dispatcher to see results
            )
        
        db.commit()
        print(f"Notifications sent to {len(admins)} admins.")
        
    except Exception as e:
        print(f"Failed to send admin notifications: {e}")

    return count
def search_merged_pos_by_site_codes(
    db: Session, 
    site_codes: List[str],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    # 1. Clean Inputs
    clean_codes = []
    for c in site_codes:
        if c and c.strip():
            clean_val = c.strip().replace('\r', '').replace('\n', '')
            clean_codes.append(clean_val)
    
    clean_codes = list(set(clean_codes))
    
    if not clean_codes:
        return []

    tbd_project = db.query(models.InternalProject).filter(
        (models.InternalProject.name == "To Be Determined") |
        (models.InternalProject.project_type == "TBD")
    ).first()

    # If we can't find the TBD project, something is wrong, return nothing.
    if not tbd_project:
        return []

    # 2. Build the query, adding a filter for the TBD project ID.
    query = db.query(models.MergedPO).options(
        joinedload(models.MergedPO.internal_project),
        joinedload(models.MergedPO.customer_project)
    ).filter(
        models.MergedPO.site_code.in_(clean_codes),
        models.MergedPO.internal_project_id == tbd_project.id # <--- ADD THIS FILTER
    )
    # 3. Apply Date Filters
    if start_date:
        query = query.filter(func.date(models.MergedPO.publish_date) >= start_date)
    if end_date:
        query = query.filter(func.date(models.MergedPO.publish_date) <= end_date)

    return query.all()
def bulk_assign_projects_only(db: Session, file_contents: bytes):
    """
    Simplified Migration:
    1. Reads 'PO ID' and 'internal Project' columns from Excel.
    2. Updates the 'internal_project_id' for existing POs.
    Ignores dates/financials.
    """
    import pandas as pd
    import io

    # 1. Load Excel
    df = pd.read_excel(io.BytesIO(file_contents), header=0,sheet_name="PO")
    
    # Clean column names (remove \n, extra spaces)
    df.columns = [str(c).replace('\n', ' ').strip() for c in df.columns]
    
    print(f"Detected Columns: {df.columns.tolist()}") # For debugging logs

    # 2. Identify Target Columns dynamically
    col_po_id = "PO ID"
    col_project = "internal Project"
   

    if not col_po_id or not col_project:
        raise ValueError(f"Could not auto-detect columns. Found: {col_po_id}, {col_project}")

    # 3. Pre-fetch Map: Project Name -> Project ID
    all_projects = db.query(models.InternalProject).all()
    # Create a map: "hw_org_wireless...": 1
    project_map = {p.name.strip().lower(): p.id for p in all_projects}

    # 4. Pre-fetch Map: PO ID -> DB Primary Key ID
    # We need the PK (id) to perform a fast bulk update
    # Fetching only ID and PO_ID is very fast even for 20k rows
    po_records = db.query(models.MergedPO.id, models.MergedPO.po_id).all()
    po_map = {p.po_id: p.id for p in po_records}

    # 5. Build Update List
    update_list = []
    for index, row in df.iterrows():
        # Get Excel Data
        raw_po_id = str(row[col_po_id]).strip()
        raw_proj_name = str(row[col_project]).strip().lower()
        
        # Check matches
        db_pk = po_map.get(raw_po_id)
        target_proj_id = project_map.get(raw_proj_name)
        
        # We only update if:
        # 1. The PO exists in our DB
        # 2. The Project Name in Excel matches a Project in our DB
        if db_pk and target_proj_id:
            update_list.append({
                "id": db_pk, # The DB Primary Key
                "internal_project_id": target_proj_id
            })

    # 6. Execute Bulk Update
    if update_list:
        # Update in batches of 5000
        batch_size = 5000
        for i in range(0, len(update_list), batch_size):
            db.bulk_update_mappings(models.MergedPO, update_list[i:i+batch_size])
            db.commit()

    return {
        "total_rows_in_excel": len(df),
        "matched_and_updated": len(update_list)
    }

def get_sbc_by_id(db: Session, sbc_id: int):
    return db.query(models.SBC).filter(models.SBC.id == sbc_id).first()
def get_bcs_export_dataframe(db: Session, search: Optional[str] = None):
    # 1. Reuse the existing query logic to filter BCs based on search
    query = db.query(models.BonDeCommande).options(
        joinedload(models.BonDeCommande.sbc),
        joinedload(models.BonDeCommande.internal_project),
        joinedload(models.BonDeCommande.creator),
        joinedload(models.BonDeCommande.approver_l1),
        joinedload(models.BonDeCommande.approver_l2),
        joinedload(models.BonDeCommande.items).joinedload(models.BCItem.merged_po)
    )

    if search:
        search_term = f"%{search}%"
        query = query.join(models.SBC).join(models.InternalProject).filter(
            (models.BonDeCommande.bc_number.ilike(search_term)) |
            (models.SBC.short_name.ilike(search_term)) |
            (models.InternalProject.name.ilike(search_term))
        )
    
    bcs = query.order_by(models.BonDeCommande.created_at.desc()).all()

    # 2. Flatten the data for Excel
    data = []
    for bc in bcs:
        # Common Header Info
        header_info = {
            "BC Number": bc.bc_number,
            "Status": bc.status,
            "Project": bc.internal_project.name if bc.internal_project else "",
            "SBC": bc.sbc.name if bc.sbc else "",
            "Total HT": bc.total_amount_ht,
            "Total TTC": bc.total_amount_ttc,
            "Created By": f"{bc.creator.first_name} {bc.creator.last_name}" if bc.creator else "",
            "Created At": bc.created_at,
            "Submitted At": bc.submitted_at,
            "Validated L1 By": f"{bc.approver_l1.first_name} {bc.approver_l1.last_name}" if bc.approver_l1 else "",
            "Validated L1 At": bc.approved_l1_at,
            "Approved L2 By": f"{bc.approver_l2.first_name} {bc.approver_l2.last_name}" if bc.approver_l2 else "",
            "Approved L2 At": bc.approved_l2_at,
            "Rejection Reason": bc.rejection_reason
        }

        # Add a row for EACH item in the BC
        for index, item in enumerate(bc.items, start=1): # Use enumerate for counter
            row = header_info.copy() # Copy header info
            # Add Item specific info
            row.update({
    "BC line": index + 1,  # ligne 1..N (mieux pour Excel)

    "PO ID": item.merged_po.po_id if item.merged_po else "",
    "BC ID": f"{bc.bc_number}-{index + 1}",  # ✅ FIX ICI (au lieu de item.line_number)
    "Site Code": item.merged_po.site_code if item.merged_po else "",
    "Item Description": item.merged_po.item_description if item.merged_po else "",
    "SBC Rate": item.rate_sbc,
    "SBC Quantity": item.quantity_sbc,
    "SBC Unit Price": item.unit_price_sbc,
    "SBC Line Total": item.line_amount_sbc,
})

            data.append(row)

    return pd.DataFrame(data)
def get_aging_analysis(db: Session,user: Optional[models.User] = None):
    """
    Groups the total remaining amount (GAP) into age buckets based on publish_date.
    """
    
    # Calculate GAP for each row: Line Amount - (Accepted AC + Accepted PAC)
    # We use coalesce to handle NULLs safely
    gap_expression = (
        func.coalesce(models.MergedPO.line_amount_hw, 0) - 
        (func.coalesce(models.MergedPO.accepted_ac_amount, 0) + func.coalesce(models.MergedPO.accepted_pac_amount, 0))
    )
    
    # Calculate Age in Days
    # DATEDIFF(NOW(), publish_date)
    age_expression = func.datediff(func.now(), models.MergedPO.publish_date)

    # Define Buckets using CASE statement
    bucket_expression = case(
        (age_expression <= 30, '0-30 Days'),
        ((age_expression > 30) & (age_expression <= 90), '30-90 Days'),
        ((age_expression > 90) & (age_expression <= 180), '90-180 Days'),
        ((age_expression > 180) & (age_expression <= 365), '180-365 Days'),
        else_='> 365 Days'
    ).label("age_bucket")

    # The Query: Sum the GAP, grouped by the Bucket
    base_query = db.query(
        bucket_expression,
        func.sum(gap_expression).label("total_gap")
    ).filter(
        # Only include rows where there IS a gap (gap > 0.01 to avoid float dust)
        gap_expression > 0.01
    )
    if user and user.role in [UserRole.PM]:
        base_query = base_query.join(models.InternalProject).filter(
            models.InternalProject.project_manager_id == user.id
        )
    # ----------------------

    results = base_query.group_by(bucket_expression).all()

    # Convert to a clean list of dicts, ensuring all buckets exist even if empty
    buckets = {
        '0-30 Days': 0.0,
        '30-90 Days': 0.0,
        '90-180 Days': 0.0,
        '180-365 Days': 0.0,
        '> 365 Days': 0.0
    }
    
    for row in results:
        if row.age_bucket in buckets:
            buckets[row.age_bucket] = row.total_gap or 0.0

    return [{"bucket": k, "amount": v} for k, v in buckets.items()]
def create_notification(
    db: Session, 
    recipient_id: int, 
    type: models.NotificationType, 
    title: str, 
    message: str, 
    link: str = None
):
    notif = models.Notification(
        recipient_id=recipient_id,
        type=type,
        title=title,
        message=message,
        link=link
    )
    db.add(notif)
    # We usually commit in the main flow, but you can commit here if you want it instant
    # db.commit() 
    return notif

def get_my_notifications(db: Session, user_id: int, unread_only: bool = False):
    query = db.query(models.Notification).filter(models.Notification.recipient_id == user_id)
    if unread_only:
        query = query.filter(models.Notification.is_read == False)
    return query.order_by(models.Notification.created_at.desc()).limit(50).all()

def mark_notification_read(db: Session, notif_id: int, user_id: int):
    notif = db.query(models.Notification).filter(
        models.Notification.id == notif_id, 
        models.Notification.recipient_id == user_id
    ).first()
    if notif:
        notif.is_read = True
        db.commit()
    return notif

def check_system_state_notifications(db: Session, user: models.User):
    """
    Generates dynamic 'virtual' notifications based on system state.
    These are not stored in the DB, just calculated on request.
    """
    virtual_todos = []
    # print(f"DEBUG NOTIF: User Role is: '{user.role}'")

    # 1. Check for TBD Sites (Only for Admins/PDs)
    role_str = str(user.role).upper() # Force uppercase for comparison
    # print(f"DEBUG NOTIF: User Role UPPER is: '{role_str}'")

    if "ADMIN" in role_str :
        tbd_project = db.query(models.InternalProject).filter(models.InternalProject.name == "To Be Determined").first()
        
        if tbd_project:
            # Simple query using the direct relationship you confirmed exists
            tbd_po_count = db.query(models.MergedPO).filter(
                models.MergedPO.internal_project_id == tbd_project.id
            ).count()
            tbd_count = db.query(
                func.count(distinct(models.MergedPO.site_id))
            ).filter(
                models.MergedPO.internal_project_id == tbd_project.id
            ).scalar() 
            
            print(f"DEBUG NOTIF: TBD Project ID is {tbd_project.id}. Count found: {tbd_count}")
            
            if tbd_count > 0:
                virtual_todos.append({
                    "id": "virtual-1",
                    "title": "Assign TBD Sites",
                    "desc": f"{tbd_count} unique sites ({tbd_po_count} POs) are waiting in 'To Be Determined'.", # Updated message
                    "priority": "High",
                    "badgeBg": "danger",
                    "link": "/site-dispatcher",
                    "action": "Go to Dispatcher",
                    "type": "TODO",
                    "is_read": False,
                    "created_at": datetime.now()
                })
        else:
             print("DEBUG NOTIF: 'To Be Determined' project not found in DB")


    # 2. Check for Missing Monthly Targets (Only for PMs)
    if "PM" in role_str:
        current_month = datetime.now().month
        current_year = datetime.now().year
        
        target = db.query(models.UserPerformanceTarget).filter(
            models.UserPerformanceTarget.user_id == user.id,
            models.UserPerformanceTarget.year == current_year,
            models.UserPerformanceTarget.month == current_month
        ).first()
        
        if not target or (target.po_monthly_update == 0 and target.acceptance_monthly_update == 0):
            virtual_todos.append({
                "id": "virtual-2",
                "title": "Set Monthly Targets",
                "desc": f"Targets for {datetime.now().strftime('%B')} are missing.",
                "priority": "Medium",
                "badgeBg": "warning",
                "link": "/users/targets",
                "action": "Set Targets"
            })

    return virtual_todos

def import_planning_targets(db: Session, df: pd.DataFrame):
    df = df.fillna(0)

    count = 0
    # Expected columns matches the Export format
    # "PM Name" (We need to resolve this to User ID), "Year", "Month", etc.
    
    # Optimization: Cache user map { "First Last": user_id }
    users = db.query(models.User).all()
    user_map = {f"{u.first_name} {u.last_name}": u.id for u in users}

    for _, row in df.iterrows():
        pm_name = row.get("PM Name")
        user_id = user_map.get(pm_name)
        
        if not user_id: continue # Skip if PM not found

        year = row.get("Year")
        month = row.get("Month")
        
        # Upsert logic
        target = db.query(models.UserPerformanceTarget).filter(
            models.UserPerformanceTarget.user_id == user_id,
            models.UserPerformanceTarget.year == year,
            models.UserPerformanceTarget.month == month
        ).first()

        if not target:
            target = models.UserPerformanceTarget(user_id=user_id, year=year, month=month)
            db.add(target)
        
        # Update fields
        target.po_master_plan = row.get("PO (Master Plan)", 0)
        target.po_monthly_update = row.get("PO (Monthly Update)", 0)
        target.acceptance_master_plan = row.get("Acceptance (Master Plan)", 0)
        target.acceptance_monthly_update = row.get("Acceptance (Monthly Update)", 0)
        
        count += 1

    db.commit()
    return count

def validate_bc_item(db: Session, item_id: int, user: models.User, action: str, comment: str = None):
    """
    Handles QC or PM approval/rejection.
    action: "APPROVE" or "REJECT"
    """
    item = db.query(models.BCItem).get(item_id)
    if not item: raise ValueError("Item not found")

    # 1. Determine Role
    is_qc = user.role == UserRole.QUALITY # Adjust to your Enum
    is_pm = user.role == UserRole.PM
    is_pd = user.role in [UserRole.PD, UserRole.ADMIN]
    print(f"DEBUG: User Role: {user.role}, is_qc: {is_qc}, is_pm: {is_pm}, is_pd: {is_pd}")
        

    if not (is_qc or is_pm or is_pd):
        print(f"user.role:{user.role}")
        raise ValueError("Unauthorized role.")



    # 2. Apply Decision
    if action == "APPROVE":
        if is_qc: item.qc_validation_status = models.ValidationState.APPROVED
        if is_pm: item.pm_validation_status = models.ValidationState.APPROVED
        if item.global_status == models.ItemGlobalStatus.POSTPONED:
             # Check if BOTH are now approved (meaning the rejector fixed their status)
             if item.qc_validation_status == models.ValidationState.APPROVED and \
                item.pm_validation_status == models.ValidationState.APPROVED:
                 
                 item.global_status = models.ItemGlobalStatus.PENDING_PD_APPROVAL
                 item.postponed_until = None # Clear the timer
        
        # --- Standard Logic (If currently Pending) ---
        elif item.global_status == models.ItemGlobalStatus.PENDING:
             if item.qc_validation_status == models.ValidationState.APPROVED and \
                item.pm_validation_status == models.ValidationState.APPROVED:
                 
                 item.global_status = models.ItemGlobalStatus.PENDING_PD_APPROVAL

        # --- PD Logic ---
        if is_pd:
            if item.global_status != models.ItemGlobalStatus.PENDING_PD_APPROVAL:
                 # PD tried to approve, but it wasn't ready. Skip or Log.
                 print(f"Skipping Item {item.id}: PD tried to approve but status is {item.global_status}")
            else:
                item.global_status = models.ItemGlobalStatus.READY_FOR_ACT


    elif action == "REJECT":
        if not comment: raise ValueError("Comment is mandatory for rejection.")
        
        # Record History
        history = models.ItemRejectionHistory(
            bc_item_id=item.id,
            rejected_by_id=user.id,
            rejected_at=datetime.now(),
            comment=comment
        )
        db.add(history)
        
        # Update Status
        if is_qc: item.qc_validation_status = models.ValidationState.REJECTED
        if is_pm: item.pm_validation_status = models.ValidationState.REJECTED
        if is_pd:
            item.global_status = models.ItemGlobalStatus.POSTPONED
            item.postponed_until = datetime.now() + timedelta(weeks=3)


        # Increment Count
        item.rejection_count += 1

    # 3. Calculate Global Status (The "Boucle" Logic)
    
    # Case A: Permanent Rejection
    if item.rejection_count >= 15:
        item.global_status = models.ItemGlobalStatus.PERMANENTLY_REJECTED
        # TODO: Trigger Notification to Admin/All
    
    # Case B: Postponement (If EITHER rejected it)
    # We only trigger postponement if BOTH have voted and at least one is REJECTED.
    # OR, do you want immediate postponement? 
    # Let's wait for BOTH to vote so they can both see it as requested.
    elif item.qc_validation_status != models.ValidationState.PENDING and \
         item.pm_validation_status != models.ValidationState.PENDING:
         
        if item.qc_validation_status == models.ValidationState.REJECTED or \
           item.pm_validation_status == models.ValidationState.REJECTED:
            
            item.global_status = models.ItemGlobalStatus.POSTPONED
            item.postponed_until = datetime.now() + timedelta(weeks=3)
            
            # Reset individual flags for next round? 
            # Usually we keep them as record until the postponement is over.
            
        

    if item.qc_validation_status == models.ValidationState.APPROVED and \
       item.pm_validation_status == models.ValidationState.APPROVED and \
       item.global_status == models.ItemGlobalStatus.PENDING: # Only move if currently pending
        
        item.global_status = models.ItemGlobalStatus.PENDING_PD_APPROVAL

    db.commit()
    return item

def generate_act_record(db: Session, bc_id: int, creator_id: int, item_ids: List[int]):
    # 1. Fetch the Parent BC to check its Type
    bc = db.query(models.BonDeCommande).get(bc_id)
    if not bc:
        raise ValueError("BC not found.")

    # 2. Fetch the Items requested for Acceptance
    items = db.query(models.BCItem).options(
        joinedload(models.BCItem.merged_po) # Ensure we have the category
    ).filter(
        models.BCItem.id.in_(item_ids),
        models.BCItem.global_status == models.ItemGlobalStatus.READY_FOR_ACT
    ).all()
    
    if not items: 
        raise ValueError("No eligible items selected.")

    # --- NEW: CURRENT TAX RATE VERIFICATION ---
    
    current_year = datetime.now().year
    
    # We will also track the tax rate to use for the final calculation
    validated_tax_rate = None 

    for item in items:
        # A. Determine what the tax rate SHOULD be right now
        if bc.bc_type == models.BCType.PERSONNE_PHYSIQUE:
            current_statutory_rate = 0.0
        else:
            category = item.merged_po.category
            # Re-fetch the rate from your TaxRule table based on NOW
            current_statutory_rate = get_tax_rate(db, category=category, year=current_year)
        
        # B. Compare with what is stored on the BC Line
        # Use a small epsilon for float comparison
        if abs(item.applied_tax_rate - current_statutory_rate) > 0.001:
            raise ValueError(
                f"Tax Rate Mismatch for item '{item.merged_po.po_no}': "
                f"BC has {item.applied_tax_rate*100:.0f}%, but the current valid rate for {current_year} is {current_statutory_rate*100:.0f}%. "
                "The BC must be updated to reflect current tax laws before acceptance."
            )
        
        # C. Ensure consistency within the selected batch (all items must share the rate)
        if validated_tax_rate is None:
            validated_tax_rate = current_statutory_rate
        elif abs(validated_tax_rate - current_statutory_rate) > 0.001:
             raise ValueError("Cannot generate ACT with mixed tax rates in the selected items.")

    # ------------------------------------------

    # 3. Create Record
    now_str = datetime.now().strftime("%Y%m%d%H%M%S")
    act_number = f"ACT-{now_str}" 

    act = models.ServiceAcceptance(
        act_number=act_number,
        bc_id=bc_id,
        creator_id=creator_id,
        applied_tax_rate=validated_tax_rate # Store the validated rate
    )
    db.add(act)
    db.flush() 
    
    # 4. Link Items & Calculate Total HT
    total_ht = 0.0
    
    for item in items:
        item.act_id = act.id
        item.global_status = models.ItemGlobalStatus.ACCEPTED
        total_ht += (item.line_amount_sbc or 0.0)
    
    # 5. Final Calculation (Using the validated rate)
    total_tax = total_ht * validated_tax_rate
    total_ttc = total_ht + total_tax
    
    act.total_amount_ht = total_ht
    act.total_tax_amount = total_tax
    act.total_amount_ttc = total_ttc
    
    db.commit()
    db.refresh(act)
    return act

def check_rejections_and_notify(db: Session, background_tasks: BackgroundTasks = None):
    now = datetime.now()
    
    # --- 1. HANDLE PERMANENT REJECTION (Unchanged) ---
    perm_rejected = db.query(models.BCItem).filter(
        models.BCItem.rejection_count >= 5, # changed from 15 to 5 as per logic
        models.BCItem.global_status != models.ItemGlobalStatus.PERMANENTLY_REJECTED
    ).all()
    
    for item in perm_rejected:
        item.global_status = models.ItemGlobalStatus.PERMANENTLY_REJECTED
        admins = db.query(models.User).filter(models.User.role == "ADMIN").all()
        for admin in admins:
            create_notification(
                db, recipient_id=admin.id, type=models.NotificationType.SYSTEM,
                title="Permanent Rejection Alert",
                message=f"Item {item.id} has reached 5 rejections.",
                link=f"/configuration/acceptance/workflow/{item.bc_id}"
            )
            
    # --- 2. HANDLE 3-WEEK POSTPONEMENT REMINDERS ---
    
    # IMPORTANT: Use joinedload to prevent errors when accessing item.bc or item.merged_po in email
    expired_items = db.query(models.BCItem).options(
        joinedload(models.BCItem.bc),
        joinedload(models.BCItem.merged_po)
    ).filter(
        models.BCItem.global_status == models.ItemGlobalStatus.POSTPONED,
        models.BCItem.postponed_until <= now
    ).all()
    
    for item in expired_items:
        # Unlock item
        item.global_status = models.ItemGlobalStatus.PENDING
        item.postponed_until = None
        
        # Get the last person who rejected it
        last_rejection = db.query(models.ItemRejectionHistory).filter(
            models.ItemRejectionHistory.bc_item_id == item.id
        ).order_by(models.ItemRejectionHistory.rejected_at.desc()).first()
        
        if last_rejection:
            rejector = db.query(models.User).get(last_rejection.rejected_by_id)
            
            # --- FIX: Everything below must be inside this if block ---
            if rejector:
                # 1. App Notification
                create_notification(
                    db, recipient_id=rejector.id, type=models.NotificationType.TODO,
                    title="Rejection Period Ended",
                    message=f"Item {item.id} is ready for re-inspection (3 weeks passed).",
                    link=f"/configuration/acceptance/workflow/{item.bc_id}"
                )
                
                # 2. Email Notification
                if background_tasks and rejector.email:
                    html_body = f"""
                    <h3>Action Required: Re-Inspection</h3>
                    <p>Hello {rejector.first_name},</p>
                    <p>The 3-week postponement period for the following item has ended:</p>
                    <ul>
                        <li><strong>BC:</strong> {item.bc.bc_number if item.bc else 'N/A'}</li>
                        <li><strong>Item:</strong> {item.merged_po.item_description if item.merged_po else 'N/A'}</li>
                        <li><strong>Previous Rejection:</strong> {last_rejection.comment}</li>
                    </ul>
                    <p>Please log in to the portal to re-validate this item.</p>
                    """
                    
                    message = MessageSchema(
                        subject="SIB Portal - Item Unlocked",
                        recipients=[rejector.email],
                        body=html_body,
                        subtype=MessageType.html
                    )
                    fm = FastMail(conf)
                    background_tasks.add_task(fm.send_message, message)

    db.commit()
def get_sbc_kpis(db: Session, user: models.User):
    if user.role != "SBC" or not user.sbc_id:
        return {"total_bc_value": 0, "total_paid_amount": 0, "pending_payment": 0, "active_bc_count": 0}

    # 1. Total Value of Approved BCs (Contract Value)
    # This is the total potential revenue for the SBC from approved contracts.
    total_bc_value = db.query(func.sum(models.BonDeCommande.total_amount_ht)).filter(
        models.BonDeCommande.sbc_id == user.sbc_id,
        models.BonDeCommande.status == models.BCStatus.APPROVED
    ).scalar() or 0.0

    # 2. Count of Active BCs (in any active state)
    active_bc_count = db.query(models.BonDeCommande).filter(
        models.BonDeCommande.sbc_id == user.sbc_id,
        models.BonDeCommande.status.in_([
            models.BCStatus.SUBMITTED, models.BCStatus.PENDING_L2, models.BCStatus.APPROVED
        ])
    ).count()

    # 3. Total Amount Paid (Actual Work Done & Accepted)
    # We sum the SBC's specific line amounts, but ONLY for items that are 'ACCEPTED' (Work Done).
    # This represents money they have "earned" and likely invoiced.
    total_paid_amount = db.query(func.sum(models.BCItem.line_amount_sbc)).join(
        models.BonDeCommande
    ).filter(
        models.BonDeCommande.sbc_id == user.sbc_id,
        models.BCItem.global_status == models.ItemGlobalStatus.ACCEPTED
    ).scalar() or 0.0

    # 4. Pending Payment (Backlog)
    # This is the value of work assigned to them (in Approved BCs) but not yet accepted.
    pending_payment = total_bc_value - total_paid_amount

    return {
        "total_bc_value": total_bc_value,
        "total_paid_amount": total_paid_amount,
        "pending_payment": max(0, pending_payment), # Ensure no negative numbers
        "active_bc_count": active_bc_count
    }
# backend/app/crud.py

def get_sbc_acceptances(db: Session, user: models.User):
    """
    Fetches the specific BC Items assigned to this SBC that are approved for payment.
    """
    if user.role != "SBC" or not user.sbc_id:
        return []

    # Query the BCItem directly. 
    # Join BonDeCommande to check ownership (sbc_id).
    # Join MergedPO to get the description/site/po_no for display.
    items = db.query(models.BCItem).join(
        models.BonDeCommande
    ).join(
        models.MergedPO
    ).options(
        joinedload(models.BCItem.merged_po), # Load details for display
        joinedload(models.BCItem.act),       # Load ACT info if generated
        joinedload(models.BCItem.bc)         # Load BC number
    ).filter(
        models.BonDeCommande.sbc_id == user.sbc_id,
        
        # FILTER: Only show items that have passed all internal validation steps.
        # i.e., "READY_FOR_ACT" (Approved by PD, waiting for paper) or "ACCEPTED" (Paper generated)
        models.BCItem.global_status.in_([
            models.ItemGlobalStatus.READY_FOR_ACT, 
            models.ItemGlobalStatus.ACCEPTED
        ])
    ).order_by(models.BCItem.id.desc()).all()

    return items
def create_fund_request(db: Session, pd_user: int, items: list):
    # Generate ID
    count = db.query(models.FundRequest).count() + 1
    req_num = f"REQ-{datetime.now().year}-{str(count).zfill(3)}"
    
    new_req = models.FundRequest(
        request_number=req_num,
        requester_id=pd_user,
        status=models.FundRequestStatus.PENDING_APPROVAL,
        created_at=datetime.now()
    )
    db.add(new_req)
    db.flush() # Get ID
    
    for item in items:
        db_item = models.FundRequestItem(
            request_id=new_req.id,
            target_pm_id=item.pm_id,
            requested_amount=item.amount,
            approved_amount=0.0, 
            remarque=item.remarque # <--- Save the remark
        )
        db.add(db_item)
        
    db.commit()
    db.refresh(new_req) # Refresh to load relationships/IDs

    return new_req
def approve_fund_request(db: Session, req_id: int, admin_id: int, approved_items: dict):
    # approved_items is a dict: { item_id: approved_amount }
    
    req = db.query(models.FundRequest).get(req_id)
    if not req: return None
    
    req.status = models.FundRequestStatus.APPROVED_WAITING_FUNDS
    req.approver_id = admin_id
    req.approved_at = datetime.now()
    
    # Update amounts
    for item in req.items:
        if str(item.id) in approved_items:
            item.approved_amount = float(approved_items[str(item.id)])
            
    db.commit()
    return req
def confirm_fund_reception(db: Session, req_id: int, pd_user: int):
    req = db.query(models.FundRequest).get(req_id)
    if req.status != models.FundRequestStatus.APPROVED_WAITING_FUNDS:
        raise ValueError("Request not ready for confirmation")
        
    # Start Transaction
    req.status = models.FundRequestStatus.COMPLETED
    req.completed_at = datetime.now()
    
    for item in req.items:
        if item.approved_amount > 0:
            # 1. Get/Create Wallet for the PM
            wallet = db.query(models.Caisse).filter(models.Caisse.user_id == item.target_pm_id).first()
            if not wallet:
                wallet = models.Caisse(user_id=item.target_pm_id, balance=0.0)
                db.add(wallet)
                db.flush()
            
            # 2. Update Balance
            wallet.balance += item.approved_amount
            
            # 3. Create CREDIT Transaction
            trx = models.Transaction(
                caisse_id=wallet.id,
                type=models.TransactionType.CREDIT,
                amount=item.approved_amount,
                description=f"Refill Request {req.request_number}",
                related_request_id=req.id,
                created_by_id=pd_user
            )
            db.add(trx)
            
    db.commit()
    return req

def get_caisse_stats(db: Session, user: models.User):
   
    
    # 1. Récupérer ou Créer la caisse (Lazy Init)
    wallet = db.query(models.Caisse).filter(models.Caisse.user_id == user.id).first()
    
    if not wallet:
        wallet = models.Caisse(user_id=user.id, balance=0.0)
        db.add(wallet)
        db.commit()
        db.refresh(wallet)

    # 2. Calculer les fonds entrants en attente (Pending Incoming Funds)
    # Somme des montants demandés (PENDING) + approuvés mais non reçus (WAITING_FUNDS)
    
    # A. Demandes en attente d'approbation (on prend le montant demandé)
    pending_reqs = db.query(func.sum(models.FundRequestItem.requested_amount))\
        .join(models.FundRequest)\
        .filter(
            models.FundRequestItem.target_pm_id == user.id,
            models.FundRequest.status == models.FundRequestStatus.PENDING_APPROVAL
        ).scalar() or 0.0
    
    # B. Demandes approuvées en attente de cash (on prend le montant approuvé)
    waiting_funds = db.query(func.sum(models.FundRequestItem.approved_amount))\
        .join(models.FundRequest)\
        .filter(
            models.FundRequestItem.target_pm_id == user.id,
            models.FundRequest.status == models.FundRequestStatus.APPROVED_WAITING_FUNDS
        ).scalar() or 0.0
    
    pending_total = float(pending_reqs) + float(waiting_funds)

    # 3. Calculer le total dépensé ce mois-ci
    today = datetime.now()
    month_start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    spent_month = db.query(func.sum(models.Transaction.amount))\
        .filter(
            models.Transaction.caisse_id == wallet.id,
            models.Transaction.type == models.TransactionType.DEBIT,
            models.Transaction.created_at >= month_start
        ).scalar() or 0.0

    # 4. Retourner le dictionnaire final
    return {
        "balance": float(wallet.balance),
        "pending_in": float(pending_total),
        "spent_month": float(spent_month)
    }

def get_transactions(
    db: Session, 
    user: models.User, 
    page: int = 1, 
    limit: int = 20,
    type_filter: str = None,
    start_date: str = None,
    end_date: str = None,
    search: str = None
):
    """
    Fetches paginated transactions with filters.
    Security: PMs see only their own. Admins/PDs see all.
    """
    query = db.query(models.Transaction).join(models.Caisse).join(models.User, models.Caisse.user_id == models.User.id)
    
    # 1. Security Filter
    # If not Admin/PD, restrict to own wallet
    if user.role not in [models.UserRole.ADMIN, models.UserRole.PD]:
        query = query.filter(models.Caisse.user_id == user.id)
    
    # 2. Apply Filters
    if type_filter and type_filter != "ALL":
        query = query.filter(models.Transaction.type == type_filter)
        
    if start_date:
        query = query.filter(func.date(models.Transaction.created_at) >= start_date)
        
    if end_date:
        query = query.filter(func.date(models.Transaction.created_at) <= end_date)
        
    if search:
        term = f"%{search}%"
        # Search in description OR user name (for admins)
        query = query.filter(
            (models.Transaction.description.ilike(term)) |
            (models.User.first_name.ilike(term)) |
            (models.User.last_name.ilike(term))
        )

    # 3. Pagination
    total_items = query.count()
    
    # Order by newest first
    transactions = query.order_by(models.Transaction.created_at.desc())\
                        .offset((page - 1) * limit)\
                        .limit(limit)\
                        .all()
    
    # 4. Format Result
    # We need to flatten the user info for the frontend
    items = []
    for t in transactions:
        t_dict = t.__dict__.copy()
        if '_sa_instance_state' in t_dict: del t_dict['_sa_instance_state']
        
        # Add User Name (Owner of the wallet)
        t_dict['user_name'] = f"{t.caisse.user.first_name} {t.caisse.user.last_name}"
        
        # Add Creator Name (Who made the transaction)
        # Note: You might need to join 'created_by' relationship or fetch it if lazy loaded
        creator = db.query(models.User).get(t.created_by_id)
        t_dict['created_by_name'] = f"{creator.first_name} {creator.last_name}" if creator else "System"
        
        items.append(t_dict)

    return {
        "items": items,
        "total_items": total_items,
        "page": page,
        "limit": limit,
        "total_pages": (total_items + limit - 1) // limit
    }
    
def get_pending_requests(db: Session):
    """
    Returns all requests that need attention (Pending or Approved/Waiting).
    Used for the Dashboard table.
    """
    # Filter for statuses that are 'active' workflow steps
    query = db.query(models.FundRequest).filter(
        models.FundRequest.status.in_([
            models.FundRequestStatus.PENDING_APPROVAL,
            models.FundRequestStatus.APPROVED_WAITING_FUNDS,
            models.FundRequestStatus.PARTIALLY_PAID 

        ])
    ).order_by(models.FundRequest.created_at.desc())
    
    reqs = query.all()
    
    # Format
    results = []
    for r in reqs:
        # Calculate total amount for this request
        total = sum(i.requested_amount for i in r.items)
        results.append({
            "id": r.id,
            "request_number": r.request_number,
            "created_at": r.created_at,
            "status": r.status,
            "total_amount": total,
            "requester_name": f"{r.requester.first_name} {r.requester.last_name}"
        })
        
    return results
def get_request_by_id(db: Session, req_id: int):
    req = db.query(models.FundRequest).get(req_id)
    if not req:
        return None
    
    # Format Items
    items = []
    for i in req.items:
        items.append({
            "id": i.id,
            "target_pm_id": i.target_pm_id,
            "target_pm_name": f"{i.target_pm.first_name} {i.target_pm.last_name}",
            "requested_amount": i.requested_amount,
            "approved_amount": i.approved_amount or 0.0,
            
            # --- NEW FIELD ---
            "remarque": i.remarque, # Pass the remark to the frontend
            "admin_note": i.admin_note or ""
        })
    
    # Calculate total requested (for safety/display)
    total_requested = sum(item['requested_amount'] for item in items)

    return {
        "id": req.id,
        "request_number": req.request_number,
        "created_at": req.created_at,
        "status": req.status,
        "requester_id": req.requester_id,
        "requester_name": f"{req.requester.first_name} {req.requester.last_name}",
        "approver_id": req.approver_id,
        "approver_name": f"{req.approver.first_name} {req.approver.last_name}" if req.approver else "",
        "approved_at": req.approved_at,
        "completed_at": req.completed_at,
        
        # --- NEW FIELDS ---
        "total_amount": total_requested, # Or req.total_amount if you stored it on the parent
        "paid_amount": req.paid_amount or 0.0,
        "admin_comment": req.admin_comment,
        
        "items": items
    }


def get_all_wallets_summary(db: Session):
    """
    Returns a list of all PM wallets with their balance and pending incoming amount.
    Used for Admin/PD detailed view.
    """
    # 1. Get all PMs, PDs, and Admins (anyone who might have a wallet)
    pms = db.query(models.User).filter(
        models.User.role.in_([models.UserRole.PM, models.UserRole.PD, models.UserRole.ADMIN])
    ).all()
    
    results = []
    for pm in pms:
        # A. Get Wallet Balance
        wallet = db.query(models.Caisse).filter(models.Caisse.user_id == pm.id).first()
        balance = wallet.balance if wallet else 0.0
        
        # B. Get Pending Incoming (Logic similar to get_caisse_stats)
        # 1. Pending Approval (Use requested_amount)
        pending_reqs = db.query(func.sum(models.FundRequestItem.requested_amount)).join(models.FundRequest).filter(
            models.FundRequestItem.target_pm_id == pm.id,
            models.FundRequest.status == models.FundRequestStatus.PENDING_APPROVAL
        ).scalar() or 0.0
        
        # 2. Approved but not yet Confirmed/Paid (Use approved_amount)
        # Note: In our new logic, "Approved" means paid, so this might be 0, 
        # but we keep it for consistency with legacy statuses.
        waiting_funds = db.query(func.sum(models.FundRequestItem.approved_amount)).join(models.FundRequest).filter(
            models.FundRequestItem.target_pm_id == pm.id,
            models.FundRequest.status == models.FundRequestStatus.APPROVED_WAITING_FUNDS
        ).scalar() or 0.0
        
        # 3. Partial Pending (Use remaining requested)
        # This is tricky. If status is PARTIALLY_PAID, the "Pending" amount is 
        # (Total Requested for this PM - Amount Paid to this PM).
        # We need to find requests where this PM is a target and status is PARTIAL.
        # Simple approximation: Sum of requested - Sum of approved for partial items?
        # Actually, let's keep it simple: Pending In = Pending Approval + Waiting Funds.
        # Partial requests are technically "Approved" partially, so the rest is not guaranteed.
        
        results.append({
            "user_id": pm.id,
            "user_name": f"{pm.first_name} {pm.last_name}",
            "balance": balance,
            "pending_in": pending_reqs + waiting_funds
        })
        
    # Sort by balance descending to see who has the most cash
    return sorted(results, key=lambda x: x['balance'], reverse=True)


def process_fund_request(
    db: Session, 
    req_id: int, 
    payload: schemas.FundRequestReviewAction, 
    admin_id: int
):
    req = db.query(models.FundRequest).get(req_id)
    if not req: raise ValueError("Request not found")

    # 1. HANDLE REJECTION
    if payload.action == "REJECT":
        if not payload.comment:
            raise ValueError("A comment is mandatory when rejecting.")
        
        req.status = models.FundRequestStatus.REJECTED
        req.admin_comment = payload.comment
        req.approver_id = admin_id
        req.approved_at = datetime.now()
        # No money moves on rejection.
    
    # 2. HANDLE APPROVAL (Full or Partial)
    elif payload.action == "APPROVE":
        if not payload.items:
             raise ValueError("Approval requires items.")

        # --- FIX: Calculate Total Amount from Items ---
        amount_giving_now = sum(i.amount_to_pay for i in payload.items)
        # ---------------------------------------------

        if amount_giving_now <= 0:
             raise ValueError("Approval requires a positive amount.")

        # A. Update Global Request Tracking
        current_paid = req.paid_amount or 0.0
        req.paid_amount = current_paid + amount_giving_now
        
        req.approver_id = admin_id
        req.approved_at = datetime.now()
        req.admin_comment = payload.comment

        # B. DISTRIBUTE MONEY & SAVE NOTES
        for item_review in payload.items:
            # Find the DB item
            db_item = next((i for i in req.items if i.id == item_review.item_id), None)
            if not db_item: continue

            # 1. Update Admin Note
            if item_review.admin_note:
                db_item.admin_note = item_review.admin_note
            
            amount = item_review.amount_to_pay
            if amount <= 0: continue

            # 2. Update Item Approved Amount
            db_item.approved_amount = (db_item.approved_amount or 0.0) + amount

            # 3. Credit Specific PM Wallet
            pm_id = db_item.target_pm_id
            wallet = db.query(models.Caisse).filter(models.Caisse.user_id == pm_id).first()
            if not wallet:
                wallet = models.Caisse(user_id=pm_id, balance=0.0)
                db.add(wallet)
                db.flush()
                
            wallet.balance += amount
            
            # 4. Create Transaction
            trx = models.Transaction(
                caisse_id=wallet.id,
                type=models.TransactionType.CREDIT,
                amount=amount,
                description=f"Refill Request #{req.request_number} (Item #{db_item.id})",
                related_request_id=req.id,
                created_by_id=admin_id
            )
            db.add(trx)

        # C. Determine New Status
        total_requested = sum(i.requested_amount for i in req.items)
        remaining = total_requested - req.paid_amount
        
        if remaining <= 0.01:
            req.status = models.FundRequestStatus.COMPLETED
        else:
            if payload.close_request:
                req.status = models.FundRequestStatus.CLOSED_PARTIAL
            else:
                req.status = models.FundRequestStatus.PARTIALLY_PAID
    
    db.commit()
    db.refresh(req)
    return req
# ==================== EXPENSES CRUD ====================

def create_expense(db: Session, current_user, payload):
    expense = models.Expense(
        project_id=payload.project_id,
        exp_type=payload.exp_type,  # Changez expense_type en exp_type
        beneficiary=payload.beneficiary,
        amount=payload.amount,
        remark=payload.remark,
        attachment=payload.attachment,
         act_id=payload.act_id,# Maintenant que c'est dans le schéma, utilisez directement
        requester_id=current_user.id,
        status='PENDING_L1',
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )
    db.add(expense)
    db.commit()
    db.refresh(expense)
    return expense
# In crud.py

def list_my_requests(db: Session, current_user: models.User):
    # 1. Base Query
    query = db.query(models.Expense).options(
        joinedload(models.Expense.internal_project),
        joinedload(models.Expense.requester),
        # Ensure we load the relationships needed for SBC logic if applicable
        joinedload(models.Expense.act).joinedload(models.ServiceAcceptance.bc)
    )
    
    role_str = str(current_user.role).upper()
    print("user role(UPPER):", role_str)
    # 2. Logic for SBC (Personne Physique)
    if current_user.role in [UserRole.SBC]:
        if not current_user.sbc_id:
            print("account not related to any sbc")
            return [] # SBC User not linked to SBC Profile
        print("Filtering expenses for SBC ID:", current_user.sbc_id)
        # Filter expenses linked to this SBC's BCs via Acceptances
        query = query.join(models.ServiceAcceptance, models.Expense.act_id == models.ServiceAcceptance.id)\
                    .join(models.BonDeCommande, models.ServiceAcceptance.bc_id == models.BonDeCommande.id)\
                    .filter(models.BonDeCommande.sbc_id == current_user.sbc_id)
        
    # 3. Logic for PM (Requester) - existing logic
    elif "ADMIN" not in role_str and "PD" not in role_str and "CEO" not in role_str:
        query = query.filter(models.Expense.requester_id == current_user.id)
        
    # 4. Logic for Admin/PD - They see everything (no filter needed)
    
    # Add explicit ordering
    result = query.order_by(models.Expense.created_at.desc()).all()
    
    return result
def list_pending_l1(db: Session):
    """Liste toutes les dépenses en attente de validation L1 (PD)"""
    return db.query(models.Expense).filter(
        models.Expense.status == "PENDING_L1"
    ).order_by(models.Expense.created_at.desc()).all()


def list_pending_l2(db: Session):
    """Liste toutes les dépenses en attente de paiement L2 (Admin/CEO)"""
    return db.query(models.Expense).filter(
        models.Expense.status == "PENDING_L2"
    ).order_by(models.Expense.created_at.desc()).all()


def submit_expense(db: Session, expense_id: int):
    """Soumet une dépense pour validation L1"""
    expense = db.query(models.Expense).filter(models.Expense.id == expense_id).first()
    if not expense:
        raise ValueError("Dépense introuvable")
    
    if expense.status != "DRAFT":
        raise ValueError("Seules les dépenses en brouillon peuvent être soumises")
    
    # Vérifier le solde de la caisse
    caisse = db.query(models.Caisse).filter(
        models.Caisse.user_id == expense.creator_id
    ).first()
    
    if not caisse or caisse.balance < expense.amount:
        raise ValueError("Solde caisse insuffisant")
    
    expense.status = "PENDING_L1"
    db.commit()
    pds = db.query(models.User).filter(
        or_(
            models.User.role.ilike("PD"),
            models.User.role.ilike("PROJECT DIRECTOR"),
            models.User.role.ilike("PROJECTDIRECTOR"),
            models.User.role.ilike("%DIRECTOR%") # Cherche n'importe quoi contenant DIRECTOR
        )
    ).all()

    # DEBUG : Regardez votre terminal VS Code pour voir ce chiffre
    print(f"DEBUG NOTIF : Nombre de PD trouvés en base : {len(pds)}")

    for pd in pds:
        print(f"DEBUG NOTIF : Envoi à {pd.username} (ID: {pd.id})")
        create_notification(
            db,
            recipient_id=pd.id,
            type=models.NotificationType.TODO,
            title="Validation L1 Requise (Petty Cash)",
            message=f"Le PM {expense.requester.first_name} a soumis {expense.amount} MAD pour le projet {expense.internal_project.name}.",
            link="/expenses?tab=l1"
        )
    
    db.commit() # Très important pour enregistrer les notifs
    db.refresh(expense)
    return expense


def approve_expense_l1(db: Session, expense_id: int, approver_id: int):
    expense = db.query(models.Expense).get(expense_id)
    if not expense:
        return None
    
    expense.status = "PENDING_L2"
    # Utilisation correcte avec l'import de datetime
    expense.approved_l1_at = datetime.utcnow()  # ✅ Date L1
    expense.approved_l1_by = approver_id
    
    db.commit()
    admins = db.query(models.User).filter(models.User.role.ilike("ADMIN")).all()
    for admin in admins:
        create_notification(
            db, recipient_id=admin.id, type=models.NotificationType.TODO,
            title="Paiement Requis (L2)",
            message=f"La dépense #{expense.id} de {expense.amount} MAD a été validée par le PD. Merci de procéder au paiement.",
            link="/expenses?tab=l2"
        )
        
    db.commit()
    db.refresh(expense)
    return expense



def approve_l2(db: Session, expense_id: int, current_user: models.User):
    """Validation L2 et paiement par Admin/CEO"""
    expense = db.query(models.Expense).filter(models.Expense.id == expense_id).first()
    if not expense:
        return None
    
    if expense.status != "PENDING_L2":
        raise ValueError("Cette dépense n'est pas en attente de paiement")
    
    # ✅ CORRECTION ICI : Utilisez requester_id au lieu de creator_id
    caisse = db.query(models.Caisse).filter(
        models.Caisse.user_id == expense.requester_id 
    ).first()
    
    if not caisse or caisse.balance < expense.amount:
        raise ValueError("Insufficient balance in caisse")
    
    # Débiter la caisse
    caisse.balance -= expense.amount
    
    # Créer une transaction de débit
    transaction = models.Transaction(
        caisse_id=caisse.id,
        type=models.TransactionType.DEBIT,
        amount=expense.amount,
        description=f"Expense payment: {expense.exp_type}",
        created_by_id=current_user.id
    )
    db.add(transaction)
    
    # Mettre à jour le statut de la dépense
    expense.status = "PENDING_PAYMENT"
    expense.approved_l2_at = datetime.utcnow() 
    expense.approved_l2_by = current_user.id
    
    db.commit()
    create_notification(
        db, recipient_id=expense.requester_id, type=models.NotificationType.APP,
        title="Dépense Payée ✅",
        message=f"Votre demande de {expense.amount} MAD a été payée. Votre solde caisse a été mis à jour.",
        link="/expenses"
    )
    
    db.commit()
    db.refresh(expense)
    return expense


def reject_expense(db: Session, expense_id: int, current_user: models.User, reason: str):
    """Rejette une dépense"""
    expense = db.query(models.Expense).filter(models.Expense.id == expense_id).first()
    if not expense:
        return None
    
    expense.status = "REJECTED"
    expense.rejected_by = current_user.id
    expense.approved_l1_at = datetime.now()
    expense.rejection_reason = reason
    
    db.commit()
    create_notification(
        db, recipient_id=expense.requester_id, type=models.NotificationType.SYSTEM,
        title="Demande Rejetée ❌",
        message=f"Votre demande de {expense.amount} MAD a été rejetée. Motif: {reason}",
        link="/expenses"
    )
    
    db.commit()
    db.refresh(expense)
    return expense


def get_caisse_stats(db: Session, current_user: models.User):
    """Retourne les stats de la caisse de l'utilisateur"""
    caisse = db.query(models.Caisse).filter(
        models.Caisse.user_id == current_user.id
    ).first()
    
    if not caisse:
        # Créer une caisse si elle n'existe pas
        caisse = models.Caisse(user_id=current_user.id, balance=0.0)
        db.add(caisse)
        db.commit()
        db.refresh(caisse)
    
    return {
        "balance": caisse.balance,
        "user_id": current_user.id
    }


def get_payable_acts(db: Session, project_id: int):
    """Retourne les ACT validés mais non payés pour un projet"""
    # Récupère tous les ACT du projet qui sont APPROVED_L2
    acts = db.query(models.ServiceAcceptance).join(
        models.BonDeCommande
    ).filter(
        
        models.BonDeCommande.project_id == project_id,
        models.BonDeCommande.status == models.BCStatus.APPROVED,
    ).all()
    
    result = []
    for act in acts:
        # Vérifier si une expense existe déjà pour cet ACT
        existing = db.query(models.Expense).filter(
            models.Expense.act_id == act.id,
            models.Expense.status.in_(["PENDING_L1", "PENDING_L2", "PAID"])
        ).first()
        
        if not existing:
            result.append({
                "id": act.id,
                "act_number": act.act_number,
                "total_amount_ht": act.total_amount_ht
            })
    
    return result


def deduct_from_caisse(db: Session, user_id: int, amount: float, description: str):
    """Débite la caisse d'un utilisateur"""
    caisse = db.query(models.Caisse).filter(models.Caisse.user_id == user_id).first()
    
    if not caisse:
        raise ValueError("Caisse introuvable pour cet utilisateur")
    
    if caisse.balance < amount:
        raise ValueError("Solde insuffisant")
    
    caisse.balance -= amount
    
    # Créer une transaction
    transaction = models.Transaction(
        caisse_id=caisse.id,
        type=models.TransactionType.DEBIT,
        amount=amount,
        description=description
    )
    db.add(transaction)
    db.commit()
def get_expenses_export_dataframe(db: Session):
    # Requête avec toutes les jointures nécessaires
    query = db.query(models.Expense).options(
        joinedload(models.Expense.internal_project),
        joinedload(models.Expense.requester),
        joinedload(models.Expense.act)
    ).all()

    data = []
    for exp in query:
        data.append({
            "ID Dépense": exp.id,
            "Date Création": exp.created_at.strftime("%d/%m/%Y") if exp.created_at else "",
          "Projet": exp.internal_project.name if exp.internal_project else "N/A",
            "Demandeur (PM)": f"{exp.requester.first_name} {exp.requester.last_name}" if exp.requester else "N/A",
            "Type": exp.exp_type,
            "Bénéficiaire": exp.beneficiary,
            "Montant (MAD)": exp.amount,
            "Statut": exp.status,
            "Référence ACT": exp.act.act_number if exp.act else "N/A",
            "Remarque": exp.remark,
            "Motif de Rejet": exp.rejection_reason if exp.rejection_reason else ""
        })

    return pd.DataFrame(data)

def bulk_update_internal_control(db: Session, identifiers: List[str], new_value: int):
    # Clean input
    clean_ids = [s.strip() for s in identifiers if s.strip()]
    if not clean_ids: return 0
    
    # Update Query
    # Matches either PO ID or Site Code
    query = db.query(models.MergedPO).filter(
        sa.or_(
            models.MergedPO.po_id.in_(clean_ids),
            models.MergedPO.site_code.in_(clean_ids)
        )
    )
    
    updated_count = query.update(
        {models.MergedPO.internal_control: new_value},
        synchronize_session=False
    )
    db.commit()
    return updated_count

def search_pos_for_control(db: Session, identifiers: List[str]):
    clean_ids = [s.strip() for s in identifiers if s.strip()]
    if not clean_ids: return []
    
    return db.query(models.MergedPO).filter(
        sa.or_(
            models.MergedPO.po_id.in_(clean_ids),
            models.MergedPO.site_code.in_(clean_ids)
        )
    ).limit(1000).all()
def list_pending_payment(db: Session):
    """Liste les dépenses validées L2 et prêtes pour le paiement final"""
    return db.query(models.Expense).options(
        joinedload(models.Expense.internal_project),
        joinedload(models.Expense.requester)
    ).filter(models.Expense.status == "PENDING_PAYMENT").all()

def confirm_expense_payment(db: Session, expense_id: int, attachment_name: str):
    """Finalise le paiement, enregistre le reçu et débite le solde de la caisse"""
    expense = db.query(models.Expense).get(expense_id)
    if not expense:
        return None
    
    # 1. Récupérer la caisse du demandeur (le PM) pour déduire l'argent
  
    caisse = db.query(models.Caisse).filter(models.Caisse.user_id == expense.requester_id).first()
    
    if not caisse or caisse.balance < expense.amount:
        raise ValueError("Solde de caisse insuffisant pour effectuer ce paiement.")
    
    # 1. Débiter la caisse
    caisse.balance -= expense.amount
    
    # 2. Mettre à jour la dépense
    expense.status = "PAID"
    expense.attachment = attachment_name
    expense.updated_at = datetime.now()
    
    db.commit()
    create_notification(
        db,
        recipient_id=expense.requester_id,
        type=models.NotificationType.APP,
        title="Dépense Payée ✅",
        message=f"Votre demande de {expense.amount} MAD a été payée. Votre solde caisse a été débité.",
        link="/expenses"
    )
    db.commit()
    db.refresh(expense)
    return expense

def update_bon_de_commande(db: Session, bc_id: int, bc_data: schemas.BCCreate, user_id: int):
    # 1. Fetch existing BC
    bc = db.query(models.BonDeCommande).get(bc_id)
    if not bc: raise ValueError("BC not found")
    
    # 2. Checks
    if bc.status != models.BCStatus.DRAFT:
        raise ValueError("Only DRAFT BCs can be edited.")
    if bc.creator_id != user_id:
        raise ValueError("You can only edit your own BCs.")
        
    # 3. Update Header Info
    bc.project_id = bc_data.internal_project_id
    bc.sbc_id = bc_data.sbc_id
    
    # 4. Handle Items (Strategy: Delete old, Re-create new)
    # This is simplest to ensure consistency with quantity checks.
    # First, restore quantities or just delete if we re-check availability.
    
    # Delete existing items
    db.query(models.BCItem).filter(models.BCItem.bc_id == bc.id).delete()
    
    total_ht = 0.0
    total_tax = 0.0
    
    # Re-create items (Reuse logic from create_bon_de_commande)
    for item_data in bc_data.items:
        po = db.query(models.MergedPO).get(item_data.merged_po_id)
        if not po: raise ValueError(f"PO {item_data.merged_po_id} not found")
        
        # Recalculate availability (excluding THIS BC since we just deleted its items)
        consumed_qty = db.query(func.sum(models.BCItem.quantity_sbc)).filter(
            models.BCItem.merged_po_id == po.id
        ).scalar() or 0.0
        
        available_qty = po.requested_qty - consumed_qty
        
        if item_data.quantity_sbc > (available_qty + 0.0001):
             raise ValueError(f"Insufficient quantity for PO {po.po_id}")

        unit_price_sbc = (po.unit_price or 0) * item_data.rate_sbc
        line_amount_sbc = unit_price_sbc * item_data.quantity_sbc
        
        current_year = datetime.now().year
        tax_rate_val = get_tax_rate(db, category=po.category, year=current_year)
        line_tax = line_amount_sbc * tax_rate_val
        
        new_item = models.BCItem(
            bc_id=bc.id,
            merged_po_id=po.id,
            rate_sbc=item_data.rate_sbc,
            quantity_sbc=item_data.quantity_sbc,
            unit_price_sbc=unit_price_sbc,
            line_amount_sbc=line_amount_sbc,
            applied_tax_rate=tax_rate_val
        )
        db.add(new_item)
        total_ht += line_amount_sbc
        total_tax += line_tax
        
    bc.total_amount_ht = total_ht
    bc.total_tax_amount = total_tax
    bc.total_amount_ttc = total_ht + total_tax
    
    db.commit()
    db.refresh(bc)
    return bc
