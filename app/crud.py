from datetime import datetime,date
from sqlalchemy.orm import Session
from typing import List, Optional
from . import auth
from . import models, schemas
import pandas as pd
from sqlalchemy.orm import joinedload,Query
import sqlalchemy as sa
from sqlalchemy import func, case, extract, and_
from sqlalchemy.sql.functions import coalesce # More explicit import
from sqlalchemy.orm import aliased
from .enum import ProjectType, UserRole
PAYMENT_TERM_MAP = {
    "【TT】▍AC1 (80.00%, INV AC -15D, Complete 80%) / AC2 (20.00%, INV AC -15D, Complete 100%) ▍": "AC1 80 | PAC 20",
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
            
            # Update assignment in case rules changed or site changed
            merged_po.internal_project_id = final_internal_project_id

        else:
            # INSERT
            new_merged_po = models.MergedPO(
                po_id=po_id,
                raw_po_id=po.id,
                customer_project_id=customer_project.id,
                internal_project_id=final_internal_project_id, # <--- Assigned here
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

def create_raw_acceptances_from_dataframe(db: Session, df: pd.DataFrame, user_id: int):
    df['uploader_id'] = user_id
    records = df.to_dict("records")
    db.bulk_insert_mappings(models.RawAcceptance, records)
    db.commit()
    return len(records)



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
    )
    db.add(history_record)
    db.commit()
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

def deduce_category(description: str) -> str:
    """Deduces the category based on keywords in the item description."""
    if not isinstance(description, str):
        return "TBD"

    description_lower = description.lower()

    # Using simple 'in' checks for broad matching
    if "transport" in description_lower:
        return "Transportation"
    if "survey" in description_lower:
        return "Survey"
    if "site engineer" in description_lower or "fsc" in description_lower:
        return "Site Engineer"

    # If it's none of the specific keywords above, default to "Service"
    # This covers the vast majority of your examples.
    # Add more specific checks above this line if needed.
    if (
        "service" in description_lower
        or "install" in description_lower
        or "zone" in description_lower
    ):
        return "Service"

    return "TBD"  # Default if no keywords match


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
def get_total_financial_summary(db: Session) -> dict:
    # Use the SQLAlchemy func module to perform SUM aggregations
    # coalesce(value, 0) is used to turn NULL results into 0.0
    total_po_value = db.query(func.sum(models.MergedPO.line_amount_hw)).scalar() or 0.0
    total_accepted_ac = db.query(func.sum(models.MergedPO.accepted_ac_amount)).scalar() or 0.0
    total_accepted_pac = db.query(func.sum(models.MergedPO.accepted_pac_amount)).scalar() or 0.0
    
    remaining_gap = total_po_value - (total_accepted_ac + total_accepted_pac)
    
    return {
        "total_po_value": total_po_value,
        "total_accepted_ac": total_accepted_ac,
        "total_accepted_pac": total_accepted_pac,
        "remaining_gap": remaining_gap
    }
def get_internal_projects_financial_summary(db: Session):
    results = db.query(
        models.InternalProject.id.label("project_id"),
        models.InternalProject.name.label("project_name"),
        func.sum(models.MergedPO.line_amount_hw).label("total_po_value"),
        (func.sum(models.MergedPO.accepted_ac_amount) + func.sum(models.MergedPO.accepted_pac_amount)).label("total_accepted")
    ).select_from(models.MergedPO)\
    .join(models.InternalProject, models.MergedPO.internal_project_id == models.InternalProject.id)\
    .group_by(models.InternalProject.id, models.InternalProject.name).all() 
    summary_list = []
    for row in results:
        po_value = row.total_po_value or 0
        accepted = row.total_accepted or 0
        gap = po_value - accepted
        completion = (accepted / po_value * 100) if po_value > 0 else 0
        
        # We need to find the project_id. This is a simplification.
        # A more robust solution would join with the projects table.
        project = db.query(models.InternalProject).filter(models.InternalProject.name == row.project_name).first()

        summary_list.append({
            "project_id": project.id if project else 0,
            "project_name": row.project_name,
            "total_po_value": po_value,
            "total_accepted": accepted,
            "remaining_gap": gap,
            "completion_percentage": completion
        })
    return summary_list
def get_customer_projects_financial_summary(db: Session):
    # This query groups by the CustomerProject.
    results = db.query(
        models.CustomerProject.id.label("project_id"),
        models.CustomerProject.name.label("project_name"),
        func.sum(models.MergedPO.line_amount_hw).label("total_po_value"),
        (func.sum(models.MergedPO.accepted_ac_amount) + func.sum(models.MergedPO.accepted_pac_amount)).label("total_accepted")
    ).select_from(models.MergedPO).join(
        models.CustomerProject
    ).group_by(models.CustomerProject.id, models.CustomerProject.name).all()
    
    # The summary calculation logic is identical, just on a different grouping.
    summary_list = []
    for row in results:
        po_value = row.total_po_value or 0
        accepted = row.total_accepted or 0
        gap = po_value - accepted
        completion = (accepted / po_value * 100) if po_value > 0 else 0
        
        summary_list.append({
            "project_id": row.project_id,
            "project_name": row.project_name,
            "total_po_value": po_value,
            "total_accepted": accepted,
            "remaining_gap": gap,
            "completion_percentage": completion
        })
    return summary_list

def get_po_value_by_category(db: Session):
    """
    Calculates the total PO value for each category, correctly grouping NULL
    and 'TBD' values together at the database level.
    """
    
    # --- THIS IS THE FIX ---
    # We use `coalesce` to tell the database: "if the category is NULL, use 'TBD' instead."
    # This happens BEFORE the GROUP BY, so the aggregation is correct.
    category_label = coalesce(models.MergedPO.category, "TBD").label("category_name")

    results = db.query(
        category_label,
        func.sum(models.MergedPO.line_amount_hw).label("total_value")
    ).group_by(category_label).all()
    
    # Now, the Python part is much simpler because the data is already clean.
    # The 'row' object will have attributes 'category_name' and 'total_value'.
    return [{"category": row.category_name, "value": row.total_value or 0} for row in results]

# backend/app/crud.py

# backend/app/crud.py

def get_remaining_to_accept_paginated(
    db: Session, 
    page: int = 1, 
    size: int = 20, 
    filter_stage: str = "ALL",
    # --- NEW FILTERS ---
    search: Optional[str] = None,
    internal_project_id: Optional[int] = None,
    customer_project_id: Optional[int] = None
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


    # 3. Apply Filters
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
def get_remaining_stats(db: Session):
    remaining_expr = models.MergedPO.line_amount_hw - (
        func.coalesce(models.MergedPO.accepted_ac_amount, 0) + func.coalesce(models.MergedPO.accepted_pac_amount, 0)
    )
    stage_expr = case(
        (models.MergedPO.date_ac_ok.is_(None), "WAITING_AC"),
        (and_(models.MergedPO.date_ac_ok.isnot(None), models.MergedPO.date_pac_ok.is_(None)), "WAITING_PAC"),
        else_="PARTIAL_GAP"
    )
    
    # We group by stage and sum the gap
    stats = db.query(
        stage_expr.label("stage"),
        func.count(models.MergedPO.id).label("count"),
        func.sum(remaining_expr).label("total_gap")
    ).filter(
        func.abs(remaining_expr) > 0.01
    ).group_by(stage_expr).all()
    
    return {row.stage: {"count": row.count, "gap": row.total_gap or 0} for row in stats}


def get_financial_summary_by_period(
    db: Session, 
    year: int, 
    month: Optional[int] = None, 
    week: Optional[int] = None
) -> dict:
    """
    Calculates the financial summary for a specific period (year, month, or week)
    using the correct date fields for each metric via conditional aggregation.
    """
    
    # --- Define the date filters for each metric ---
    # We will build a list of conditions for each date column
    
    # Filter for Total PO Value (based on publish_date)
    po_date_filters = [extract('year', models.MergedPO.publish_date) == year]
    
    # Filter for Accepted AC (based on date_ac_ok)
    ac_date_filters = [extract('year', models.MergedPO.date_ac_ok) == year]
    
    # Filter for Accepted PAC (based on date_pac_ok)
    pac_date_filters = [extract('year', models.MergedPO.date_pac_ok) == year]

    # Add month or week filters if they are provided
    if month:
        po_date_filters.append(extract('month', models.MergedPO.publish_date) == month)
        ac_date_filters.append(extract('month', models.MergedPO.date_ac_ok) == month)
        pac_date_filters.append(extract('month', models.MergedPO.date_pac_ok) == month)

    if week:
        po_date_filters.append(extract('week', models.MergedPO.publish_date) == week)
        ac_date_filters.append(extract('week', models.MergedPO.date_ac_ok) == week)
        pac_date_filters.append(extract('week', models.MergedPO.date_pac_ok) == week)

    # --- Perform the conditional aggregation in a single, efficient query ---
    summary = db.query(
        # 1. Sum line_amount_hw IF its publish_date matches the period
        func.sum(case((and_(*po_date_filters), models.MergedPO.line_amount_hw), else_=0)).label("total_po_value"),
        
        # 2. Sum accepted_ac_amount IF its date_ac_ok matches the period
        func.sum(case((and_(*ac_date_filters), models.MergedPO.accepted_ac_amount), else_=0)).label("total_accepted_ac"),
        
        # 3. Sum accepted_pac_amount IF its date_pac_ok matches the period
        func.sum(case((and_(*pac_date_filters), models.MergedPO.accepted_pac_amount), else_=0)).label("total_accepted_pac")
        
    ).one() # .one() executes the query and returns the single row of results

    # Process the results (this part is the same)
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

def get_yearly_chart_data(db: Session, year: int):
    # We need to get data for all 12 months
    months = db.query(
        extract('month', models.MergedPO.publish_date).label("month")
    ).filter(
        extract('year', models.MergedPO.publish_date) == year
    ).distinct().all()

    monthly_data = []
    for month_row in months:
        month = month_row.month
        if not month: continue

        # For each month, run our powerful conditional aggregation
        summary = get_financial_summary_by_period(db=db, year=year, month=month)
        
        total_paid = summary["total_accepted_ac"] + summary["total_accepted_pac"]
        monthly_data.append({
            "month": month,
            "total_po_value": summary["total_po_value"],
            "total_paid": total_paid
        })
        
    return sorted(monthly_data, key=lambda x: x['month'])
def get_export_dataframe(
    db: Session,
    internal_project_id: Optional[int] = None,
    customer_project_id: Optional[int] = None,
    site_code: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    search: Optional[str] = None
) -> pd.DataFrame:
    
    CustProj = aliased(models.CustomerProject)
    IntProj = aliased(models.InternalProject)
    
    # --- UPDATED SELECT STATEMENT ---
    query = db.query(
        IntProj.name.label("Internal Project"),
        CustProj.name.label("Customer Project"),
        models.MergedPO.site_code.label("Site Code"),
        models.MergedPO.po_id.label("PO ID"),
        models.MergedPO.po_no.label("PO No."),
        models.MergedPO.po_line_no.label("PO Line No."),
        models.MergedPO.item_description.label("Item Description"),
        models.MergedPO.category.label("Category"),
        models.MergedPO.publish_date.label("Publish Date"),
        
        # 1. Base Amount
        models.MergedPO.line_amount_hw.label("Line Amount"),
        
        # 2. AC Columns (Total 80% vs Accepted)
        models.MergedPO.total_ac_amount.label("Total AC (80%)"), # Added
        models.MergedPO.accepted_ac_amount.label("Accepted AC Amount"),
        models.MergedPO.date_ac_ok.label("Date AC OK"),
        
        # 3. PAC Columns (Total 20% vs Accepted)
        models.MergedPO.total_pac_amount.label("Total PAC (20%)"), # Added
        models.MergedPO.accepted_pac_amount.label("Accepted PAC Amount"),
        models.MergedPO.date_pac_ok.label("Date PAC OK"),

        # 4. Calculated Remaining (Light Red)
        # Logic: Line Amount - (Accepted AC + Accepted PAC)
        (models.MergedPO.line_amount_hw - (
            func.coalesce(models.MergedPO.accepted_ac_amount, 0) + 
            func.coalesce(models.MergedPO.accepted_pac_amount, 0)
        )).label("Remaining Amount")
    ).select_from(models.MergedPO)


    # 2. Fix Joins
    # Join Customer Project
    query = query.join(CustProj, models.MergedPO.customer_project_id == CustProj.id)
    
    # FIX: Join Internal Project DIRECTLY from MergedPO
    query = query.join(IntProj, models.MergedPO.internal_project_id == IntProj.id, isouter=True)

    # 3. Apply Filters
    if internal_project_id:
        query = query.filter(IntProj.id == internal_project_id)
    if customer_project_id:
        query = query.filter(CustProj.id == customer_project_id)
    if site_code:
        query = query.filter(models.MergedPO.site_code == site_code)
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
            db_target.target_po_amount = target.target_po_amount
        if target.target_invoice_amount is not None:
            db_target.target_invoice_amount = target.target_invoice_amount
    else:
        # Create new
        db_target = models.UserPerformanceTarget(**target.model_dump())
        db.add(db_target)
    
    db.commit()
    return db_target

def get_performance_matrix(
    db: Session, 
    year: int, 
    month: Optional[int] = None, 
    filter_user_id: Optional[int] = None # <--- NEW PARAMETER
):
    """
    Generates the data for both Monthly and Yearly widgets.
    """
    
    # 1. Start query for eligible users (PMs, Admins, CEOs)
    query = db.query(models.User).filter(models.User.role.in_(['PM', 'ADMIN', 'PD']))
    
    # 2. Apply Security Filter (Row-Level Security)
    if filter_user_id:
        query = query.filter(models.User.id == filter_user_id)
        
    pms = query.all()
    
    results = []

    for pm in pms:

        # A. Fetch Targets (Plan)
        target_query = db.query(
            func.sum(models.UserPerformanceTarget.target_po_amount),
            func.sum(models.UserPerformanceTarget.target_invoice_amount)
        ).filter(
            models.UserPerformanceTarget.user_id == pm.id,
            models.UserPerformanceTarget.year == year
        )
        
        if month:
            target_query = target_query.filter(models.UserPerformanceTarget.month == month)
            
        plan_po, plan_invoice = target_query.first()
        plan_po = plan_po or 0.0
        plan_invoice = plan_invoice or 0.0

        # B. Fetch Actuals (From MergedPO)
        # We reuse the logic from get_user_performance_stats but specific to the period
        
        # Base filter: Projects managed by this PM
        base_filters = [models.InternalProject.project_manager_id == pm.id]
        
        # Date filters
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
            models.InternalProject, models.MergedPO.internal_project_id == models.InternalProject.id
        ).first()

        actual_po = summary[0] or 0.0
        actual_paid = (summary[1] or 0.0) + (summary[2] or 0.0)
        
        # Total Gap (Remaining to be paid on what was produced)
        total_gap = actual_po - actual_paid

        results.append({
            "user_id": pm.id,
            "user_name": f"{pm.first_name} {pm.last_name}",
            "total_gap": total_gap,
            "plan_po": plan_po,
            "actual_po": actual_po,
            "percent_po": (actual_po / plan_po * 100) if plan_po > 0 else 0,
            "plan_invoice": plan_invoice,
            "actual_invoice": actual_paid,
            "percent_invoice": (actual_paid / plan_invoice * 100) if plan_invoice > 0 else 0,
        })
        
    return results
def get_yearly_matrix_data(db: Session, year: int):
    # 1. Get all PMs
    pms = db.query(models.User).filter(models.User.role.in_(['PM', 'ADMIN', 'CEO'])).all()
    
    matrix_data = []

    for pm in pms:
        # Initialize arrays for 12 months (0.0)
        target_po_monthly = [0.0] * 12
        actual_po_monthly = [0.0] * 12
        target_inv_monthly = [0.0] * 12
        actual_inv_monthly = [0.0] * 12

        # 2. Fetch ALL Targets for this year for this PM
        targets = db.query(models.UserPerformanceTarget).filter(
            models.UserPerformanceTarget.user_id == pm.id,
            models.UserPerformanceTarget.year == year
        ).all()

        for t in targets:
            # Month is 1-based, array is 0-based
            if 1 <= t.month <= 12:
                target_po_monthly[t.month - 1] = t.target_po_amount
                target_inv_monthly[t.month - 1] = t.target_invoice_amount

        # 3. Fetch ALL Actuals (Grouped by Month)
        # We do 2 queries: one for PO (publish_date), one for Invoice (AC/PAC dates)
        
        # A. Actual POs
        po_results = db.query(
            extract('month', models.MergedPO.publish_date).label('month'),
            func.sum(models.MergedPO.line_amount_hw)
        ).join(models.InternalProject).filter(
            models.InternalProject.project_manager_id == pm.id,
            extract('year', models.MergedPO.publish_date) == year
        ).group_by('month').all()

        for m, val in po_results:
            if m: actual_po_monthly[int(m) - 1] = val or 0

        # B. Actual Invoices (Paid) - This is trickier because AC and PAC have different dates.
        # We iterate 1-12 and query efficiently or fetch all and aggregate in python.
        # Let's fetch all accepted items for this PM and year and bucket them in Python.
        
        # (Simplified logic for performance: Fetch items where EITHER date is in year)
        paid_items = db.query(models.MergedPO).join(models.InternalProject).filter(
            models.InternalProject.project_manager_id == pm.id,
            (extract('year', models.MergedPO.date_ac_ok) == year) | (extract('year', models.MergedPO.date_pac_ok) == year)
        ).all()

        for item in paid_items:
            # Add AC amount to the AC month
            if item.date_ac_ok and item.date_ac_ok.year == year:
                actual_inv_monthly[item.date_ac_ok.month - 1] += (item.accepted_ac_amount or 0)
            
            # Add PAC amount to the PAC month
            if item.date_pac_ok and item.date_pac_ok.year == year:
                actual_inv_monthly[item.date_pac_ok.month - 1] += (item.accepted_pac_amount or 0)

        # 4. Construct the Rows
        rows = [
            { "name": "Target PO Received", "values": target_po_monthly, "total": sum(target_po_monthly) },
            { "name": "Actual PO Received", "values": actual_po_monthly, "total": sum(actual_po_monthly) },
            { "name": "Target Invoice", "values": target_inv_monthly, "total": sum(target_inv_monthly) },
            { "name": "Actual Invoice", "values": actual_inv_monthly, "total": sum(actual_inv_monthly) }
        ]

        matrix_data.append({
            "pm_name": f"{pm.first_name} {pm.last_name}",
            "milestones": rows
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
    
    elif user.role in [UserRole.PM, UserRole.PD]:
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
def get_remaining_to_accept_paginated(
    db: Session, 
    page: int = 1, 
    size: int = 20, 
    filter_stage: str = "ALL",
    search: Optional[str] = None,
    internal_project_id: Optional[int] = None,
    customer_project_id: Optional[int] = None
):
    # 1. Expressions
    remaining_expr = models.MergedPO.line_amount_hw - (
        func.coalesce(models.MergedPO.accepted_ac_amount, 0) + 
        func.coalesce(models.MergedPO.accepted_pac_amount, 0)
    )
    
    stage_expr = case(
        (models.MergedPO.date_ac_ok.is_(None), "WAITING_AC"),
        (and_(models.MergedPO.date_ac_ok.isnot(None), models.MergedPO.date_pac_ok.is_(None)), "WAITING_PAC"),
        else_="PARTIAL_GAP"
    )

    # 2. Base Query with Eager Loading (To get Project Names)
    query = db.query(
        models.MergedPO,
        remaining_expr.label("remaining_amount"),
        stage_expr.label("remaining_stage")
    ).options(
        joinedload(models.MergedPO.internal_project), # <--- Critical for Project Name
        joinedload(models.MergedPO.customer_project)  # <--- Critical for Customer Project Name
    ).filter(
        func.abs(remaining_expr) > 0.01
    )

    # 3. APPLY FILTERS (This is the part that was likely failing)
    
    # Stage Filter
    if filter_stage != "ALL":
        query = query.filter(stage_expr == filter_stage)
        
    # Internal Project Filter
    if internal_project_id:
        query = query.filter(models.MergedPO.internal_project_id == internal_project_id)
        
    # Customer Project Filter
    if customer_project_id:
        query = query.filter(models.MergedPO.customer_project_id == customer_project_id)

    # Search Filter
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
        po_dict = po.__dict__.copy() # Copy to avoid mutation issues
        if '_sa_instance_state' in po_dict: del po_dict['_sa_instance_state']
        
        po_dict['remaining_amount'] = rem_amount
        po_dict['remaining_stage'] = stage
        
        # --- FIX: Populate Names manually if serialization fails ---
        if po.internal_project:
            po_dict['internal_project_name'] = po.internal_project.name
        else:
            po_dict['internal_project_name'] = "—"
            
        if po.customer_project:
            po_dict['customer_project_name'] = po.customer_project.name
        else:
            po_dict['customer_project_name'] = "—"

        items.append(po_dict)

    return {
        "items": items,
        "total_items": total_items,
        "page": page,
        "size": size,
        "total_pages": (total_items + size - 1) // size
    }

def get_remaining_to_accept_dataframe(
    db: Session,
    filter_stage: str = "ALL",
    search: Optional[str] = None,
    internal_project_id: Optional[int] = None,
    customer_project_id: Optional[int] = None
) -> pd.DataFrame:
    """
    Construit une requête pour l'export "Remaining To Accept" en se basant sur les filtres,
    et retourne un DataFrame Pandas prêt à être exporté.
    """
    remaining_expr = models.MergedPO.line_amount_hw - (
        func.coalesce(models.MergedPO.accepted_ac_amount, 0) + 
        func.coalesce(models.MergedPO.accepted_pac_amount, 0)
    )
    stage_expr = case(
        (models.MergedPO.date_ac_ok.is_(None), "WAITING_AC"),
        (and_(models.MergedPO.date_ac_ok.isnot(None), models.MergedPO.date_pac_ok.is_(None)), "WAITING_PAC"),
        else_="PARTIAL_GAP"
    )

    # --- CORRECTION DE LA REQUÊTE AVEC LES BONNES JOINTURES ---
    query = db.query(
        models.MergedPO.po_no,
        models.MergedPO.site_code,
        models.MergedPO.item_description,
        models.InternalProject.name.label("internal_project_name"), # On sélectionne le nom depuis la table jointe
        models.CustomerProject.name.label("customer_project_name"), # Idem pour le projet client
        models.MergedPO.line_amount_hw,
        models.MergedPO.accepted_ac_amount,
        models.MergedPO.accepted_pac_amount,
        remaining_expr.label("remaining_amount"),
        stage_expr.label("remaining_stage"),
        models.MergedPO.publish_date
    ).select_from(models.MergedPO).outerjoin(
        models.InternalProject, models.MergedPO.internal_project_id == models.InternalProject.id
    ).outerjoin(
        models.CustomerProject, models.MergedPO.customer_project_id == models.CustomerProject.id
    ).filter(
        func.abs(remaining_expr) > 0.01
    )

    # 3. On applique les mêmes filtres que pour la pagination
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
    
    # 4. On exécute la requête et on la charge dans un DataFrame SANS pagination
    df = pd.read_sql(query.statement, db.bind)

    if df.empty:
        return pd.DataFrame()

    # 5. On renomme les colonnes pour le fichier Excel
    df.rename(columns={
        'po_no': 'PO Number',
        'site_code': 'Site Code',
        'item_description': 'Item Description',
        'name': 'Internal Project', # Nom de colonne après la jointure
        'line_amount_hw': 'Total PO Value',
        'accepted_ac_amount': 'Accepted AC',
        'accepted_pac_amount': 'Accepted PAC',
        'remaining_amount': 'Remaining to Accept',
        'remaining_stage': 'Stage',
        'publish_date': 'Publish Date'
    }, inplace=True)
    
    return df


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


def bulk_assign_sites_to_internal_project_by_code(
    db: Session,
    site_codes: List[str],
    internal_project_name: str,
) -> int:
    
    """
    Assigne PLUSIEURS sites (via liste de site_code)
    à un PROJET INTERNE (via nom).
    Retourne le nombre total de lignes MergedPO mises à jour.
    """

    if not site_codes:
        return 0

    # 1. Récupérer le projet interne
    internal_project = (
        db.query(models.InternalProject)
        .filter(models.InternalProject.name == internal_project_name)
        .first()
    )
    if not internal_project:
        return 0

    # 2. Récupérer tous les sites correspondants
    sites = (
        db.query(models.Site)
        .filter(models.Site.site_code.in_(site_codes))
        .all()
    )
    if not sites:
        return 0

    site_ids = [s.id for s in sites]

    # 3. Créer / mettre à jour les allocations SiteProjectAllocation
    existing_allocs = (
        db.query(models.SiteProjectAllocation)
        .filter(models.SiteProjectAllocation.site_id.in_(site_ids))
        .all()
    )
    alloc_by_site_id = {a.site_id: a for a in existing_allocs}

    for site in sites:
        alloc = alloc_by_site_id.get(site.id)
        if alloc:
            alloc.internal_project_id = internal_project.id
        else:
            db.add(
                models.SiteProjectAllocation(
                    site_id=site.id,
                    internal_project_id=internal_project.id,
                )
            )

    # 4. Mise à jour massive des MergedPO
    total_updated = (
        db.query(models.MergedPO)
        .filter(models.MergedPO.site_id.in_(site_ids))
        .update(
            {models.MergedPO.internal_project_id: internal_project.id},
            synchronize_session=False,
        )
    )

    db.commit()
    return total_updated