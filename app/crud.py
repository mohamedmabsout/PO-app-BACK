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




def process_and_merge_pos(db: Session):
    """
    Final, robust version: Processes unprocessed RawPurchaseOrders, de-duplicates,
    applies rules, and inserts/updates the MergedPO table.
    """
    # 1. Get the "To Be Determined" project ID
    tbd_project = db.query(models.InternalProject).filter_by(name="To Be Determined").first()
    if not tbd_project:
        tbd_project = models.InternalProject(name="To Be Determined")
        db.add(tbd_project)
        db.commit()
    tbd_project_id = tbd_project.id

    # 2. Query for unprocessed raw POs and EAGERLY LOAD their related Site objects
    unprocessed_pos_query = db.query(models.RawPurchaseOrder).filter(models.RawPurchaseOrder.is_processed == False)
    unprocessed_pos = unprocessed_pos_query.options(joinedload(models.RawPurchaseOrder.site)).all()
    
    if not unprocessed_pos:
        return 0
    
    # --- NEW: Hydrate the CustomerProject table ---
    # 1. Get all unique customer project names from the raw data
    customer_project_names = {po.project_code for po in unprocessed_pos if po.project_code}
    
    # 2. Find which ones already exist in our CustomerProject table
    existing_cust_projs = {p.name: p for p in db.query(models.CustomerProject).filter(models.CustomerProject.name.in_(customer_project_names)).all()}
    
    # 3. For the new ones, apply rules and create them
    for name in customer_project_names:
        if name not in existing_cust_projs:
            internal_project_id = get_internal_project_id_from_rules(db, name, tbd_project_id)
            new_cust_proj = models.CustomerProject(name=name, internal_project_id=internal_project_id)
            db.add(new_cust_proj)
    
    db.commit() # Commit all new customer projects
    
    # 4. Create a final map for lookup: Customer Project Name -> CustomerProject Object
    all_cust_projs_map = {p.name: p for p in db.query(models.CustomerProject).filter(models.CustomerProject.name.in_(customer_project_names)).all()}
    # 3. De-duplicate the raw POs in memory to get the latest version of each line
    unique_pos_map = {}
    for po in unprocessed_pos:
        key = (po.po_no, po.po_line_no)
        # If the key already exists, only replace it if the new one has a later publish date
        if key not in unique_pos_map or po.publish_date > unique_pos_map[key].publish_date:
            unique_pos_map[key] = po
            
    clean_pos_list = list(unique_pos_map.values())

    # 4. Prepare for batch processing
    po_ids_to_check = [f"{po.po_no}-{po.po_line_no}" for po in clean_pos_list]
    existing_merged_map = {mp.po_id: mp for mp in db.query(models.MergedPO).filter(models.MergedPO.po_id.in_(po_ids_to_check)).all()}

    # 5. Loop through the CLEAN list of PO objects
    for po in clean_pos_list:
        po_id = f"{po.po_no}-{po.po_line_no}"
        # Get the CustomerProject object from our map
        customer_project = all_cust_projs_map.get(po.project_code)
        if not customer_project: continue # Skip if no matching customer project was found


        # --- UPDATE PATH ---
        if po_id in existing_merged_map:
            merged_po = existing_merged_map[po_id]
            
            # Your critical update logic
            if po.requested_qty == 0:
                merged_po.requested_qty = 0
                merged_po.line_amount_hw = 0
            else:
                merged_po.requested_qty = po.requested_qty
                merged_po.unit_price = po.unit_price
                merged_po.line_amount_hw = (po.unit_price or 0) * (po.requested_qty or 0)
            
            # Always update these fields on an update
            # merged_po.internal_project_id = customer_project.internal_project_id
            merged_po.customer_project_id = customer_project.id
            merged_po.publish_date = po.publish_date
            merged_po.site_id = po.site_id
            merged_po.site_code = po.site.site_code if po.site else None
        
        # --- INSERT PATH ---
        else:
            new_merged_po = models.MergedPO(
                po_id=po_id,
                raw_po_id=po.id,
                customer_project_id=customer_project.id,
                site_id=po.site_id,
                site_code=po.site.site_code if po.site else None, # We can access this because of `joinedload`
                po_no=po.po_no,
                po_line_no=po.po_line_no,
                item_description=po.item_description,
                payment_term=PAYMENT_TERM_MAP.get(po.payment_terms_raw, "UNKNOWN"),
                unit_price=po.unit_price,
                requested_qty=po.requested_qty,
                internal_control=1,
                line_amount_hw=(po.unit_price or 0) * (po.requested_qty or 0),
                publish_date=po.publish_date,
            )
            db.add(new_merged_po)

    # 6. Mark ALL original unprocessed rows as processed in a single, efficient query
    unprocessed_ids = [po.id for po in unprocessed_pos]
    if unprocessed_ids: # Ensure the list is not empty
        db.query(models.RawPurchaseOrder).filter(models.RawPurchaseOrder.id.in_(unprocessed_ids)).update({"is_processed": True})
    
    # 7. Commit all changes
    db.commit()

    return len(clean_pos_list)

def get_all_po_data(db: Session):
    return db.query(models.MergedPO).all()


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
            payment_term = merged_po_to_update.payment_term
            shipment_no = acceptance_row['shipment_no']

            # Deduce and Update Category
            merged_po_to_update.category = deduce_category(merged_po_to_update.item_description)

            # --- REVISED AC/PAC CALCULATION LOGIC ---

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
        joinedload(models.MergedPO.customer_project).joinedload(models.CustomerProject.internal_project),
        joinedload(models.MergedPO.site)
    )

   
    if internal_project_id:
        # To filter on a related model, you must join it first.
        # The filter is then applied on the joined model's column.
        query = query.join(models.CustomerProject).filter(
            models.CustomerProject.internal_project_id == internal_project_id
        )
    
    if customer_project_id:
        # This filter is on the MergedPO model itself, so it's simpler.
        query = query.filter(models.MergedPO.customer_project_id == customer_project_id)
    
    if site_code:
        # Your previous logic for site_code was referencing MergedPO, which is correct.
        query = query.filter(models.MergedPO.site_code == site_code)

    if start_date:
        query = query.filter(sa.func.date(models.MergedPO.publish_date) >= start_date)

    if end_date:
        query = query.filter(sa.func.date(models.MergedPO.publish_date) <= end_date)
        
    if search:
        search_term = f"%{search}%"
        # Search on MergedPO fields AND related fields
        query = query.join(models.CustomerProject, isouter=True).join(models.InternalProject, isouter=True)
        query = query.filter(
            (models.MergedPO.po_id.ilike(search_term)) |
            (models.MergedPO.item_description.ilike(search_term)) |
            (models.InternalProject.name.ilike(search_term)) | # Search by Internal Project name
            (models.CustomerProject.name.ilike(search_term))   # Search by Customer Project name
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
    # This query groups by the InternalProject.
    results = db.query(
        models.InternalProject.id.label("project_id"),
        models.InternalProject.name.label("project_name"),
        func.sum(models.MergedPO.line_amount_hw).label("total_po_value"),
        (func.sum(models.MergedPO.accepted_ac_amount) + func.sum(models.MergedPO.accepted_pac_amount)).label("total_accepted")
    ).select_from(models.MergedPO).join(
        models.CustomerProject
    ).join(
        models.InternalProject
    ).group_by(models.InternalProject.id, models.InternalProject.name).all()

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
    """
    Constructs a specific query for the Excel export, joining tables to get all
    necessary names and selecting columns in the correct order.
    """
    # Create aliases for our joined tables to make the query clearer
    CustProj = aliased(models.CustomerProject)
    IntProj = aliased(models.InternalProject)
    
    # 1. Define the exact columns we want to SELECT in the final export
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
        models.MergedPO.line_amount_hw.label("Line Amount"),
        models.MergedPO.accepted_ac_amount.label("Accepted AC Amount"),
        models.MergedPO.date_ac_ok.label("Date AC OK"),
        models.MergedPO.accepted_pac_amount.label("Accepted PAC Amount"),
        models.MergedPO.date_pac_ok.label("Date PAC OK")
    ).select_from(models.MergedPO).join(
        CustProj, models.MergedPO.customer_project_id == CustProj.id
    ).join(
        IntProj, CustProj.internal_project_id == IntProj.id
    )

    # 2. Apply the same filters as before
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

    # 3. Read the precisely constructed query into a DataFrame
    df = pd.read_sql(query.statement, db.bind)
    
    return df