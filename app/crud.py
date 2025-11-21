from datetime import datetime,date
from sqlalchemy.orm import Session
from typing import List, Optional
from . import auth
from . import models, schemas
import pandas as pd
from sqlalchemy.orm import joinedload,Query
import sqlalchemy as sa
from sqlalchemy import func, case
from sqlalchemy.sql.functions import coalesce # More explicit import

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

def save_and_hydrate_raw_pos(db: Session, df: pd.DataFrame, user_id: int):
    # Standardize column names from the Excel file - This part is perfect.
    df.rename(columns={
        'PO NO.': 'po_no', 'PO Line NO.': 'po_line_no', 'Project Name': 'project_code',
        'Site Code': 'site_code', 'Customer': 'customer','PO Status': 'po_status',
        'Item Description': 'item_description', 'Payment Terms': 'payment_terms_raw',
        'Unit Price': 'unit_price', 'Requested Qty': 'requested_qty', 'Publish Date': 'publish_date'
    }, inplace=True, errors='ignore')

    # --- REVISED AND CORRECTED HYDRATION LOGIC ---

    # Helper function is now cleaner
    def hydrate_lookup_table(model, model_key_col: str, df_key_col: str, df_val_col: str = None):
        if df_key_col not in df.columns:
            return {} # If the source column doesn't exist in the Excel file, do nothing.
            
        unique_keys = df[df_key_col].dropna().unique()
        if len(unique_keys) == 0:
            return {}

        existing_items = db.query(model).filter(getattr(model, model_key_col).in_(unique_keys)).all()
        existing_map = {getattr(item, model_key_col): item for item in existing_items}
        
        new_keys = set(unique_keys) - set(existing_map.keys())
        
        if new_keys:
            new_items_to_insert = []
            if df_val_col: # Case for Sites (key-value pairs)
                unique_new_sites = df[df[df_key_col].isin(new_keys)][[df_key_col, df_val_col]].drop_duplicates(subset=[df_key_col])
                for _, row in unique_new_sites.iterrows():
                    new_items_to_insert.append({model_key_col: row[df_key_col], df_val_col: row[df_val_col]})
            else: # Case for Projects/Customers (single key)
                new_items_to_insert = [{model_key_col: key} for key in new_keys]
            
            if new_items_to_insert:
                db.bulk_insert_mappings(model, new_items_to_insert)
                db.commit()
            
            newly_created_items = db.query(model).filter(getattr(model, model_key_col).in_(new_keys)).all()
            for item in newly_created_items:
                existing_map[getattr(item, model_key_col)] = item
        
        return existing_map

    # Run the hydration for each lookup table with corrected arguments
    # hydrate_lookup_table(Model, 'model_column_name', 'dataframe_column_name')
    project_map = hydrate_lookup_table(models.Project, 'name', 'project_code')

    customer_map = hydrate_lookup_table(models.Customer, 'name', 'customer')
    # For sites: hydrate_lookup_table(Model, 'model_key_col', 'df_key_col', 'df_val_col')
    site_map = hydrate_lookup_table(models.Site, 'site_code', 'site_code')

    # Map names/codes to their database IDs - This part is now more robust
    if project_map:
        df['project_id'] = df['project_code'].map({p.name: p.id for p in project_map.values()})
    if customer_map:
        df['customer_id'] = df['customer'].map({c.name: c.id for c in customer_map.values()})
    if site_map:
        df['site_id'] = df['site_code'].map({s.site_code: s.id for s in site_map.values()})
    
    df['uploader_id'] = user_id

    # -----------------------------------------------

    # Select only the columns that exist in the RawPurchaseOrder model
    # This part is correct and remains the same.
    model_columns = [c.key for c in models.RawPurchaseOrder.__table__.columns if c.key != 'id']
    df_to_insert = df[[col for col in model_columns if col in df.columns]]
    
    records = df_to_insert.to_dict("records")
    db.bulk_insert_mappings(models.RawPurchaseOrder, records)
    db.commit()
    return len(records)



def process_and_merge_pos(db: Session):
    # 1. Find all purchase orders that haven't been processed yet
    unprocessed_pos_query = db.query(models.RawPurchaseOrder).filter(models.RawPurchaseOrder.is_processed == False)
 
    # Check if there's anything to do before proceeding
    if unprocessed_pos_query.count() == 0:
        return 0

    # --- NEW: DE-DUPLICATION STEP ---
    # Load all unprocessed records into a Pandas DataFrame
    unprocessed_df = pd.read_sql(unprocessed_pos_query.statement, db.bind)
    
    # Ensure publish_date is in datetime format for correct sorting
    unprocessed_df['publish_date'] = pd.to_datetime(unprocessed_df['publish_date'])
    
    # Sort by publish_date to ensure we keep the most recent record
    unprocessed_df.sort_values('publish_date', inplace=True)
    
    # Drop duplicates based on po_no and po_line_no, keeping the 'last' (most recent) one
    clean_df = unprocessed_df.drop_duplicates(subset=['po_no', 'po_line_no'], keep='last').copy()
    
    # Generate the po_id on the clean DataFrame
    clean_df['po_id'] = clean_df['po_no'] + '-' + clean_df['po_line_no'].astype(int).astype(str)
    # --------------------------------

    po_ids_to_check = clean_df['po_id'].tolist()
    existing_merged_map = {mp.po_id: mp for mp in db.query(models.MergedPO).filter(models.MergedPO.po_id.in_(po_ids_to_check)).all()}


     # We need to fetch the related project and site objects for the clean data
    project_ids = clean_df['project_id'].dropna().unique().tolist()
    site_ids = clean_df['site_id'].dropna().unique().tolist()
    
    project_map = {p.id: p for p in db.query(models.Project).filter(models.Project.id.in_(project_ids)).all()}
    site_map = {s.id: s for s in db.query(models.Site).filter(models.Site.id.in_(site_ids)).all()}

    # Loop through the rows of the CLEAN DataFrame
    for _, po_row in clean_df.iterrows():
        po_id = po_row['po_id']
        
        # Get the related objects from our maps
        project = project_map.get(po_row['project_id'])
        site = site_map.get(po_row['site_id'])

        if po_id in existing_merged_map:
            # UPDATE logic
            merged_po_to_update = existing_merged_map[po_id]
            merged_po_to_update.requested_qty = po_row['requested_qty']
            merged_po_to_update.unit_price = po_row['unit_price']
            merged_po_to_update.publish_date = po_row['publish_date']
            merged_po_to_update.line_amount_hw = (po_row['unit_price'] or 0) * (po_row['requested_qty'] or 0)
        else:
            # INSERT logic
            new_merged_po = models.MergedPO(
                po_id=po_id,
                raw_po_id=po_row['id'], # Use the ID from the raw table row
                project_id=po_row['project_id'],
                site_id=po_row['site_id'],
                project_name=project.name if project else None,
                site_code=site.site_code if site else None,
                po_no=po_row['po_no'],
                po_line_no=po_row['po_line_no'],
                item_description=po_row['item_description'],
                payment_term=PAYMENT_TERM_MAP.get(po_row['payment_terms_raw'], "UNKNOWN"),
                unit_price=po_row['unit_price'],
                requested_qty=po_row['requested_qty'],
                internal_control=1,
                line_amount_hw=(po_row['unit_price'] or 0) * (po_row['requested_qty'] or 0),
                publish_date=po_row['publish_date'],
            )
            db.add(new_merged_po)

    # Mark the original unprocessed rows as processed
    unprocessed_pos_query.update({"is_processed": True})
    
    db.commit()

    return len(clean_df) 


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
            latest_processed_date = acceptance_row['application_processed_date'].date()
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
    project_name: Optional[str] = None,
    site_code: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    search: Optional[str] = None # Keeping the general search filter
) -> Query:
    """
    Builds a SQLAlchemy Query for the MergedPO table with multiple optional filters.
    Returns the Query object, not the results.
    """
    # Start with a base query
    query = db.query(models.MergedPO)

    # Apply filters conditionally
    if project_name:
        query = query.filter(models.MergedPO.project_name == project_name)
    
    if site_code:
        query = query.filter(models.MergedPO.site_code == site_code)

    if start_date:
        # Filter for publish_date >= start_date
        # We use a cast to date to ignore the time part for comparison
        query = query.filter(sa.func.date(models.MergedPO.publish_date) >= start_date)

    if end_date:
        # Filter for publish_date <= end_date
        query = query.filter(sa.func.date(models.MergedPO.publish_date) <= end_date)
        
    if search:
        search_term = f"%{search}%"
        query = query.filter(
            (models.MergedPO.po_no.ilike(search_term)) |
            (models.MergedPO.item_description.ilike(search_term))
        )

    # Return the complete, but not yet executed, query object
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
def get_projects_financial_summary(db: Session):
    # This query groups all MergedPOs by project and calculates the sums for each.
    results = db.query(
        models.MergedPO.project_name,
        func.sum(models.MergedPO.line_amount_hw).label("total_po_value"),
        (func.sum(models.MergedPO.accepted_ac_amount) + func.sum(models.MergedPO.accepted_pac_amount)).label("total_accepted")
    ).group_by(models.MergedPO.project_name).all()

    summary_list = []
    for row in results:
        po_value = row.total_po_value or 0
        accepted = row.total_accepted or 0
        gap = po_value - accepted
        completion = (accepted / po_value * 100) if po_value > 0 else 0
        
        # We need to find the project_id. This is a simplification.
        # A more robust solution would join with the projects table.
        project = db.query(models.Project).filter(models.Project.name == row.project_name).first()

        summary_list.append({
            "project_id": project.id if project else 0,
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
def get_financial_summary_for_year(db: Session, year: int) -> dict:
    """Calculates the financial summary for a specific year based on 'publish_date'."""
    
    # Base query filtering by year
    query = db.query(models.MergedPO).filter(
        func.extract('year', models.MergedPO.publish_date) == year
    )
    
    # Perform aggregations on the filtered query
    total_po_value = query.with_entities(func.sum(models.MergedPO.line_amount_hw)).scalar() or 0.0
    total_accepted_ac = query.with_entities(func.sum(models.MergedPO.accepted_ac_amount)).scalar() or 0.0
    total_accepted_pac = query.with_entities(func.sum(models.MergedPO.accepted_pac_amount)).scalar() or 0.0
    remaining_gap = total_po_value - (total_accepted_ac + total_accepted_pac)
    
    return {
        "total_po_value": total_po_value,
        "total_accepted_ac": total_accepted_ac,
        "total_accepted_pac": total_accepted_pac,
        "remaining_gap": remaining_gap,
        
    }


def get_financial_summary_for_month(db: Session, year: int, month: int) -> dict:
    """Calculates the financial summary for a specific month and year."""
    
    query = db.query(models.MergedPO).filter(
        func.extract('year', models.MergedPO.publish_date) == year,
        func.extract('month', models.MergedPO.publish_date) == month
    )
    
    total_po_value = query.with_entities(func.sum(models.MergedPO.line_amount_hw)).scalar() or 0.0
    total_accepted_ac = query.with_entities(func.sum(models.MergedPO.accepted_ac_amount)).scalar() or 0.0
    total_accepted_pac = query.with_entities(func.sum(models.MergedPO.accepted_pac_amount)).scalar() or 0.0
    
    remaining_gap = total_po_value - (total_accepted_ac + total_accepted_pac)
    
    return {
        "total_po_value": total_po_value,
        "total_accepted_ac": total_accepted_ac,
        "total_accepted_pac": total_accepted_pac,
        "remaining_gap": remaining_gap,
        
    }


def get_financial_summary_for_week(db: Session, year: int, week: int) -> dict:
    """Calculates the financial summary for a specific week and year."""
    
    # 'week' in PostgreSQL/SQLAlchemy returns the week number (1-53)
    query = db.query(models.MergedPO).filter(
        func.extract('year', models.MergedPO.publish_date) == year,
        func.extract('week', models.MergedPO.publish_date) == week
    )
    
    total_po_value = query.with_entities(func.sum(models.MergedPO.line_amount_hw)).scalar() or 0.0
    total_accepted_ac = query.with_entities(func.sum(models.MergedPO.accepted_ac_amount)).scalar() or 0.0
    total_accepted_pac = query.with_entities(func.sum(models.MergedPO.accepted_pac_amount)).scalar() or 0.0
    
    remaining_gap = total_po_value - (total_accepted_ac + total_accepted_pac)
    
    return {
        "total_po_value": total_po_value,
        "total_accepted_ac": total_accepted_ac,
        "total_accepted_pac": total_accepted_pac,
        "remaining_gap": remaining_gap,
        
    }