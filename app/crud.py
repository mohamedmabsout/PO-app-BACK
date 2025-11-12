from datetime import datetime
from sqlalchemy.orm import Session
from typing import List, Optional
from . import auth
from . import models, schemas
import pandas as pd

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


# Function to create a new project
def create_project(db: Session, project: schemas.ProjectCreate):
    project_data = project.model_dump()

    # Step 2: Intercept and convert the date strings
    # We define the expected format from the frontend/Excel ('DD/MM/YYYY')
    date_format = "%d/%m/%Y" # Use %Y for 4-digit year, %m for month, %d for day

    if project_data.get("start_date"):
        # strptime means "string parse time" - it converts a string to a datetime object
        project_data["start_date"] = datetime.strptime(project_data["start_date"], date_format).date()

    if project_data.get("plan_end_date"):
        project_data["plan_end_date"] = datetime.strptime(project_data["plan_end_date"], date_format).date()
        
    # Step 3: Create the SQLAlchemy model instance using the MODIFIED dictionary
    db_project = models.Project(**project_data)
    db.add(db_project)
    db.commit()
    db.refresh(db_project)
    return db_project

def delete_project(db: Session, project_id: int):
    db_project = db.query(models.Project).filter(models.Project.id == project_id).first()
    if db_project:
        db.delete(db_project)
        db.commit()
    return db_project


def create_purchase_orders_from_dataframe(db: Session, df: pd.DataFrame):
    # We can optionally clear the table if we want to re-import from scratch
    # db.query(models.PurchaseOrder).delete()

    records = df.to_dict(orient="records")
    db.bulk_insert_mappings(models.PurchaseOrder, records)
    db.commit()
    return len(records)  # Return the number of records created


def process_and_merge_pos(db: Session):
    # 1. Find all purchase orders that haven't been processed yet
    unprocessed_pos = (
        db.query(models.PurchaseOrder)
        .filter(models.PurchaseOrder.is_processed == False)
        .all()
    )

    if not unprocessed_pos:
        return 0  # Nothing to process
    po_ids_to_check = [f"{po.po_no}-{po.po_line_no}" for po in unprocessed_pos]

    # 3. In ONE efficient query, fetch all existing MergedPO records that match our batch
    existing_merged_pos_query = (
        db.query(models.MergedPO)
        .filter(models.MergedPO.po_id.in_(po_ids_to_check))
        .all()
    )

    # 4. Create a dictionary for fast lookups (O(1) complexity). This is the key to performance.
    #    The key is the po_id, the value is the SQLAlchemy MergedPO object.
    existing_pos_map = {mp.po_id: mp for mp in existing_merged_pos_query}
    for po in unprocessed_pos:
        po_id = f"{po.po_no}-{po.po_line_no}"
        # Check if this PO already exists in our merged table
        if po_id in existing_pos_map:
            if po.requested_qty == 0:
                existing_po_to_update = existing_pos_map[po_id]

                existing_po_to_update.requested_qty = 0
                # BONUS: We should also update the calculated amount
                existing_po_to_update.line_amount_hw = 0

        # If po.requested_qty is not 0, we do nothing, as per the rule.

        else:
            # --- INSERT PATH ---
            # This is the only place we should create a new record.

            # Calculate values for the new record
            line_amount_hw = po.unit_price * po.requested_qty
            payment_term_abbreviation = PAYMENT_TERM_MAP.get(
                po.payment_terms_raw, "UNKNOWN"
            )

            # I noticed you added 'site_code' to the mapping.
            # Make sure 'site_code' exists as a column on your PurchaseOrder model
            # and that it's being read from the Excel file correctly.
            # If not, the import will fail silently. Let's use a safe default for now.
            site_code = getattr(po, "site_code", "N/A")

            # Assemble the data for the new MergedPO
            new_merged_data = {
                "po_id": po_id,
                "project_name": po.project_code,
                "site_code": site_code,
                "po_no": po.po_no,
                "po_line_no": po.po_line_no,
                "item_description": po.item_code,
                "payment_term": payment_term_abbreviation,
                "unit_price": po.unit_price,
                "requested_qty": po.requested_qty,
                "internal_control": 1,
                "line_amount_hw": line_amount_hw,
                "publish_date": po.publish_date,
            }
            # Create and add the new object TO THE SESSION
            new_merged_po = models.MergedPO(**new_merged_data)
            db.add(new_merged_po)
        # Mark the raw PO as processed, this happens on both paths
        po.is_processed = True
    # 6. Commit the session. This saves all changes (updates, inserts, and is_processed flags)
    #    in a single, atomic database transaction.
    db.commit()

    return len(unprocessed_pos)


def get_all_po_data(db: Session):
    return db.query(models.MergedPO).all()


def create_po_data_from_dataframe(db: Session, df: pd.DataFrame):
    db.query(models.MergedPO).delete()
    db.close()

    records = df.to_dict(orient="records")
    db.bulk_insert_mappings(models.MergedPO, records)
    db.commit()
# in backend/app/crud.py

def create_upload_history_record(
    db: Session, 
    filename: str, 
    status: str, 
    user_id: int, 
    total_rows: int = 0, 
    error_msg: str = None
):
    history_record = models.UploadHistory(
        original_filename=filename,
        status=status,
        user_id=user_id,
        total_rows=total_rows,
        error_message=error_msg
    )
    db.add(history_record)
    db.commit()
    return history_record

def get_upload_history(db: Session, skip: int = 0, limit: int = 100):
    # Use order_by to get the most recent uploads first
    return db.query(models.UploadHistory).order_by(models.UploadHistory.uploaded_at.desc()).offset(skip).limit(limit).all()

def get_raw_po_data_as_dataframe(
    db: Session,
    status: Optional[str] = None,
    project_name: Optional[str] = None,
    search: Optional[str] = None
    # We are not including 'category' as it's not in the table
) -> pd.DataFrame:
    """
    Queries the raw PurchaseOrder table with optional filters and returns a DataFrame.
    """
    # Start with a base query on the PurchaseOrder table
    query = db.query(models.PurchaseOrder)

    # Apply filters to the query if they are provided
    if status:
        # The 'po_status' column in the table matches this filter
        query = query.filter(models.PurchaseOrder.po_status == status)

    if project_name:
        # The 'project_code' column in the table matches this filter
        query = query.filter(models.PurchaseOrder.project_code == project_name)

    if search:
        # Create a search filter for PO number
        search_term = f"%{search}%"
        query = query.filter(models.PurchaseOrder.po_no.ilike(search_term))

    # Execute the query and read the results directly into a Pandas DataFrame
    df = pd.read_sql(query.statement, db.bind)

    # Return the DataFrame, which includes all columns from the table
    return df
