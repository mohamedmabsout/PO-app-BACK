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

    # MODIFICATION 1: We will build this list inside the loop for clarity
    # po_ids_to_check = [f"{po.po_no}-{po.po_line_no}" for po in unprocessed_pos]

    # 3. In ONE efficient query, fetch all existing MergedPO records that match our batch
    # This part is excellent and remains the same.
    po_ids_to_check = [f"{po.po_no}-{po.po_line_no}" for po in unprocessed_pos]
    existing_merged_pos_query = (
        db.query(models.MergedPO)
        .filter(models.MergedPO.po_id.in_(po_ids_to_check))
        .all()
    )

    # 4. Create a dictionary for fast lookups (O(1) complexity).
    # This is also excellent and remains the same.
    existing_pos_map = {mp.po_id: mp for mp in existing_merged_pos_query}

    # 5. Loop through the raw POs
    for po in unprocessed_pos:
        po_id = f"{po.po_no}-{po.po_line_no}"

        # --- MODIFICATION 2: SIMPLIFY THE UPDATE/INSERT LOGIC ---

        # --- UPDATE PATH ---
        # Check if this PO already exists in our merged table
        if po_id in existing_pos_map:
            merged_po_to_update = existing_pos_map[po_id]

            # ALWAYS update the key fields from the raw PO file.
            # This handles cases where a PO is re-uploaded with changes.
            merged_po_to_update.requested_qty = po.requested_qty
            merged_po_to_update.unit_price = (
                po.unit_price
            )  # It's good to update the price too
            merged_po_to_update.publish_date = po.publish_date

            # Recalculate the line amount based on the potentially new values
            merged_po_to_update.line_amount_hw = po.unit_price * po.requested_qty

            # You could also update other fields here if they can change, e.g., payment_term
            merged_po_to_update.payment_term = PAYMENT_TERM_MAP.get(
                po.payment_terms_raw, "UNKNOWN"
            )
        else:
            # This part of your logic is mostly correct and can stay.
            line_amount_hw = po.unit_price * po.requested_qty
            payment_term_abbreviation = PAYMENT_TERM_MAP.get(
                po.payment_terms_raw, "UNKNOWN"
            )

            # MODIFICATION 3: Use the correct column name for item description
            # Your old code had 'item_code_description', let's assume the model has 'item_code'
            # based on your database screenshot.
            item_desc_value = getattr(po, "item_description", None)

            new_merged_data = {
                "po_id": po_id,
                "project_name": po.project_code,
                "site_code": po.site_code,
                "po_no": po.po_no,
                "po_line_no": po.po_line_no,
                "item_description": item_desc_value,  # Use the corrected variable
                "payment_term": payment_term_abbreviation,
                "unit_price": po.unit_price,
                "requested_qty": po.requested_qty,
                "internal_control": 1,
                "line_amount_hw": line_amount_hw,
                "publish_date": po.publish_date,
            }
            new_merged_po = models.MergedPO(**new_merged_data)
            db.add(new_merged_po)

            # --- MODIFICATION 4: This is CRITICAL and stays ---
            # Mark the raw PO as processed, regardless of whether it was an INSERT or UPDATE.
        po.is_processed = True

    # 6. Commit the session. This saves all changes at once.
    # This is also correct and stays.
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

    po_ids_to_update = aggregated_df["po_id"].unique().tolist()
    if not po_ids_to_update:
        return 0  # Nothing to process

    merged_po_records = (
        db.query(models.MergedPO)
        .filter(models.MergedPO.po_id.in_(po_ids_to_update))
        .all()
    )
    merged_po_map = {mp.po_id: mp for mp in merged_po_records}
    print("\n--- DEBUG: Database Match ---")
    print(f"Found {len(merged_po_map)} matching records in the MergedPO table.")
    if merged_po_map:
        print("First 5 po_ids found in database:", list(merged_po_map.keys())[:5])
    updated_records = []

    for index, acceptance_row in aggregated_df.iterrows():
        po_id = acceptance_row["po_id"]

        if po_id in merged_po_map:
            merged_po_to_update = merged_po_map[po_id]
            updated_records.append(po_id)

            unit_price = merged_po_to_update.unit_price or 0
            req_qty = merged_po_to_update.requested_qty or 0
            agg_acceptance_qty = acceptance_row["acceptance_qty"]
            # Get the latest date from the correct column and extract only the date part
            latest_processed_date = acceptance_row["application_processed_date"].date()

            # --- 1. Deduce and Update Category ---
            merged_po_to_update.category = deduce_category(
                merged_po_to_update.item_description
            )

            # --- 2. AC Calculation ---
            if acceptance_row["shipment_no"] == 1:
                merged_po_to_update.total_ac_amount = unit_price * req_qty * 0.80
                merged_po_to_update.accepted_ac_amount = (
                    unit_price * agg_acceptance_qty * 0.80
                )
                merged_po_to_update.date_ac_ok = latest_processed_date

            # --- 3. PAC Calculation ---
            payment_term = merged_po_to_update.payment_term

            if payment_term == "ACPAC 100%":
                # This logic is triggered by shipment 1
                if acceptance_row["shipment_no"] == 1:
                    merged_po_to_update.total_pac_amount = unit_price * req_qty * 0.20
                    merged_po_to_update.accepted_pac_amount = (
                        unit_price * agg_acceptance_qty * 0.20
                    )
                    merged_po_to_update.date_pac_ok = (
                        latest_processed_date  # Same as AC date
                    )

            elif payment_term == "AC1 80 | PAC 20":
                # This logic is triggered by shipment 2
                if acceptance_row["shipment_no"] == 2:
                    merged_po_to_update.total_pac_amount = unit_price * req_qty * 0.20
                    merged_po_to_update.accepted_pac_amount = (
                        unit_price * agg_acceptance_qty * 0.20
                    )
                    merged_po_to_update.date_pac_ok = latest_processed_date
        else:
            print(f"  [!] No match found in database for po_id: '{po_id}'")
    db.commit()

    return len(set(updated_records))
