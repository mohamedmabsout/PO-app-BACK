# backend/app/routers/acceptances.py

from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, status
from sqlalchemy.orm import Session
import pandas as pd
import io

from ..dependencies import get_db
from .. import crud
from ..auth import get_current_user # Import your authentication dependency
from .. import models # To specify the user model type

router = APIRouter(
    prefix="/api/acceptances",
    tags=["Acceptances"],
    # This ensures all routes in this file require an authenticated user
    dependencies=[Depends(get_current_user)] 
)

@router.post("/upload", status_code=status.HTTP_200_OK)
def upload_and_process_acceptances(
    file: UploadFile = File(..., description="The Acceptance Excel file (.xlsx or .xls) to be processed."),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user) # Get the user object if needed for logging
):
    """
    Uploads an Excel file containing acceptance data.

    The system will perform the following steps:
    1. Pre-process and aggregate the data from the file.
    2. Deduce the 'category' for each corresponding PO.
    3. Calculate AC (Acceptance Certificate) and PAC (Provisional Acceptance Certificate) amounts and dates.
    4. Update the existing records in the Merged PO table.
    """
    # Check if the uploaded file is an Excel file
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail="Invalid file type. Please upload an Excel file (.xlsx or .xls)."
        )
    history_record = crud.create_upload_history_record(
        db=db,
        filename=file.filename,
        status="PROCESSING",
        user_id=current_user.id,
total_rows=0
    )
    try:
        # Read the Excel file directly into a Pandas DataFrame
        contents = file.file.read()
        acceptance_df = pd.read_excel(io.BytesIO(contents))
        column_mapping = {
            'ShipmentNO.': 'shipment_no', 'AcceptanceQty': 'acceptance_qty', 'ApplicationProcessed': 'application_processed_date',
            'PONo.': 'po_no', 'POLineNo.': 'po_line_no', 
        }
        acceptance_df.rename(columns=column_mapping, inplace=True)
        # Basic validation and type conversion
        acceptance_df['application_processed_date'] = pd.to_datetime(acceptance_df['application_processed_date'])
        numeric_cols = [
            'acceptance_qty', 'po_line_no', 'shipment_no',
        ]
        for col in numeric_cols:
            acceptance_df[col] = pd.to_numeric(acceptance_df[col], errors='coerce').fillna(0)


        raw_count = crud.create_raw_acceptances_from_dataframe(db, acceptance_df, current_user.id)
        # Call the core logic function in crud.py to do all the work
        updated_count = crud.process_acceptance_dataframe(db=db, acceptance_df=acceptance_df)
        history_record.status = "SUCCESS"
        history_record.total_rows = raw_count # Store how many rows were in the file
        db.commit()
        
        # Return a detailed success message
        return {
            "message": "Acceptance file processed successfully.",
            "filename": file.filename,
            "total_records_updated": updated_count
        }

    except Exception as e:
        # Log the actual, detailed error on the server for debugging
        # In a real production app, you'd use a proper logger here
        db.rollback() # Rollback any partial changes
        history_record.status = "FAILED"
        history_record.error_message = str(e)
        db.commit()
        
        print(f"An error occurred during acceptance processing for user {current_user.last_name}: {e}")
        
        # Return a user-friendly error message
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while processing the file. Please check the file format and content."
        )