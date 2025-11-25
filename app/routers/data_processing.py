# in app/routers/data_processing.py
from typing import List
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import logging
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from sqlalchemy.orm import Session,query
from typing import Optional
from fastapi import Query, status
from ..dependencies import get_db
from .. import crud, models, auth, schemas
from datetime import datetime,date

router = APIRouter(prefix="/api/data", tags=["data_processing"])
logger = logging.getLogger(__name__)


@router.post("/import/purchase-orders")
async def import_purchase_orders(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file type.")

    history_record = crud.create_upload_history_record(
        db=db, filename=file.filename, status="PROCESSING", user_id=current_user.id
    )

    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))

        # --- FIX 1: Delegate ALL logic to a new, smart CRUD function ---
        num_raw_records = crud.create_raw_purchase_orders_from_dataframe(
            db=db, df=df, user_id=current_user.id
        )

        # --- FIX 2: Immediately trigger the merge process ---
        num_merged_records = crud.process_and_merge_pos(db=db)

        # Update history on success
        history_record.status = "SUCCESS"
        history_record.total_rows = num_raw_records
        db.commit()

        return {
            "message": f"{num_raw_records} POs saved and {num_merged_records} records merged.",
            "raw_records_saved": num_raw_records,
            "merged_records_processed": num_merged_records,
        }

    except Exception as e:
        db.rollback()
        history_record.status = "FAILURE"
        history_record.error_message = str(e)
        db.commit()
        logger.error(f"Error during PO import: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/import/history", response_model=List[schemas.UploadHistory])
def read_upload_history(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    return crud.get_upload_history(db=db)


@router.get("/staging/purchase-orders", response_model=List[schemas.RawPurchaseOrder])
def get_staging_purchase_orders(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    """
    Retrieves all records from the purchase_orders staging table.
    """
    # We can add pagination later if needed (skip, limit)
    return (
        db.query(models.RawPurchaseOrder)
        .order_by(models.RawPurchaseOrder.id.desc())
        .all()
    )


@router.get("/merged-pos", response_model=schemas.PaginatedMergedPO) # Use a paginated schema
def get_merged_pos_preview(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
    # Add filter parameters as Query dependencies
    internal_project_id: Optional[int] = Query(None),
    customer_project_id: Optional[int] = Query(None),
    site_code: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None, description="Format: YYYY-MM-DD"),
    end_date: Optional[date] = Query(None, description="Format: YYYY-MM-DD"),
    search: Optional[str] = Query(None),
    # Add pagination parameters
    page: int = Query(1, gt=0),
    per_page: int = Query(20, gt=0)
):
    """
    Retrieves a paginated and filtered list of records from the MergedPO table.
    """
    # 1. Get the base filtered query from our new CRUD function
    query = crud.get_filtered_merged_pos(
        db, internal_project_id=internal_project_id, customer_project_id=customer_project_id, site_code=site_code, 
        start_date=start_date, end_date=end_date, search=search
    )

    # 2. Get the total count of items that match the filters (for pagination)
    total_items = query.count()
    
    # 3. Apply pagination to the query
    items = query.offset((page - 1) * per_page).limit(per_page).all()
    
    # 4. Return the data in a structured pagination format
    return {
        "items": items,
        "total_items": total_items,
        "page": page,
        "per_page": per_page,
        "total_pages": (total_items + per_page - 1) // per_page
    }

@router.get("/export-merged-pos", status_code=status.HTTP_200_OK)
def export_merged_pos_report(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
    # Use the same filter parameters
    project_name: Optional[str] = Query(None),
    site_code: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None, description="Format: YYYY-MM-DD"),
    end_date: Optional[date] = Query(None, description="Format: YYYY-MM-DD"),
    search: Optional[str] = Query(None)
):
    """
    Generates an Excel report of Merged PO data using the same filters as the preview.
    """
    try:
        # 1. Get the base filtered query from the SAME CRUD function
        query = crud.get_filtered_merged_pos(
            db, project_name=project_name, site_code=site_code, 
            start_date=start_date, end_date=end_date, search=search
        )
        
        # 2. Read the entire query result into a DataFrame (no pagination for export)
        merged_df = pd.read_sql(query.statement, db.bind)
        
        if merged_df.empty:
            raise HTTPException(status_code=404, detail="No data found for the selected filters.")        # 2. Create the Excel file in memory
          # Identify all columns that are of a date or datetime type
        for col in merged_df.select_dtypes(include=['datetime64[ns]', 'datetime']).columns:
            # Format them to 'DD/MM/YYYY'. The '.dt' accessor is for datetime operations.
            merged_df[col] = merged_df[col].dt.strftime('%d/%m/%Y')

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter', datetime_format='dd/mm/yyyy') as writer:
            merged_df.to_excel(writer, sheet_name='Merged PO Data', index=False)
            
            # Get the workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets['Merged PO Data']

            # Create format objects for the colors
            ac_format = workbook.add_format({'bg_color': '#D9EAD3'}) # Light green
            pac_format = workbook.add_format({'bg_color': '#CFE2F3'}) # Light blue
            
            # Find the column index for the AC/PAC columns
            # header=True by default, so row 1 is the first data row.
            header = list(merged_df.columns)
            try:
                ac_amount_col = header.index('accepted_ac_amount')
                ac_date_col = header.index('date_ac_ok')
                pac_amount_col = header.index('accepted_pac_amount')
                pac_date_col = header.index('date_pac_ok')

                # Apply the format conditionally to each row
                for row_num, row_data in merged_df.iterrows():
                    # xlsxwriter is 0-indexed for cols, 1-indexed for data rows
                    if pd.notna(row_data['accepted_ac_amount']):
                        worksheet.write(row_num + 1, ac_amount_col, row_data['accepted_ac_amount'], ac_format)
                        worksheet.write(row_num + 1, ac_date_col, row_data['date_ac_ok'], ac_format)
                    
                    if pd.notna(row_data['accepted_pac_amount']):
                        worksheet.write(row_num + 1, pac_amount_col, row_data['accepted_pac_amount'], pac_format)
                        worksheet.write(row_num + 1, pac_date_col, row_data['date_pac_ok'], pac_format)
            except ValueError:
                # Handle case where a column might be missing
                print("A required column for formatting was not found in the export DataFrame.")

        output.seek(0)

       # 1. Get the current timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M") # e.g., "2025-11-23_18-45"
        
        # 2. Construct the dynamic filename
        filename = f"Merged_PO_Report_{timestamp}.xlsx"
        
        # 3. Set up headers for the file download with the new filename
        headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
        # 4. Return the file as a streaming response
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        print(f"Error during export: {e}")  # Log the error for debugging
        raise HTTPException(
            status_code=500, detail="Could not generate the Excel report."
        )
