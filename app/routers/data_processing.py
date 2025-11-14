# in app/routers/data_processing.py
from typing import List
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import logging
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from sqlalchemy.orm import Session
from typing import Optional
from fastapi import Query,status
from ..dependencies import get_db
from .. import crud, models, auth, schemas
from datetime import datetime

router = APIRouter(
    prefix="/api/data",
    tags=["data_processing"]
)
logger = logging.getLogger(__name__)

@router.post("/import/purchase-orders")
async def import_purchase_orders(file: UploadFile = File(...), db: Session = Depends(get_db), current_user: models.User = Depends(auth.get_current_user) # PROTECT
):
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Invalid file type.")
    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))

        # Column mapping to match the database model
        column_mapping = {
            'Due Qty': 'due_qty', 'PO Status': 'po_status', 'Unit Price': 'unit_price',
            'Line Amount': 'line_amount', 'Billed Quantity': 'billed_quantity',
            'PO NO.': 'po_no', 'PO Line NO.': 'po_line_no', 'Item Code': 'item_code', 'Item Description': 'item_description',
            'Requested Qty': 'requested_qty', 'Publish Date': 'publish_date',
            'Project Code': 'project_code','Payment Terms': 'payment_terms_raw' ,'Site Code':'site_code'

        }
        df.rename(columns=column_mapping, inplace=True)
        
        # Basic validation and type conversion
        df['publish_date'] = pd.to_datetime(df['publish_date'])
        numeric_cols = [
            'due_qty', 'unit_price', 'line_amount', 
            'billed_quantity', 'po_line_no', 'requested_qty'
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Call the new CRUD function to save to the raw table
        num_records = crud.create_purchase_orders_from_dataframe(db, df=df, user_id=current_user.id)

        crud.create_upload_history_record(
            db=db, 
            filename=file.filename, 
            status="SUCCESS", 
            user_id=current_user.id,
            total_rows=num_records
        )
           
        # 2. Immediately trigger the processing
        processed_count = crud.process_and_merge_pos(db=db)
        
        return {
            "filename": file.filename,
            "message": f"PO file uploaded and processed successfully, {num_records} raw PO records saved successfully!",
            "records_processed": processed_count
        }
    
    except Exception as e:
        logger.error(f"Error during PO import: {e}", exc_info=True)
        logger.error(f"Error during PO import: {e}", exc_info=True)
        crud.create_upload_history_record(
            db=db, 
            filename=file.filename, 
            status="FAILURE", 
            user_id=current_user.id,
            error_msg=str(e)
        )
        raise HTTPException(status_code=500, detail=str(e))

# @router.post("/process/merge-pos")
# def trigger_po_merge(db: Session = Depends(get_db),current_user: models.User = Depends(auth.get_current_user)):
#     try:
#         num_processed = crud.process_and_merge_pos(db)
#         if num_processed == 0:
#             return {"message": "No new POs to process."}
#         return {"message": f"Successfully processed and merged {num_processed} POs."}
#     except Exception as e:
#         logger.error(f"Error during PO merge processing: {e}", exc_info=True)
#         raise HTTPException(status_code=500, detail=str(e))

@router.get("/import/history", response_model=List[schemas.UploadHistory])
def read_upload_history(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    return crud.get_upload_history(db=db)

@router.get("/staging/purchase-orders", response_model=List[schemas.PurchaseOrder])
def get_staging_purchase_orders(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Retrieves all records from the purchase_orders staging table.
    """
    # We can add pagination later if needed (skip, limit)
    return db.query(models.PurchaseOrder).order_by(models.PurchaseOrder.id.desc()).all()
@router.get("/merged-pos", response_model=List[schemas.MergedPO])
def get_merged_pos(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Retrieves all records from the final merged_pos table.
    """
    return db.query(models.MergedPO).order_by(models.MergedPO.id.desc()).all()

@router.get("/export-merged-pos", status_code=status.HTTP_200_OK)
def export_merged_pos_report(
    db: Session = Depends(get_db),
    status: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    project_name: Optional[str] = Query(None),
    search: Optional[str] = Query(None)
):
    """
    Generates and streams an Excel report of the Merged PO data based on filters.
    """
    try:
        # 1. Call our new CRUD function to get the data
        merged_df = crud.get_merged_po_data_as_dataframe(
            db=db, 
            status=status, 
            category=category, 
            project_name=project_name, 
            search=search
        )
        
        if merged_df.empty:
            raise HTTPException(status_code=404, detail="No data found for the selected filters.")

        # 2. Create the Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            merged_df.to_excel(writer, sheet_name='Merged PO Data', index=False)
        
        output.seek(0)

        # 3. Set headers for the file download
        filename = "Merged_PO_Report.xlsx"
        headers = {'Content-Disposition': f'attachment; filename="{filename}"'}

        # 4. Return the file as a streaming response
        return StreamingResponse(
            output, 
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 
            headers=headers
        )

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        print(f"Error during export: {e}") # Log the error for debugging
        raise HTTPException(status_code=500, detail="Could not generate the Excel report.")
