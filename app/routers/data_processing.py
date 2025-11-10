# in app/routers/data_processing.py
from typing import List
import pandas as pd
import io
import logging
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from sqlalchemy.orm import Session

from ..dependencies import get_db
from .. import crud, models, auth, schemas

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
            'PO NO.': 'po_no', 'PO Line NO.': 'po_line_no', 'Item Code': 'item_code',
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
        num_records = crud.create_purchase_orders_from_dataframe(db, df=df)
        crud.create_upload_history_record(
            db=db, 
            filename=file.filename, 
            status="SUCCESS", 
            user_id=current_user.id,
            total_rows=num_records
        )
        return {"filename": file.filename, "message": f"{num_records} raw PO records saved successfully!"}
    
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

@router.post("/process/merge-pos")
def trigger_po_merge(db: Session = Depends(get_db),current_user: models.User = Depends(auth.get_current_user)):
    try:
        num_processed = crud.process_and_merge_pos(db)
        if num_processed == 0:
            return {"message": "No new POs to process."}
        return {"message": f"Successfully processed and merged {num_processed} POs."}
    except Exception as e:
        logger.error(f"Error during PO merge processing: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

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
