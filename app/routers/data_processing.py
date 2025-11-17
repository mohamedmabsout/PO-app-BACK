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
        num_raw_records = crud.save_and_hydrate_raw_pos(
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
    project_name: Optional[str] = Query(None),
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
        db, project_name=project_name, site_code=site_code, 
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
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            merged_df.to_excel(writer, sheet_name="Merged PO Data", index=False)

        output.seek(0)

        # 3. Set headers for the file download
        filename = "Merged_PO_Report.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}

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
