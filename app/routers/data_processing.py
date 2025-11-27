# in app/routers/data_processing.py
from typing import List
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import logging
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from sqlalchemy.orm import Session, query
from typing import Optional
from fastapi import Query, status
from ..dependencies import get_db
from .. import crud, models, auth, schemas
from datetime import datetime, date
from xlsxwriter.utility import xl_col_to_name

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


@router.get(
    "/merged-pos", response_model=schemas.PaginatedMergedPO
)  # Use a paginated schema
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
    per_page: int = Query(20, gt=0),
):
    """
    Retrieves a paginated and filtered list of records from the MergedPO table.
    """
    # 1. Get the base filtered query from our new CRUD function
    query = crud.get_filtered_merged_pos(
        db,
        internal_project_id=internal_project_id,
        customer_project_id=customer_project_id,
        site_code=site_code,
        start_date=start_date,
        end_date=end_date,
        search=search,
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
        "total_pages": (total_items + per_page - 1) // per_page,
    }

@router.get("/export-merged-pos", status_code=status.HTTP_200_OK)
def export_merged_pos_report(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
    internal_project_id: Optional[int] = Query(None),
    customer_project_id: Optional[int] = Query(None),
    site_code: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None, description="Format: YYYY-MM-DD"),
    end_date: Optional[date] = Query(None, description="Format: YYYY-MM-DD"),
    search: Optional[str] = Query(None),
):
    """
    Generates an Excel report with colored headers and robust data highlighting.
    """
    try:
        # 1. Fetch Data
        export_df = crud.get_export_dataframe(
            db,
            internal_project_id=internal_project_id,
            customer_project_id=customer_project_id,
            site_code=site_code,
            start_date=start_date,
            end_date=end_date,
            search=search,
        )

        if export_df.empty:
            raise HTTPException(
                status_code=404, detail="No data found for the selected filters."
            )

        # 2. Format Dates
        for col in export_df.select_dtypes(include=["datetime64"]).columns:
            export_df[col] = (
                pd.to_datetime(export_df[col]).dt.strftime("%d/%m/%Y").fillna("")
            )

        # 3. Setup Excel Writer
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            # Write data starting from Row 1 (leaving Row 0 for our custom headers)
            export_df.to_excel(writer, sheet_name="Merged PO Data", startrow=1, header=False, index=False)

            workbook = writer.book
            worksheet = writer.sheets["Merged PO Data"]
            (max_row, max_col) = export_df.shape

            # --- DEFINING FORMATS ---

            # 1. Header Formats
            header_base = {
                'bold': True,
                'text_wrap': True,
                'valign': 'vcenter',
                'align': 'center',
                'border': 1
            }
            
            fmt_header_std = workbook.add_format({**header_base, 'fg_color': '#EEEEEE'}) # Light Gray
            fmt_header_ac = workbook.add_format({**header_base, 'fg_color': '#93c47d'})  # Darker Green
            fmt_header_pac = workbook.add_format({**header_base, 'fg_color': '#6d9eeb'}) # Darker Blue

            # 2. Data Highlight Formats
            fmt_data_ac = workbook.add_format({"bg_color": "#D9EAD3"})  # Light Green
            fmt_data_pac = workbook.add_format({"bg_color": "#CFE2F3"})  # Light Blue

            # --- WRITING HEADERS MANUALLY ---
            
            headers = export_df.columns.tolist()
            
            for col_idx, column_name in enumerate(headers):
                # Determine color based on column name keywords
                if "AC" in column_name and "PAC" not in column_name:
                    style = fmt_header_ac
                elif "PAC" in column_name:
                    style = fmt_header_pac
                else:
                    style = fmt_header_std
                
                # Write the header
                worksheet.write(0, col_idx, column_name, style)
                
                # Optional: Set column width for better readability
                worksheet.set_column(col_idx, col_idx, 20) 

            # --- CONDITIONAL FORMATTING (DATA) ---
            
            # We want to format the cells if they are NOT empty (including 0.0)
            # The range starts at A2
            
            # Find column letters for AC and PAC fields
            try:
                # AC Columns (Amount and Date)
                ac_amt_idx = headers.index("Accepted AC Amount")
                ac_date_idx = headers.index("Date AC OK")
                
                # PAC Columns (Amount and Date)
                pac_amt_idx = headers.index("Accepted PAC Amount")
                pac_date_idx = headers.index("Date PAC OK")

                # Apply formatting for AC Columns
                for idx in [ac_amt_idx, ac_date_idx]:
                    col_letter = xl_col_to_name(idx)
                    range_str = f"{col_letter}2:{col_letter}{max_row + 1}"
                    
                    # Rule: Cell Value != "" (This highlights values, including 0, but ignores empty)
                    worksheet.conditional_format(range_str, {
                        'type': 'cell',
                        'criteria': '!=',
                        'value': '""', 
                        'format': fmt_data_ac
                    })

                # Apply formatting for PAC Columns
                for idx in [pac_amt_idx, pac_date_idx]:
                    col_letter = xl_col_to_name(idx)
                    range_str = f"{col_letter}2:{col_letter}{max_row + 1}"
                    
                    worksheet.conditional_format(range_str, {
                        'type': 'cell',
                        'criteria': '!=',
                        'value': '""',
                        'format': fmt_data_pac
                    })

            except ValueError:
                print("Warning: Could not find specific AC/PAC columns for formatting.")

        output.seek(0)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        filename = f"Merged_PO_Report_{timestamp}.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        print(f"Error during export: {e}")
        raise HTTPException(
            status_code=500, detail="Could not generate the Excel report."
        )
