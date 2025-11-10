import pandas as pd
import io
import logging
from typing import List

from fastapi import FastAPI, Depends, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from .routers import data_processing
from .routers import projects, users, auth
from .database import engine ,SessionLocal
from  .dependencies import get_db
from . import crud, models, schemas


models.Base.metadata.create_all(bind=engine)

app=FastAPI()

origins = ["http://localhost:5173", "http://127.0.0.1:5173"]
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.include_router(projects.router)
app.include_router(users.router)
app.include_router(auth.router) 
app.include_router(data_processing.router)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.post("/upload-and-process/")
async def upload_and_process(file: UploadFile = File(...), db:Session = Depends(get_db)):
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Invalid file type.")

    try:
        contents = await file.read()

        dtype_spec = {'Item Code': str, 'PO NO.': str}
        df = pd.read_excel(io.BytesIO(contents), dtype=dtype_spec)

        column_mapping = {
            'Due Qty': 'due_qty', 'PO Status': 'po_status', 'Unit Price': 'unit_price',
            'Line Amount': 'line_amount', 'Billed Quantity': 'billed_quantity',
            'PO NO.': 'po_no', 'PO Line NO.': 'po_line_no', 'Item Code': 'item_code',
            'Requested Qty': 'requested_qty', 'Publish Date': 'publish_date',
            'Project Code': 'project_code'
        }
        df.rename(columns=column_mapping, inplace=True)

        columns_to_keep = list(column_mapping.values())
        if not all(col in df.columns for col in columns_to_keep):
            raise ValueError("Uploaded file is missing required columns.")
        df = df[columns_to_keep]

        df['publish_date'] = pd.to_datetime(df['publish_date'])

        numeric_cols = ['due_qty', 'unit_price', 'line_amount', 'billed_quantity', 'po_line_no', 'requested_qty']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        num_records = crud.create_po_data_from_dataframe(db, df=df)

        return {"filename": file.filename, "message": f"{num_records} records processed and saved successfully!"}

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/get-data/", response_model=List[schemas.MergedPO])
def get_data(db: Session = Depends(get_db)):
    data = crud.get_all_po_data(db=db)
    return data
