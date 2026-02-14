import pandas as pd
import io
import logging
from typing import List

from fastapi import FastAPI, Depends, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from app.routers import projects, users, auth, selectors, sites, data_processing, acceptances, summary, targets, export, sbcs, notifications,importation
from .database import engine ,SessionLocal
from  .dependencies import get_db
from . import crud, models, schemas
import os
from fastapi.staticfiles import StaticFiles
from app.routers import expenses, facturation
models.Base.metadata.create_all(bind=engine, checkfirst=True)

app=FastAPI()

origins = ["http://localhost:5173", "http://127.0.0.1:5173"]
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"],expose_headers=["Content-Disposition"])
app.include_router(projects.router)
app.include_router(users.router)
app.include_router(auth.router) 
app.include_router(data_processing.router)
app.include_router(selectors.router )
app.include_router(acceptances.router)
app.include_router(sites.router)
app.include_router(summary.router)
app.include_router(targets.router)
app.include_router(export.router)
app.include_router(sbcs.router)
app.include_router(notifications.router)
app.include_router(expenses.router)
app.include_router(facturation.router)  # <-- NEW: Include CRUD router for testing purposes
app.include_router(importation.router)  # <-- NEW: Include CRUD router for testing purposes
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

os.makedirs("uploads/sbc_docs", exist_ok=True)

app.mount("/static", StaticFiles(directory="uploads"), name="static")

@app.get("/")
def read_root():
    return {"status": "API is running"}

