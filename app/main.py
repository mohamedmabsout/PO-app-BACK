import pandas as pd
import io
import logging
from typing import List

from fastapi import FastAPI, Depends, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from .routers import projects, users, auth, selectors,sites,data_processing,acceptances, summary,targets
from .database import engine ,SessionLocal
from  .dependencies import get_db
from . import crud, models, schemas


models.Base.metadata.create_all(bind=engine, checkfirst=True)

app=FastAPI()

origins = ["http://localhost:5173", "http://127.0.0.1:5173"]
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.include_router(projects.router)
app.include_router(users.router)
app.include_router(auth.router) 
app.include_router(data_processing.router)
app.include_router(selectors.router )
app.include_router(acceptances.router)
app.include_router(sites.router)
app.include_router(summary.router)
app.include_router(targets.router)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
@app.get("/")
def read_root():
    return {"status": "API is running"}
