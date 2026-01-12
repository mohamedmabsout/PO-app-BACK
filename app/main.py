# app/main.py
import os
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from .routers import otp

from .database import engine
from . import models
from .routers import (
    projects, users, auth, selectors, sites, data_processing,
    acceptances, summary, targets, export, sbcs, notifications
)

app = FastAPI(title="PO API", version="0.1.0")

models.Base.metadata.create_all(bind=engine, checkfirst=True)
app.include_router(otp.router)

origins = ["http://localhost:5173", "http://127.0.0.1:5173"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

# ✅ include routers ONCE
app.include_router(projects.router)
app.include_router(users.router)
app.include_router(auth.router)  # must be a FastAPI APIRouter (not django router)
app.include_router(data_processing.router)
app.include_router(selectors.router)
app.include_router(acceptances.router)
app.include_router(sites.router)
app.include_router(summary.router)
app.include_router(targets.router)
app.include_router(export.router)
app.include_router(sbcs.router)
app.include_router(notifications.router)

logging.basicConfig(level=logging.INFO)

os.makedirs("uploads/sbc_docs", exist_ok=True)
app.mount("/static", StaticFiles(directory="uploads"), name="static")

@app.get("/")
def read_root():
    return {"status": "API is running"}
