# backend/app/routers/acceptances.py

from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, status
from sqlalchemy.orm import Session
import pandas as pd
import io

from ..dependencies import get_db
from .. import crud
from ..auth import get_current_user  # Import your authentication dependency
from .. import models  # To specify the user model type
from fastapi import BackgroundTasks # Import this

router = APIRouter(
    prefix="/api/acceptances",
    tags=["Acceptances"],
    # This ensures all routes in this file require an authenticated user
    dependencies=[Depends(get_current_user)],
)


@router.post("/upload", status_code=status.HTTP_200_OK)
def upload_and_process_acceptances(
    background_tasks: BackgroundTasks,  # <-- Add this parameter
    file: UploadFile = File(..., description="The Acceptance Excel file"),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file type. Please upload an Excel file.",
        )

    # 1. Create History Record (PROCESSING)
    history_record = crud.create_upload_history_record(
        db=db,
        filename=file.filename,
        status="PROCESSING",
        user_id=current_user.id,
        upload_type="Acceptance",  # Ensure this matches your Enum/String for acceptance types
    )

    try:
        # 2. Save file to disk
        temp_dir = "temp_uploads"
        os.makedirs(temp_dir, exist_ok=True)
        temp_file_path = f"{temp_dir}/{history_record.id}_{file.filename}"

        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # 3. Dispatch Background Task
        # Pass the file path, history ID, and user ID to the worker
        background_tasks.add_task(
            crud.process_acceptance_file_background,
            temp_file_path,
            history_record.id,
            current_user.id,
        )

        # 4. Return Immediate Success
        return {
            "message": "Acceptance file uploaded. Processing started in background.",
            "filename": file.filename,
            "history_id": history_record.id,
        }

    except Exception as e:
        # If saving to disk fails before dispatching, update history to failed immediately
        history_record.status = "FAILED"
        history_record.error_message = f"Upload failed: {str(e)}"
        db.commit()
        raise HTTPException(
            status_code=500, detail="Failed to save file for processing."
        )
