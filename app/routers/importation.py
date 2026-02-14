import os
import shutil
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, UploadFile, File, status
from sqlalchemy.orm import Session
from .. import crud, models, auth
from ..dependencies import get_db
router = APIRouter(prefix="/api/import", tags=["import"])

@router.post("/unified")
async def unified_import(
    background_tasks: BackgroundTasks,
    po_file: UploadFile = File(None),
    ac_file: UploadFile = File(None),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    if not po_file and not ac_file:
        raise HTTPException(status_code=400, detail="No files provided.")

    temp_dir = "temp_uploads"
    os.makedirs(temp_dir, exist_ok=True)

    po_info = None
    ac_info = None

    # 1. Prepare PO File if exists
    if po_file:
        if not po_file.filename.endswith((".xlsx", ".xls")):
            raise HTTPException(status_code=400, detail="Invalid PO file type.")
        
        history_po = crud.create_upload_history_record(
            db=db, filename=f"[PO] {po_file.filename}", status="PROCESSING", user_id=current_user.id
        )
        po_path = f"{temp_dir}/po_{history_po.id}_{po_file.filename}"
        with open(po_path, "wb") as buffer:
            shutil.copyfileobj(po_file.file, buffer)
        po_info = {"path": po_path, "history_id": history_po.id}

    # 2. Prepare AC File if exists
    if ac_file:
        if not ac_file.filename.endswith((".xlsx", ".xls")):
            raise HTTPException(status_code=400, detail="Invalid Acceptance file type.")
        
        # If PO file exists, set AC status to "WAITING" (Waiting for PO to finish)
        initial_status = "WAITING" if po_file else "PROCESSING"
        history_ac = crud.create_upload_history_record(
            db=db, filename=f"[AC] {ac_file.filename}", status=initial_status, user_id=current_user.id
        )
        ac_path = f"{temp_dir}/ac_{history_ac.id}_{ac_file.filename}"
        with open(ac_path, "wb") as buffer:
            shutil.copyfileobj(ac_file.file, buffer)
        ac_info = {"path": ac_path, "history_id": history_ac.id}

    # 3. Trigger Background Tasks
    if po_info:
        # If we have a PO file, start it and tell it to trigger AC after it finishes
        background_tasks.add_task(
            crud.process_po_file_background,
            po_info["path"],
            po_info["history_id"],
            current_user.id,
            ac_info # This is the "Chain" parameter
        )
    elif ac_info:
        # Only AC file provided, start it immediately
        background_tasks.add_task(
            crud.process_acceptance_file_background,
            ac_info["path"],
            ac_info["history_id"],
            current_user.id
        )

    return {
        "message": "Upload successful. Processing background tasks.",
        "po_history_id": po_info["history_id"] if po_info else None,
        "ac_history_id": ac_info["history_id"] if ac_info else None
    }