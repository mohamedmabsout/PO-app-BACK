from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from typing import Optional
import io
import pandas as pd

from .. import crud, models, schemas, auth
from ..dependencies import get_db

router = APIRouter(prefix="/api/category-rules", tags=["category_rules"])

VALID_CATEGORIES = ["Service", "Transport", "Survey", "Civil Work", "Material"]


def _require_admin(current_user: models.User = Depends(auth.get_current_user)):
    if current_user.role != models.UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Admin only.")
    return current_user


@router.get("/template")
def download_template(current_user: models.User = Depends(_require_admin)):
    df = pd.DataFrame({
        "item_description": ["Paste the exact item description here"],
        "category": [f"One of: {', '.join(VALID_CATEGORIES)}"],
    })
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="CategoryRules")
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=category_rules_template.xlsx"},
    )


@router.post("/bulk-import")
def bulk_import(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(_require_admin),
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Upload an Excel file (.xlsx or .xls)")
    try:
        df = pd.read_excel(io.BytesIO(file.file.read()))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse file: {e}")

    required = {"item_description", "category"}
    if not required.issubset({c.strip().lower() for c in df.columns}):
        raise HTTPException(status_code=400, detail="File must have columns: item_description, category")

    df.columns = [c.strip().lower() for c in df.columns]
    rows = df[["item_description", "category"]].dropna(how="all").to_dict(orient="records")
    result = crud.bulk_upsert_category_rules(db, rows)
    return result


@router.get("/", response_model=schemas.PaginatedCategoryRules)
def list_rules(
    page: int = Query(1, gt=0),
    per_page: int = Query(50, gt=0),
    search: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    return crud.get_category_rules(db, page=page, per_page=per_page, search=search)


@router.post("/", response_model=schemas.CategoryRuleOut, status_code=201)
def create_rule(
    payload: schemas.CategoryRuleCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(_require_admin),
):
    try:
        return crud.create_category_rule(db, payload.item_description, payload.category)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/{rule_id}", response_model=schemas.CategoryRuleOut)
def update_rule(
    rule_id: int,
    payload: schemas.CategoryRuleCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(_require_admin),
):
    try:
        return crud.update_category_rule(db, rule_id, payload.item_description, payload.category)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.delete("/{rule_id}", status_code=204)
def delete_rule(
    rule_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(_require_admin),
):
    try:
        crud.delete_category_rule(db, rule_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
