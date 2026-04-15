"""
Service: Merged PO bulk update via re-uploaded Excel.

Flow:
1. Parse xlsx with openpyxl
2. Validate ALL rows (identity + enums) — no writes
3. If errors → raise HTTPException(400) with line-by-line error list
4. Resolve each row's writable columns based on user's workflow roles
5. Update in a single transaction, write change-log entries for diffs
"""
from __future__ import annotations

import openpyxl
from datetime import date, datetime
from typing import BinaryIO, List, Dict, Any, Optional, Set

from fastapi import HTTPException
from sqlalchemy.orm import Session

from .. import models
from ..enum import (
    StatusInstallation, ReadinessAcceptance,
    StatusReportIsdp, NeedDocument,
)

# ── Role → column mapping ──────────────────────────────────────────────────
PM_COORD_ROLES: Set[str] = {"ROLE_PM", "ROLE_PC", "ROLE_PD"}
QC_ROLES: Set[str]       = {"ROLE_RQC"}

PM_COORD_COLS: Set[str] = {
    "status_installation", "date_installation", "need_document",
    "date_document_ok", "remark_last_remark", "readiness_acceptance",
    "rejection_remark", "remarks",
}
QC_COLS: Set[str] = {
    "status_report_isdp", "date_close_report_isdp", "remark_qc_reason",
}

PRICE_COLS: Set[str] = {
    "unit_price", "line_amount_hw", "total_ac_amount",
    "accepted_ac_amount", "total_pac_amount", "accepted_pac_amount",
}

ENUM_VALIDATORS: Dict[str, List[str]] = {
    "status_installation":  [e.value for e in StatusInstallation],
    "readiness_acceptance": [e.value for e in ReadinessAcceptance],
    "status_report_isdp":   [e.value for e in StatusReportIsdp],
    "need_document":        [e.value for e in NeedDocument],
}

# Required columns that must exist in the uploaded file
REQUIRED_COLS = {"po_id", "site_code", "item_description"}

# All editable columns we care about (used to parse from file)
ALL_EDITABLE_COLS = PM_COORD_COLS | QC_COLS


def _normalize_col(name: str) -> str:
    """Normalize Excel column header → snake_case field name."""
    return name.strip().lower().replace(" ", "_").replace("/", "_").replace("-", "_")


def _parse_cell_value(value: Any, field: str) -> Any:
    """Convert openpyxl cell value to a Python type suitable for the DB field."""
    if value is None or value == "":
        return None
    if field in ("date_installation", "date_document_ok", "date_close_report_isdp"):
        if isinstance(value, (date, datetime)):
            return value.date() if isinstance(value, datetime) else value
        try:
            return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
        except ValueError:
            try:
                return datetime.strptime(str(value).strip(), "%d/%m/%Y").date()
            except ValueError:
                return None
    return str(value).strip() if value is not None else None


def _get_user_roles_for_project(db: Session, user_id: int, project_id: int) -> Set[str]:
    """Return the set of action_type strings the user holds for a given project."""
    workflows = (
        db.query(models.ProjectWorkflow)
        .filter(models.ProjectWorkflow.project_id == project_id)
        .all()
    )
    roles: Set[str] = set()
    for wf in workflows:
        user_ids = {u.id for u in wf.primary_users} | {u.id for u in wf.support_users}
        if user_id in user_ids:
            roles.add(wf.action_type.value if hasattr(wf.action_type, "value") else str(wf.action_type))
    return roles


def _build_writable_cols(user_roles: Set[str]) -> Set[str]:
    writable: Set[str] = set()
    if user_roles & PM_COORD_ROLES:
        writable |= PM_COORD_COLS
    if user_roles & QC_ROLES:
        writable |= QC_COLS
    return writable


def _primary_action_type(user_roles: Set[str]) -> str:
    """Pick one representative action_type string for the change log."""
    for role in ("ROLE_PM", "ROLE_PC", "ROLE_PD", "ROLE_RQC"):
        if role in user_roles:
            return role
    return "UNKNOWN"


def _is_admin(current_user: models.User) -> bool:
    """Return True if the user has the ADMIN role (bypasses workflow check)."""
    role = getattr(current_user, "role", None)
    if role is None:
        return False
    role_str = role.value if hasattr(role, "value") else str(role)
    return role_str.upper() == "ADMIN"


def process_update_file(
    file_bytes: bytes,
    filename: str,
    current_user: models.User,
    db: Session,
) -> Dict[str, int]:
    """
    Main entry point. Validates and applies the update file.
    Returns {"updated": N, "unchanged": M}.
    Raises HTTPException(400) if validation fails.
    """
    # ── Parse workbook ────────────────────────────────────────────────────
    try:
        wb = openpyxl.load_workbook(filename=__import__("io").BytesIO(file_bytes), data_only=True)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Cannot read xlsx file: {exc}")

    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if len(rows) < 2:
        raise HTTPException(status_code=400, detail="File has no data rows.")

    # Map header → col index (normalised)
    header_row = rows[0]
    col_map: Dict[str, int] = {}
    for idx, cell in enumerate(header_row):
        if cell is not None:
            col_map[_normalize_col(str(cell))] = idx

    # Verify required columns present
    missing = REQUIRED_COLS - set(col_map.keys())
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required columns in file: {', '.join(missing)}",
        )

    data_rows = rows[1:]  # 0-indexed; line numbers reported as 1-based (header=1, first data=2)

    def get(row, field: str) -> Any:
        idx = col_map.get(field)
        return row[idx] if idx is not None and idx < len(row) else None

    # ── Phase 1: Validate all rows, collect errors ────────────────────────
    errors: List[Dict[str, Any]] = []

    for row_idx, row in enumerate(data_rows):
        line_no = row_idx + 2  # 1-based, header is line 1

        po_id_val = get(row, "po_id")
        if not po_id_val:
            errors.append({"line": line_no, "message": "po_id is empty."})
            continue

        po_id_str = str(po_id_val).strip()
        mpo = db.query(models.MergedPO).filter(models.MergedPO.po_id == po_id_str).first()
        if not mpo:
            errors.append({"line": line_no, "message": f"PO ID '{po_id_str}' not found in database."})
            continue

        # Verify site_code
        file_site = str(get(row, "site_code") or "").strip()
        db_site = str(mpo.site_code or "").strip()
        if file_site != db_site:
            errors.append({
                "line": line_no,
                "message": f"Site code mismatch for PO '{po_id_str}': file has '{file_site}', DB has '{db_site}'.",
            })

        # Verify item_description (case-insensitive strip)
        file_desc = str(get(row, "item_description") or "").strip()
        db_desc = str(mpo.item_description or "").strip()
        if file_desc.lower() != db_desc.lower():
            errors.append({
                "line": line_no,
                "message": f"Description mismatch for PO '{po_id_str}': file has '{file_desc[:60]}...', DB has '{db_desc[:60]}...'.",
            })

        # Validate enum columns
        for field, allowed in ENUM_VALIDATORS.items():
            if field not in col_map:
                continue
            raw = get(row, field)
            if raw is None or str(raw).strip() == "":
                continue  # null is fine
            val = str(raw).strip()
            if val not in allowed:
                errors.append({
                    "line": line_no,
                    "message": f"Invalid value '{val}' for column '{field}'. Allowed: {', '.join(allowed)}.",
                })

    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors})

    # ── Phase 2: Apply updates ────────────────────────────────────────────
    updated = 0
    unchanged = 0

    admin = _is_admin(current_user)

    try:
        role_cache: Dict[int, Set[str]] = {}

        for row_idx, row in enumerate(data_rows):
            po_id_str = str(get(row, "po_id") or "").strip()
            if not po_id_str:
                continue

            mpo = db.query(models.MergedPO).filter(models.MergedPO.po_id == po_id_str).first()
            if not mpo:
                continue

            # ADMIN gets full write access without checking project workflows
            if admin:
                user_roles = PM_COORD_ROLES | QC_ROLES
                writable = PM_COORD_COLS | QC_COLS
            else:
                # Resolve writable columns for this project
                project_id = mpo.internal_project_id
                if project_id is None:
                    unchanged += 1
                    continue

                if project_id not in role_cache:
                    role_cache[project_id] = _get_user_roles_for_project(db, current_user.id, project_id)
                user_roles = role_cache[project_id]
                writable = _build_writable_cols(user_roles)
                if not writable:
                    unchanged += 1
                    continue

            # Compute diff
            diff: Dict[str, Dict[str, Any]] = {}
            for field in ALL_EDITABLE_COLS & writable & set(col_map.keys()):
                raw = get(row, field)
                new_val = _parse_cell_value(raw, field)
                old_val = getattr(mpo, field, None)

                # Normalise for comparison
                if isinstance(old_val, date) and not isinstance(old_val, datetime):
                    old_cmp = old_val
                elif old_val is not None:
                    old_cmp = str(old_val).strip() if isinstance(old_val, str) else old_val
                else:
                    old_cmp = None

                if isinstance(new_val, date) and not isinstance(new_val, datetime):
                    new_cmp = new_val
                elif new_val is not None:
                    new_cmp = str(new_val).strip() if isinstance(new_val, str) else new_val
                else:
                    new_cmp = None

                if old_cmp != new_cmp:
                    diff[field] = {
                        "old": str(old_val) if old_val is not None else None,
                        "new": str(new_val) if new_val is not None else None,
                    }
                    setattr(mpo, field, new_val)

            if diff:
                action_type = _primary_action_type(user_roles)
                log_entry = models.MergedPOChangeLog(
                    merged_po_id=mpo.id,
                    po_id=mpo.po_id,
                    site_code=mpo.site_code,
                    item_description=mpo.item_description,
                    changed_by_user_id=current_user.id,
                    changed_at=datetime.utcnow(),
                    action_type=action_type,
                    changes=diff,
                )
                db.add(log_entry)
                updated += 1
            else:
                unchanged += 1

        db.commit()
    except Exception:
        db.rollback()
        raise

    return {"updated": updated, "unchanged": unchanged}


def user_has_pm_or_pc_role(db: Session, user_id: int) -> bool:
    """
    Returns True if the user has ROLE_PM, ROLE_PC or ROLE_PD in ANY project's workflow,
    or if the user is a global ADMIN.
    Used by export to decide whether to strip price columns.
    """
    from sqlalchemy import or_
    
    # 1. Check Global Admin Role
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if user:
        role_str = user.role.value if hasattr(user.role, "value") else str(user.role)
        if role_str.upper() in ["ADMIN", "RAF", "CEO"]:
            return True

    # 2. Check Workflow Roles
    workflows = (
        db.query(models.ProjectWorkflow)
        .filter(
            models.ProjectWorkflow.action_type.in_(["ROLE_PM", "ROLE_PC", "ROLE_PD"]),
            or_(
                models.ProjectWorkflow.primary_users.any(id=user_id),
                models.ProjectWorkflow.support_users.any(id=user_id),
            ),
        )
        .first()
    )
    return workflows is not None
