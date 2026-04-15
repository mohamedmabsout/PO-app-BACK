"""
Huawei eSupplier B2B — Acceptance Module (9 APIs).

This module handles submitting SIB's acceptance orders TO Huawei.
(RawAcceptance data coming FROM Huawei is a separate concern handled in crud.py.)

Full flow:
  1. find_ac_pending_po_list()         — find PO lines eligible for acceptance
  2. find_ac_approvers()               — get approver employee numbers
  3. query_ac_milestone()              — get milestone data for those PO lines
  4. upload_file()                     — upload acceptance document → docId
  5. create_ac()                       — create acceptance order (uses docId)
  6. find_ac_line_list()               — track/poll status of acceptance orders
  7. withdraw_ac()                     — withdraw if status == Pending
  8. delete_ac()                       — delete if status == Rejected or Withdrawn

Rules:
- findACLineList MUST always include creationDateFrom + creationDateTo (API will timeout without them)
- Only group PO lines with the same project AND same PO type in one createAC call
- withdrawAC: only allowed when status = Pending
- deleteAC: only allowed when status = Rejected or Withdrawn
"""

import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

from .huawei_client import HuaweiClient, PAGE_SIZE_AC

logger = logging.getLogger(__name__)

INSTANCE_ID = 1


class AcceptanceService:
    def __init__(self, client: Optional[HuaweiClient] = None):
        self.client = client or HuaweiClient()

    # ------------------------------------------------------------------
    # 1. findACPendingPOList — PO lines eligible for acceptance
    # ------------------------------------------------------------------

    def find_ac_pending_po_list(
        self,
        org_id: Optional[str] = None,
        instance_id: int = INSTANCE_ID,
        extra: Optional[Dict] = None,
    ) -> List[Dict]:
        """
        Fetch PO lines that are ready to be accepted (pending acceptance).
        Returns flat list across all pages.
        """
        body = {
            "instanceId": instance_id,
            **({"orgId": org_id} if org_id else {}),
            **(extra or {}),
        }
        return self.client.post(
            "/findACPendingPOList/1.0.0",
            body=body,
            paginated=True,
            page_size=PAGE_SIZE_AC,
        )

    # ------------------------------------------------------------------
    # 2. findACPendingISDPPOList — ISDP project PO lines for acceptance
    # ------------------------------------------------------------------

    def find_ac_pending_isdp_po_list(
        self,
        org_id: Optional[str] = None,
        instance_id: int = INSTANCE_ID,
        extra: Optional[Dict] = None,
    ) -> List[Dict]:
        """
        Same as find_ac_pending_po_list but for ISDP project PO lines.
        """
        body = {
            "instanceId": instance_id,
            **({"orgId": org_id} if org_id else {}),
            **(extra or {}),
        }
        return self.client.post(
            "/findACPendingISDPPOList/1.0.0",
            body=body,
            paginated=True,
            page_size=PAGE_SIZE_AC,
        )

    # ------------------------------------------------------------------
    # 3. findACApprovers — get approvers before creating acceptance
    # ------------------------------------------------------------------

    def find_ac_approvers(
        self,
        org_id: str,
        instance_id: int = INSTANCE_ID,
    ) -> List[Dict]:
        """
        Fetch approvers for an acceptance order.
        Returns list of approver dicts (each contains employeeNum).
        Store employeeNum — required in createAC.
        """
        body = {
            "orgId": org_id,
            "instanceId": instance_id,
        }
        resp = self.client.post("/findACApprovers/1.0.0", body=body, paginated=False)
        if isinstance(resp, list):
            return resp
        return resp.get("result") or resp.get("approvers") or []

    # ------------------------------------------------------------------
    # 4. queryACMilestone — milestone data for PO lines
    # ------------------------------------------------------------------

    def query_ac_milestone(
        self,
        po_line_ids: List[str],
        instance_id: int = INSTANCE_ID,
    ) -> List[Dict]:
        """
        Fetch milestone data for the given PO line IDs.
        po_line_ids: list of poLineId strings from findACPendingPOList.
        """
        body = {
            "poLineIds": po_line_ids,
            "instanceId": instance_id,
        }
        resp = self.client.post("/queryACMilestone/1.0.0", body=body, paginated=False)
        if isinstance(resp, list):
            return resp
        return resp.get("result") or resp.get("milestones") or []

    # ------------------------------------------------------------------
    # 5. uploadFile — upload acceptance document, returns docId
    # ------------------------------------------------------------------

    def upload_file(
        self,
        file_path: Path,
        file_name: Optional[str] = None,
        instance_id: int = INSTANCE_ID,
    ) -> str:
        """
        Upload an acceptance document (PDF, image, etc.) via multipart/form-data.

        Returns docId string — required in create_ac().
        """
        name = file_name or file_path.name
        with open(file_path, "rb") as f:
            files = {"file": (name, f, _mime_type(file_path))}
            data = {"instanceId": str(instance_id)}
            resp = self.client.post_multipart("/uploadFile/1.0.0", files=files, data=data)

        doc_id = resp.get("docId") or resp.get("result")
        if not doc_id:
            logger.error("uploadFile: no docId in response: %s", resp)
        return doc_id

    # ------------------------------------------------------------------
    # 6. createAC — create acceptance order
    # ------------------------------------------------------------------

    def create_ac(
        self,
        po_lines: List[Dict],
        doc_id: str,
        approver_employee_num: str,
        ac_date: Optional[str] = None,
        instance_id: int = INSTANCE_ID,
        extra: Optional[Dict] = None,
    ) -> Dict:
        """
        Create an acceptance order.

        po_lines: must all share the same project AND the same PO type.
            Each dict needs at minimum: poNumber, poLineId, orgId, acceptQty, unitCode.
        doc_id: from upload_file().
        approver_employee_num: from find_ac_approvers().
        ac_date: ISO date string (YYYY-MM-DD). Defaults to today.
        Returns raw response dict — capture acNum and acHeaderId from it.
        """
        body = {
            "instanceId": instance_id,
            "docId": doc_id,
            "approverEmployeeNum": approver_employee_num,
            "acDate": ac_date or date.today().isoformat(),
            "poLines": po_lines,
            **(extra or {}),
        }
        return self.client.post("/createAC/1.0.0", body=body, paginated=False)

    # ------------------------------------------------------------------
    # 7. findACLineList — track acceptance order status
    # ------------------------------------------------------------------

    def find_ac_line_list(
        self,
        creation_date_from: str,
        creation_date_to: str,
        instance_id: int = INSTANCE_ID,
        extra: Optional[Dict] = None,
    ) -> List[Dict]:
        """
        Track/poll the status of acceptance orders.

        REQUIRED: creation_date_from and creation_date_to (ISO date strings YYYY-MM-DD).
        The API will timeout without these filters — never call without them.

        Returns flat list across all pages.
        """
        if not creation_date_from or not creation_date_to:
            raise ValueError(
                "findACLineList requires both creation_date_from and creation_date_to. "
                "The API will timeout without date filters."
            )
        body = {
            "instanceId": instance_id,
            "creationDateFrom": creation_date_from,
            "creationDateTo": creation_date_to,
            **(extra or {}),
        }
        return self.client.post(
            "/findACLineList/1.0.0",
            body=body,
            paginated=True,
            page_size=PAGE_SIZE_AC,
        )

    # ------------------------------------------------------------------
    # 8. withdrawAC — withdraw a pending acceptance
    # ------------------------------------------------------------------

    def withdraw_ac(
        self,
        ac_header_id: str,
        ac_num: str,
        instance_id: int = INSTANCE_ID,
    ) -> Dict:
        """
        Withdraw an acceptance order. Only valid when status == Pending.
        ac_header_id: from createAC response or findACLineList.
        ac_num: acceptance order number from createAC.
        """
        body = {
            "instanceId": instance_id,
            "acHeaderId": ac_header_id,
            "acNum": ac_num,
        }
        return self.client.post("/withdrawAC/1.0.0", body=body, paginated=False)

    # ------------------------------------------------------------------
    # 9. deleteAC — delete a rejected or withdrawn acceptance
    # ------------------------------------------------------------------

    def delete_ac(
        self,
        ac_header_id: str,
        ac_num: str,
        instance_id: int = INSTANCE_ID,
    ) -> Dict:
        """
        Delete an acceptance order. Only valid when status == Rejected or Withdrawn.
        """
        body = {
            "instanceId": instance_id,
            "acHeaderId": ac_header_id,
            "acNum": ac_num,
        }
        return self.client.post("/deleteAC/1.0.0", body=body, paginated=False)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _mime_type(path: Path) -> str:
    ext = path.suffix.lower()
    return {
        ".pdf": "application/pdf",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xls": "application/vnd.ms-excel",
    }.get(ext, "application/octet-stream")


# Singleton
acceptance_service = AcceptanceService()
