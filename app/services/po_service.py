"""
Huawei eSupplier B2B — PO Module (8 APIs).

All methods return parsed dicts/lists from the API.
Callers are responsible for persisting to DB.

Rules enforced here:
- poSubType is always "E" (Engineering POs)
- instanceId is always 1 (Huawei Technologies)
- EP-Settlement POs (PurchaseType == "EP-Settlement") must NOT be signed back
  (Huawei auto-accepts them every 30 min)
- New POs and cancellation notifications: operateType must be "accept" only
- cancelNeedQuantity: modify is one line at a time; cancel is batch-allowed
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from .huawei_client import HuaweiClient, PAGE_SIZE_PO

logger = logging.getLogger(__name__)

INSTANCE_ID = 1
PO_SUB_TYPE = "E"


class POService:
    def __init__(self, client: Optional[HuaweiClient] = None):
        self.client = client or HuaweiClient()

    # ------------------------------------------------------------------
    # 1. findPoLineList — query all Engineering PO lines
    # ------------------------------------------------------------------

    def find_po_line_list(
        self,
        status_type: str = "COL_TASK_STATUS",
        extra: Optional[Dict] = None,
    ) -> List[Dict]:
        """
        Fetch all Engineering PO lines (paginated).

        status_type: "COL_TASK_STATUS" for new/pending lines (default).
        Returns flat list of PO line dicts.
        """
        body = {
            "poSubType": PO_SUB_TYPE,
            "statusType": status_type,
            **(extra or {}),
        }
        return self.client.post(
            "/findPoLineList/1.0.0",
            body=body,
            paginated=True,
            page_size=PAGE_SIZE_PO,
        )

    # ------------------------------------------------------------------
    # 2. findColTaskList — PO change details for a specific line
    # ------------------------------------------------------------------

    def find_col_task_list(
        self,
        line_location_id: str,
        instance_id: int = INSTANCE_ID,
    ) -> List[Dict]:
        """
        Fetch change/task details for a specific PO line.
        Requires lineLocationId obtained from findPoLineList.
        """
        body = {
            "lineLocationId": line_location_id,
            "instanceId": instance_id,
        }
        return self.client.post(
            "/findColTaskList/1.0.0",
            body=body,
            paginated=True,
            page_size=PAGE_SIZE_PO,
        )

    # ------------------------------------------------------------------
    # 3. signBackPOList — countersign (accept/reject) PO lines
    # ------------------------------------------------------------------

    def sign_back_po_list(
        self,
        po_lines: List[Dict],
        operate_type: str = "accept",
        instance_id: int = INSTANCE_ID,
    ) -> Dict:
        """
        Accept or reject a batch of PO lines.

        po_lines: list of dicts, each must contain at minimum:
            { "poNumber": ..., "poLineId": ..., "taskId": ..., "lineLocationId": ... }

        operate_type: "accept" or "reject".
            NOTE: New POs and cancellation notifications must always be "accept".
            EP-Settlement POs must be SKIPPED — Huawei auto-accepts them.

        Returns raw API response dict.
        """
        if operate_type not in ("accept", "reject"):
            raise ValueError("operate_type must be 'accept' or 'reject'")

        # Filter out EP-Settlement POs (safety guard)
        safe_lines = [
            line for line in po_lines
            if line.get("purchaseType") != "EP-Settlement"
        ]
        skipped = len(po_lines) - len(safe_lines)
        if skipped:
            logger.warning(
                "signBackPOList: skipped %d EP-Settlement PO line(s) — Huawei auto-accepts them.",
                skipped,
            )

        body = {
            "instanceId": instance_id,
            "operateType": operate_type,
            "poLines": safe_lines,
        }
        return self.client.post("/signBackPOList/1.0.0", body=body, paginated=False)

    # ------------------------------------------------------------------
    # 4. genPdfOfPo — generate PDF for a PO, returns downloadKey
    # ------------------------------------------------------------------

    def gen_pdf_of_po(
        self,
        po_number: str,
        instance_id: int = INSTANCE_ID,
    ) -> str:
        """
        Trigger PDF generation for a PO.
        Returns the downloadKey string needed for file_download().
        """
        body = {
            "poNumber": po_number,
            "instanceId": instance_id,
        }
        resp = self.client.post("/genPdfOfPo/1.0.0", body=body, paginated=False)
        download_key = resp.get("downloadKey") or resp.get("result")
        if not download_key:
            logger.error("genPdfOfPo: no downloadKey in response: %s", resp)
        return download_key

    # ------------------------------------------------------------------
    # 5. filedownload — download binary file using downloadKey
    # ------------------------------------------------------------------

    def file_download(self, download_key: str, instance_id: int = INSTANCE_ID) -> bytes:
        """
        Download a file (PDF) using the key from gen_pdf_of_po().
        Returns raw bytes.
        """
        body = {
            "downloadKey": download_key,
            "instanceId": instance_id,
        }
        return self.client.post_binary("/download/1.0.0", body=body)

    def save_pdf(self, download_key: str, dest_path: Path, instance_id: int = INSTANCE_ID) -> Path:
        """Convenience: download PDF and save to dest_path. Returns the path."""
        raw = self.file_download(download_key, instance_id)
        dest_path.write_bytes(raw)
        logger.info("PO PDF saved to %s (%d bytes)", dest_path, len(raw))
        return dest_path

    # ------------------------------------------------------------------
    # 6. findApproverList — query approvers before cancelling a PO
    # ------------------------------------------------------------------

    def find_approver_list(
        self,
        po_number: str,
        instance_id: int = INSTANCE_ID,
    ) -> List[Dict]:
        """
        Fetch the list of approvers for a PO.
        Required step before calling cancel_need_quantity().
        """
        body = {
            "poNumber": po_number,
            "instanceId": instance_id,
        }
        return self.client.post(
            "/findApproverList",
            body=body,
            paginated=False,
        )

    # ------------------------------------------------------------------
    # 7. findChangeReason — query change reasons before cancelling a PO
    # ------------------------------------------------------------------

    def find_change_reason(self, instance_id: int = INSTANCE_ID) -> List[Dict]:
        """
        Fetch the list of valid change/cancel reasons.
        Required step before calling cancel_need_quantity().
        """
        body = {"instanceId": instance_id}
        resp = self.client.post("/findChangeReason", body=body, paginated=False)
        # Response is typically a list directly or wrapped
        if isinstance(resp, list):
            return resp
        return resp.get("result") or resp.get("reasons") or []

    # ------------------------------------------------------------------
    # 8. cancelNeedQuantity — cancel remaining qty or modify qty
    # ------------------------------------------------------------------

    def cancel_need_quantity(
        self,
        po_line: Dict,
        cancel_type: str,
        reason_code: str,
        approver_employee_num: str,
        instance_id: int = INSTANCE_ID,
    ) -> Dict:
        """
        Cancel remaining quantity or modify quantity on a PO line.

        cancel_type: "cancel" (batch OK) or "modify" (one line at a time only).
        po_line must contain: poNumber, poLineId, lineLocationId, and for modify: newQty.
        reason_code: from findChangeReason().
        approver_employee_num: from findApproverList().

        Returns raw API response dict.
        """
        if cancel_type not in ("cancel", "modify"):
            raise ValueError("cancel_type must be 'cancel' or 'modify'")

        body = {
            "instanceId": instance_id,
            "cancelType": cancel_type,
            "reasonCode": reason_code,
            "approverEmployeeNum": approver_employee_num,
            "poLine": po_line,
        }
        return self.client.post("/cancelNeedQuantity", body=body, paginated=False)


# Singleton
po_service = POService()
