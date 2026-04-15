"""
Huawei eSupplier B2B — Invoice & Deduction Module (9 APIs).

Covers:
  - Invoice queries (per PO, header details, payment status, vendor payment)
  - Deduction queries and reply
  - History/audit logs (AC operation history, approval log)
"""

import logging
from typing import Any, Dict, List, Optional

from .huawei_client import HuaweiClient, PAGE_SIZE_INV

logger = logging.getLogger(__name__)

INSTANCE_ID = 1


class InvoiceService:
    def __init__(self, client: Optional[HuaweiClient] = None):
        self.client = client or HuaweiClient()

    # ------------------------------------------------------------------
    # 1. queryPoInvoice — invoices linked to a PO (GET)
    # ------------------------------------------------------------------

    def query_po_invoice(
        self,
        po_number: str,
        instance_id: int = INSTANCE_ID,
    ) -> List[Dict]:
        """
        Fetch all invoices linked to a PO number (GET endpoint).
        """
        resp = self.client.get(
            "/queryPoInvoice",
            params={"poNumber": po_number, "instanceId": instance_id},
        )
        if isinstance(resp, list):
            return resp
        return resp.get("result") or resp.get("invoices") or []

    # ------------------------------------------------------------------
    # 2. queryInvoiceHead — invoice header details
    # ------------------------------------------------------------------

    def query_invoice_head(
        self,
        invoice_ids: List[str],
        instance_id: int = INSTANCE_ID,
    ) -> List[Dict]:
        """
        Fetch header details for the given invoice IDs.
        """
        body = {
            "instanceId": instance_id,
            "invoiceIds": invoice_ids,
        }
        resp = self.client.post("/queryInvoiceHead", body=body, paginated=False)
        if isinstance(resp, list):
            return resp
        return resp.get("result") or resp.get("invoices") or []

    # ------------------------------------------------------------------
    # 3. queryInvoicePayment — payment status of invoices
    # ------------------------------------------------------------------

    def query_invoice_payment(
        self,
        invoice_ids: List[str],
        instance_id: int = INSTANCE_ID,
    ) -> List[Dict]:
        """
        Fetch payment status for the given invoice IDs.
        """
        body = {
            "instanceId": instance_id,
            "invoiceIds": invoice_ids,
        }
        resp = self.client.post("/queryInvoicePayment", body=body, paginated=False)
        if isinstance(resp, list):
            return resp
        return resp.get("result") or resp.get("payments") or []

    # ------------------------------------------------------------------
    # 4. queryPaymentVendor — vendor payment info
    # ------------------------------------------------------------------

    def query_payment_vendor(
        self,
        vendor_code: str,
        instance_id: int = INSTANCE_ID,
        extra: Optional[Dict] = None,
    ) -> Dict:
        """
        Fetch vendor payment details.
        """
        body = {
            "instanceId": instance_id,
            "vendorCode": vendor_code,
            **(extra or {}),
        }
        return self.client.post("/queryPaymentVendor", body=body, paginated=False)

    # ------------------------------------------------------------------
    # 5. queryPoDeductList — list of deductions on POs
    # ------------------------------------------------------------------

    def query_po_deduct_list(
        self,
        po_number: str,
        instance_id: int = INSTANCE_ID,
        extra: Optional[Dict] = None,
    ) -> List[Dict]:
        """
        Fetch the list of deductions applied to a PO.
        """
        body = {
            "instanceId": instance_id,
            "poNumber": po_number,
            **(extra or {}),
        }
        resp = self.client.post(
            "/queryPoDeductList",
            body=body,
            paginated=True,
            page_size=PAGE_SIZE_INV,
        )
        return resp  # paginated returns a flat list

    # ------------------------------------------------------------------
    # 6. queryPoDeductInfo — deduction detail per PO
    # ------------------------------------------------------------------

    def query_po_deduct_info(
        self,
        deduct_id: str,
        instance_id: int = INSTANCE_ID,
    ) -> Dict:
        """
        Fetch full detail for a specific deduction.
        deduct_id: from queryPoDeductList.
        """
        body = {
            "instanceId": instance_id,
            "deductId": deduct_id,
        }
        return self.client.post("/queryPoDeductInfo", body=body, paginated=False)

    # ------------------------------------------------------------------
    # 7. replyPoDeduct — respond to a deduction
    # ------------------------------------------------------------------

    def reply_po_deduct(
        self,
        deduct_id: str,
        reply_type: str,
        remark: str = "",
        instance_id: int = INSTANCE_ID,
        extra: Optional[Dict] = None,
    ) -> Dict:
        """
        Submit a reply (agree/dispute) to a deduction.

        deduct_id: from queryPoDeductList.
        reply_type: typically "agree" or "dispute" — verify with Huawei docs for allowed values.
        remark: optional comment text.
        """
        body = {
            "instanceId": instance_id,
            "deductId": deduct_id,
            "replyType": reply_type,
            "remark": remark,
            **(extra or {}),
        }
        return self.client.post("/replyPoDeduct", body=body, paginated=False)

    # ------------------------------------------------------------------
    # 8. queryACOprationHis — full operation history of an acceptance order
    # ------------------------------------------------------------------

    def query_ac_operation_history(
        self,
        ac_header_id: str,
        instance_id: int = INSTANCE_ID,
    ) -> List[Dict]:
        """
        Fetch the complete operation history for an acceptance order.
        ac_header_id: from createAC or findACLineList.
        """
        body = {
            "instanceId": instance_id,
            "acHeaderId": ac_header_id,
        }
        resp = self.client.post("/queryACOprationHis", body=body, paginated=False)
        if isinstance(resp, list):
            return resp
        return resp.get("result") or resp.get("history") or []

    # ------------------------------------------------------------------
    # 9. queryApproveLogList — approval log per PO or AC
    # ------------------------------------------------------------------

    def query_approve_log_list(
        self,
        reference_id: str,
        reference_type: str = "PO",
        instance_id: int = INSTANCE_ID,
    ) -> List[Dict]:
        """
        Fetch approval log entries for a PO or acceptance order.

        reference_type: "PO" or "AC".
        reference_id: poNumber or acNum depending on reference_type.
        """
        body = {
            "instanceId": instance_id,
            "referenceId": reference_id,
            "referenceType": reference_type,
        }
        resp = self.client.post(
            "/queryApproveLogList",
            body=body,
            paginated=True,
            page_size=PAGE_SIZE_INV,
        )
        return resp  # paginated returns a flat list


# Singleton
invoice_service = InvoiceService()
