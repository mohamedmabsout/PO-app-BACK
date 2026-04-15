"""
Huawei eSupplier B2B — Base HTTP client.

Auth: two static headers per request (x-hw-id + x-hw-appkey).
Pagination: cursor loop — stop when totalPages == 0 or curPage >= totalPages.
"""

import logging
from typing import Any, Dict, Generator, List, Optional

import httpx

from ..config import settings

logger = logging.getLogger(__name__)

BASE_URLS = {
    "test": "https://apigw-scs-beta.huawei.com/api/service/esupplier",
    "prod": "https://apigw-scs.huawei.com/api/service/esupplier",
}

# Recommended page sizes per module (from Huawei docs)
PAGE_SIZE_PO = 200
PAGE_SIZE_AC = 100
PAGE_SIZE_INV = 100


class HuaweiApiError(Exception):
    """Raised when Huawei API returns a non-200 HTTP status or success:false body."""

    def __init__(self, message: str, status_code: int = 0, body: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class HuaweiClient:
    """
    Thin wrapper around httpx for calling Huawei eSupplier ROMA REST APIs.

    Usage:
        client = HuaweiClient()
        data = client.post("/findPoLineList/1.0.0", body={...}, paginated=True)
    """

    def __init__(self):
        env = settings.huawei_env.lower()
        if env not in BASE_URLS:
            raise ValueError(f"Invalid HUAWEI_ENV '{env}'. Must be 'test' or 'prod'.")
        self.base_url = BASE_URLS[env]
        self._headers = {
            "x-hw-id": settings.huawei_app_id,
            "x-hw-appkey": settings.huawei_app_key,
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _url(self, path: str, page_size: int = PAGE_SIZE_PO, cur_page: int = 1) -> str:
        """Build full URL with optional suffix_path pagination query param."""
        base = f"{self.base_url}/{path.lstrip('/')}"
        return f"{base}?suffix_path=/{page_size}/{cur_page}"

    def _url_no_paging(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def _check_response(self, response: httpx.Response) -> Dict:
        if response.status_code != 200:
            raise HuaweiApiError(
                f"HTTP {response.status_code} from {response.url}",
                status_code=response.status_code,
                body=response.text,
            )
        data = response.json()
        # Some Huawei endpoints wrap errors in a success flag
        if isinstance(data, dict) and data.get("success") is False:
            raise HuaweiApiError(
                f"API returned success=false: {data.get('errorMsg') or data}",
                status_code=response.status_code,
                body=data,
            )
        return data

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def post(
        self,
        path: str,
        body: Dict,
        *,
        paginated: bool = False,
        page_size: int = PAGE_SIZE_PO,
        timeout: int = 30,
    ) -> Any:
        """
        POST to a Huawei endpoint.

        If paginated=True, fetches all pages and returns a flat list of all
        items found across pages (reads the first non-empty list value in the
        response body — works for all known Huawei list endpoints).

        If paginated=False, returns the raw response dict.
        """
        if not paginated:
            url = self._url_no_paging(path)
            with httpx.Client(headers=self._headers, timeout=timeout) as client:
                resp = client.post(url, json=body)
            return self._check_response(resp)

        # --- Paginated fetch ---
        all_items: List[Any] = []
        cur_page = 1

        with httpx.Client(headers=self._headers, timeout=timeout) as client:
            while True:
                url = self._url(path, page_size=page_size, cur_page=cur_page)
                resp = client.post(url, json=body)
                data = self._check_response(resp)

                page_vo = data.get("pageVO") or data.get("page") or {}
                total_pages = int(page_vo.get("totalPages") or page_vo.get("totalPage") or 0)
                items = _extract_list(data)
                all_items.extend(items)

                logger.debug(
                    "Huawei paginated fetch %s — page %d/%d, got %d items",
                    path, cur_page, total_pages, len(items),
                )

                if total_pages == 0 or cur_page >= total_pages:
                    break
                cur_page += 1

        return all_items

    def get(self, path: str, params: Optional[Dict] = None, timeout: int = 30) -> Any:
        """GET request (used by a few endpoints like queryPoInvoice)."""
        url = self._url_no_paging(path)
        with httpx.Client(headers=self._headers, timeout=timeout) as client:
            resp = client.get(url, params=params)
        return self._check_response(resp)

    def post_multipart(self, path: str, files: Dict, data: Optional[Dict] = None, timeout: int = 60) -> Any:
        """Multipart/form-data POST — used for uploadFile."""
        url = self._url_no_paging(path)
        # httpx handles multipart automatically; don't pass Content-Type header
        headers = {k: v for k, v in self._headers.items() if k != "Content-Type"}
        with httpx.Client(headers=headers, timeout=timeout) as client:
            resp = client.post(url, files=files, data=data or {})
        return self._check_response(resp)

    def post_binary(self, path: str, body: Dict, timeout: int = 60) -> bytes:
        """POST that returns raw binary (used for filedownload)."""
        url = self._url_no_paging(path)
        with httpx.Client(headers=self._headers, timeout=timeout) as client:
            resp = client.post(url, json=body)
        if resp.status_code != 200:
            raise HuaweiApiError(
                f"HTTP {resp.status_code} from {resp.url}",
                status_code=resp.status_code,
                body=resp.text,
            )
        return resp.content


# ------------------------------------------------------------------
# Utility
# ------------------------------------------------------------------

def _extract_list(data: Dict) -> List:
    """
    Find the first list value in the response dict.
    Huawei endpoints vary the key name (result, poList, acList, etc.).
    pageVO / page are skipped.
    """
    skip_keys = {"pageVO", "page", "success", "errorMsg", "errorCode"}
    for key, value in data.items():
        if key in skip_keys:
            continue
        if isinstance(value, list):
            return value
    return []


# Singleton for import convenience
huawei_client = HuaweiClient()
