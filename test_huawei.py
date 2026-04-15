"""
Smoke test for Huawei eSupplier B2B integration.

Run from the backend directory:
    python test_huawei.py

Expected result on test environment:
    200 OK — result is an empty list (test env has no POs yet, that's normal).
"""

import json
import sys
import os

# Make sure the app package is importable from backend/
sys.path.insert(0, os.path.dirname(__file__))

# Load .env
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from app.services.huawei_client import HuaweiClient, HuaweiApiError
from app.services.po_service import POService
from app.services.acceptance_service import AcceptanceService


def test_find_po_line_list():
    print("\n=== findPoLineList (paginated) ===")
    client = HuaweiClient()
    svc = POService(client=client)
    try:
        result = svc.find_po_line_list()
        print(f"OK — returned {len(result)} PO line(s)")
        if result:
            print("First item:", json.dumps(result[0], indent=2, ensure_ascii=False))
        else:
            print("(empty — normal on test environment)")
    except HuaweiApiError as e:
        print(f"FAIL — HuaweiApiError: {e}  |  status={e.status_code}")
        sys.exit(1)
    except Exception as e:
        print(f"FAIL — {type(e).__name__}: {e}")
        sys.exit(1)


def test_find_ac_pending_po_list():
    print("\n=== findACPendingPOList (paginated) ===")
    client = HuaweiClient()
    svc = AcceptanceService(client=client)
    try:
        result = svc.find_ac_pending_po_list()
        print(f"OK — returned {len(result)} pending AC PO line(s)")
        if result:
            print("First item:", json.dumps(result[0], indent=2, ensure_ascii=False))
        else:
            print("(empty — normal on test environment)")
    except HuaweiApiError as e:
        print(f"FAIL — HuaweiApiError: {e}  |  status={e.status_code}")
        sys.exit(1)
    except Exception as e:
        print(f"FAIL — {type(e).__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    print("Huawei B2B smoke test — starting")
    from app.config import settings
    print(f"  ENV       : {settings.huawei_env}")
    print(f"  APP_ID    : {settings.huawei_app_id}")
    print(f"  APP_KEY   : {'*' * len(settings.huawei_app_key)}")

    test_find_po_line_list()
    test_find_ac_pending_po_list()

    print("\nAll tests passed.")
