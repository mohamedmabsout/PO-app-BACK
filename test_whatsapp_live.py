"""
Live smoke test for WasenderAPI WhatsApp integration.

Sends a real test message — no server needed, runs standalone.

Usage:
    venv/Scripts/python test_whatsapp_live.py +212600000000

The phone number argument is the recipient in E.164 format.
Use your own number to verify receipt.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

import httpx

WASENDER_URL = "https://www.wasenderapi.com/api/send-message"


def send_live(to_number: str) -> dict:
    api_key = os.getenv("WASENDER_API_KEY")
    if not api_key:
        print("ERROR: WASENDER_API_KEY not found in .env")
        sys.exit(1)

    message = (
        "*SIB Portal — WhatsApp Integration Test\n"
        "Status: TEST MESSAGE\n"
        "Reference: SMOKE-TEST-001\n"
        "Project: Integration Verification\n"
        "Total: 0.00 MAD\n"
        "Portal: https://po.sib.co.ma"
    )

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {"to": to_number, "text": message}

    print(f"\nSending to : {to_number}")
    print(f"API key   : {api_key[:8]}{'*' * (len(api_key) - 8)}")
    print(f"Endpoint  : {WASENDER_URL}")
    print(f"Message   :\n{message}\n")

    with httpx.Client(timeout=15) as client:
        resp = client.post(WASENDER_URL, json=payload, headers=headers)

    print(f"HTTP status : {resp.status_code}")
    print(f"Response    : {resp.text}")

    return resp


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: venv/Scripts/python test_whatsapp_live.py +212600000000")
        sys.exit(1)

    to = sys.argv[1]
    resp = send_live(to)

    if resp.status_code == 200:
        print("\nSUCCESS — check your WhatsApp for the message.")
        sys.exit(0)
    else:
        print(f"\nFAILED — status {resp.status_code}")
        sys.exit(1)
