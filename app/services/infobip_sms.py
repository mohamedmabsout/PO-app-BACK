# app/services/infobip_sms.py
import requests
from fastapi import HTTPException
from app.config import settings

def send_sms(phone: str, message: str):
    base = settings.INFOBIP_BASE_URL.strip()

    # si jamais tu as mis https:// par erreur dans .env
    base = base.replace("https://", "").replace("http://", "")

    url = f"https://eejjzn.api.infobip.com/sms/2/text/advanced"

    payload = {
        "messages": [
            {
                "from": settings.INFOBIP_SENDER,
                "destinations": [{"to": phone}],
                "text": message,
            }
        ]
    }

    headers = {
        "Authorization": f"App {settings.INFOBIP_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    try:
        r = requests.post(url, json=payload, headers=headers, timeout=20)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Infobip request error: {e}")

    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Infobip error: {r.text}")

    return r.json()
