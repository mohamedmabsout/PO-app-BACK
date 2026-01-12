# app/routers/otp.py
import time, random, datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.config import settings
# from app.services.infobip_sms import send_sms
from app.auth import create_access_token
import app.config as cfg

print("CONFIG FILE USED =", cfg.__file__)
print("SETTINGS TYPE =", type(settings))
print("HAS INFOBIP_BASE_URL ?", hasattr(settings, "INFOBIP_BASE_URL"))
print("SETTINGS KEYS =", list(settings.model_dump().keys()))
router = APIRouter(prefix="/auth", tags=["OTP"])

OTP_STORE = {}  # demo only (use Redis in prod)

class SendOtpIn(BaseModel):
    phone: str

class VerifyOtpIn(BaseModel):
    phone: str
    code: str

def normalize_phone_ma(phone: str) -> str:
    p = phone.strip().replace(" ", "").replace("-", "")
    if p.startswith("+"):
        return p
    if p.startswith("212"):
        return "+" + p
    if p.startswith("0") and len(p) >= 10:
        return "+212" + p[1:]
    return p

@router.post("/send-otp")
def send_otp(payload: SendOtpIn):
    if not settings.INFOBIP_BASE_URL or not settings.INFOBIP_API_KEY:
        raise HTTPException(status_code=500, detail="Missing Infobip env variables")

    phone = normalize_phone_ma(payload.phone)
    code = "".join(str(random.randint(0, 9)) for _ in range(6))
    expires_at = int(time.time()) + settings.OTP_TTL_SECONDS

    OTP_STORE[phone] = {"code": code, "expires_at": expires_at}

  
    return {"success": True, "ttl_seconds": settings.OTP_TTL_SECONDS}

@router.post("/verify-otp")
def verify_otp(payload: VerifyOtpIn):
    phone = normalize_phone_ma(payload.phone)
    data = OTP_STORE.get(phone)

    if not data:
        raise HTTPException(status_code=400, detail="No OTP found for this phone")
    if int(time.time()) > data["expires_at"]:
        raise HTTPException(status_code=400, detail="OTP expired")
    if payload.code != data["code"]:
        raise HTTPException(status_code=400, detail="Invalid OTP")

    del OTP_STORE[phone]

    token = create_access_token(
        data={"sub": phone},
        expires_delta=datetime.timedelta(days=7)
    )

    return {
        "verified": True,
        "access_token": token,
        "token_type": "bearer"
    }