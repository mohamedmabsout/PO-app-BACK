from typing import Optional
from sqlalchemy.orm import Session

from app import models


def _normalize_phone_ma(phone: str) -> str:
    p = (phone or "").strip().replace(" ", "").replace("-", "")
    if p.startswith("+"):
        return p
    if p.startswith("212"):
        return "+" + p
    if p.startswith("0") and len(p) >= 10:
        return "+212" + p[1:]
    return p

def notify_user(
    db: Session,
    recipient: models.User,
    title: str,
    message: str,
    notif_type: models.NotificationType = models.NotificationType.ALERT,
    link: Optional[str] = None,
    send_sms_flag: bool = True,
):
    # 1) DB notification
    n = models.Notification(
        recipient_id=recipient.id,
        title=title,
        message=message,
        type=notif_type,
        link=link,
        is_read=False,
    )
    db.add(n)
    db.commit()
    db.refresh(n)

    # 2) SMS (optionnel)
    if send_sms_flag and getattr(recipient, "phone", None):
        phone = _normalize_phone_ma(recipient.phone)
        sms_text = f"{title}\n{message}"
       

    return n
