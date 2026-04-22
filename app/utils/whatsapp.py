import httpx, os
from fastapi import BackgroundTasks

WASENDER_URL = "https://www.wasenderapi.com/api/send-message"

def _build_message(module: str, status_text: str, details: dict, link: str = None) -> str:
    lines = [
        f"*SIB Portal — {module} Notification*",
        f"Status: {status_text}",
    ]
    field_map = [
        ("id", "Reference"),
        ("project", "Project"),
        ("pm", "Project Manager"),
        ("creator", "Created By"),
        ("date", "Date"),
        ("beneficiary", "Beneficiary"),
        ("category", "Category"),
        ("total", "Total"),
        ("description", "Description"),
        ("remark", "Remark"),
    ]
    for key, label in field_map:
        val = details.get(key)
        if val:
            lines.append(f"{label}: {val}")
    if link:
        base = os.getenv("FRONTEND_URL", "https://po.sib.co.ma")
        lines.append(f"Portal: {base}{link}")
    return "\n".join(lines)

def send_whatsapp_notification(
    to_numbers: list[str],
    module: str,
    status_text: str,
    details: dict,
    background_tasks: BackgroundTasks,
    link: str = None,
):
    api_key = os.getenv("WASENDER_API_KEY")
    valid_numbers = [n for n in to_numbers if n]
    if not api_key or not valid_numbers:
        return

    message = _build_message(module, status_text, details, link)

    def _send():
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        with httpx.Client(timeout=10) as client:
            for number in valid_numbers:
                client.post(WASENDER_URL, json={"to": number, "text": message}, headers=headers)

    background_tasks.add_task(_send)