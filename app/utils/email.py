# In utils/email.py or similar

from typing import List
from fastapi import BackgroundTasks
from fastapi_mail import MessageSchema, MessageType
from ..config import conf 
from fastapi_mail import FastMail, MessageSchema, MessageType


async def send_bc_status_email(bc, recipient_email, status, background_tasks):
    if not recipient_email: return

    subject = f"SIB Portal - BC {bc.bc_number} Update: {status}"
    
    html = f"""
    <h3>Bon de Commande Update</h3>
    <p>The BC <strong>{bc.bc_number}</strong> has moved to status: <strong>{status}</strong>.</p>
    <p>Project: {bc.internal_project.name}</p>
    <p>Amount: {bc.total_amount_ht:,.2f} MAD</p>
    <br>
    <p>Please log in to the portal to view details.</p>
    """

    message = MessageSchema(
        subject=subject,
        recipients=[recipient_email],
        body=html,
        subtype=MessageType.html
    )

    fm = FastMail(conf)
    background_tasks.add_task(fm.send_message, message)
def send_email_background(
    background_tasks: BackgroundTasks,
    subject: str,
    email_to: List[str],
    body: str
):
    message = MessageSchema(
        subject=subject,
        recipients=email_to,
        body=body,
        subtype=MessageType.html
    )
    
    fm = FastMail(conf)
    background_tasks.add_task(fm.send_message, message)
