# In utils/email.py or similar

from typing import List
from fastapi import BackgroundTasks
from fastapi_mail import MessageSchema, MessageType
from ..config import conf 
from fastapi_mail import FastMail, MessageSchema, MessageType
from xml.sax.saxutils import escape
import os

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



# Define your public logo URLs here
# LOGO_BASE_URL = "https://sib.co.ma/uploads/emails" # Adjust to your real URL
LOGO_BASE_URL = "uploads/emails" # Adjust to your real URL

LOGOS = {
    "SYSTEM": f"{LOGO_BASE_URL}/sib_logo.png",
    "EXP": f"{LOGO_BASE_URL}/iExpense.png",
    "BC": f"{LOGO_BASE_URL}/iPo.png",
    "ACCEPTANCE": f"{LOGO_BASE_URL}/iAcceptance.png",
    "CAISSE": f"{LOGO_BASE_URL}/iExpense.png", # Caisse usually uses iExpense branding
    "LOGISTIC": f"{LOGO_BASE_URL}/iLogistic.png"
}

IMAGE_DIR = os.path.abspath("uploads/emails") 

def send_notification_email(
    background_tasks: BackgroundTasks,
    recipients: List[str],
    subject: str,
    module: str, # "EXP", "BC", "ACCEPTANCE", "CAISSE", "LOGISTIC"
    status_text: str,
    details: dict,
    link: str = None
):
    if not recipients:
        return

    # 1. Map module names to actual filenames on your disk
    module_filenames = {
        "EXP": "iExpense.png",
        "BC": "iPo.png",
        "ACCEPTANCE": "iAcceptance.png",
        "CAISSE": "iExpense.png",
        "LOGISTIC": "iLogistic.png",
        "SYSTEM": "sib_logo.png"
    }
    
    module_file = module_filenames.get(module, "sib_logo.png")
    
    # 2. Define the local file paths for attachments
    sib_logo_path = os.path.join(IMAGE_DIR, "sib_logo.png")
    module_logo_path = os.path.join(IMAGE_DIR, module_file)

    # Table Row Helper (Internal to the function)
    def row(label, value):
        return f"""
        <tr>
            <td style="padding: 6px 10px; border: 1px solid #333; background-color: #d9e1f2; font-weight: bold; width: 30%; font-size: 13px;">{label}</td>
            <td style="padding: 6px 10px; border: 1px solid #333; font-size: 13px;">{str(value or "")}</td>
        </tr>
        """

    # 3. HTML Content using "cid:" references
    # cid:siblogo and cid:modulelogo match the headers we set below
    html_content = f"""
    <html>
        <body style="font-family: Arial, sans-serif; color: #000; margin: 0; padding: 20px;">
            <div style="max-width: 800px; margin: auto;">
                <table width="100%" style="border-collapse: collapse; border: 1px solid #333; text-align: center;">
                    <tr>
                        <td width="25%" style="border: 1px solid #333; padding: 10px;">
                            <img src="cid:siblogo" alt="SIB" style="max-height: 50px;">
                        </td>
                        <td width="50%" style="border: 1px solid #333; padding: 10px; font-size: 22px; color: #e46c0a; font-weight: bold;">
                            SIB Portal Notification
                        </td>
                        <td width="25%" style="border: 1px solid #333; padding: 10px;">
                            <img src="cid:modulelogo" alt="Module" style="max-height: 50px;">
                        </td>
                    </tr>
                    <tr>
                        <td colspan="3" style="background-color: #f7caac; border: 1px solid #333; padding: 5px; font-weight: bold; font-size: 14px;">
                            Status { "Expense" if module == "EXP" or module == "CAISSE" else "Document" } "{status_text}"
                        </td>
                    </tr>
                </table>

                <table width="100%" style="border-collapse: collapse; border: 1px solid #333; margin-top: -1px;">
                    {row("ID Expense:", details.get("id"))}
                    {row("Project:", details.get("project"))}
                    {row("Project Manager:", details.get("pm"))}
                    {row("The creator:", details.get("creator"))}
                    {row("Date Creation:", details.get("date"))}
                    {row("The Beneficiary:", details.get("beneficiary"))}
                    {row("Cost category:", details.get("category"))}
                    {row("Total:", details.get("total"))}
                    {row("Expense Description:", details.get("remark", details.get("description", "")))}
                </table>

                <div style="border: 1px solid #333; border-top: none; padding: 10px; font-size: 12px;">
                    <p style="margin: 0 0 5px 0;">Notes:</p>
                    <ol style="margin: 0; padding-left: 20px;">
                        <li>If payment has not been received, please check with the finance department.</li>
                        <li>Once received, please confirm it in the system to avoid blocking future requests.</li>
                    </ol>
                </div>

                <div style="text-align: center; margin-top: 20px;">
                    <a href="{os.getenv('FRONTEND_URL', 'http://localhost:3000')}{link}" 
                       style="background-color: #2e75b6; color: white; padding: 10px 25px; text-decoration: none; font-weight: bold; border-radius: 4px; display: inline-block;">
                       ACCESS THE PORTAL
                    </a>
                </div>
            </div>
        </body>
    </html>
    """

    # 4. Create the attachments list with Content-ID headers
    attachments = []
    if os.path.exists(sib_logo_path):
        attachments.append({
            "file": sib_logo_path,
            "headers": {"Content-ID": "<siblogo>"}, # The brackets < > are important
            "mime_type": "image",
            "mime_subtype": "png"
        })
    
    if os.path.exists(module_logo_path):
        attachments.append({
            "file": module_logo_path,
            "headers": {"Content-ID": "<modulelogo>"},
            "mime_type": "image",
            "mime_subtype": "png"
        })

    # 5. Build and Send
    message = MessageSchema(
        subject=f"SIB Portal: {subject}",
        recipients=recipients,
        body=html_content,
        subtype=MessageType.html,
        attachments=attachments # Pass the attachments here
    )

    fm = FastMail(conf)
    background_tasks.add_task(fm.send_message, message)
