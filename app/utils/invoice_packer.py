import zipfile
import io
from .pdf_generator import generate_invoice_pdf, generate_bc_pdf, generate_act_pdf
from .. import crud
def create_invoice_zip(invoice):
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        # 1. The Main Invoice
        inv_pdf = generate_invoice_pdf(invoice)
        zip_file.writestr(f"INVOICE_{invoice.invoice_number}.pdf", inv_pdf.getvalue())
        excel_data = crud.generate_invoice_excel_bytes(invoice)
        zip_file.writestr(f"DATA_DETAILS_{invoice.invoice_number}.xlsx", excel_data)
        
        # 3. Add all related ACT PDFs
        added_bcs = set()
        for act in invoice.acts:
            # Add ACT PDF
            act_pdf = generate_act_pdf(act)
            zip_file.writestr(f"Acceptances/ACT_{act.act_number}.pdf", act_pdf.getvalue())
            
            # Add unique BC PDF
            if act.bc_id not in added_bcs:
                bc_pdf = generate_bc_pdf(act.bc)
                zip_file.writestr(f"Purchase_Orders/BC_{act.bc.bc_number}.pdf", bc_pdf.getvalue())
                added_bcs.add(act.bc_id)
                
    zip_buffer.seek(0)
    return zip_buffer
