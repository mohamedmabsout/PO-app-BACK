import os
import io
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm, mm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER
from xml.sax.saxutils import escape
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from fastapi import Depends
# Assuming you import your models to check the Enum
from .. import models , crud
from ..dependencies import get_db
from sqlalchemy.orm import object_session # <--- ADD THIS IMPORT

# SIB Legal Information constants
SIB_NAME = "SOLUTION INTEGRALE BUILDING (SIB) SARL"
SIB_ADDRESS = "57 RUE MOSTAPHA RAFII RES DES JARDINS IM 2 B 9 KENITRA"
SIB_RC = "42505/KENITRA"
SIB_ICE = "001704095000027"
SIB_IF = "29156258"
SIB_CNSS = "4312980"
SIB_WEB = "www.sib.co.ma"
def format_ice(val):
    """
    Cleans ICE from float-like strings (e.g. '123.0') 
    and pads with leading zeros to 15 digits.
    """
    if not val or val == "None" or val == "-":
        return "-"
    
    # 1. Convert to string
    s = str(val).strip()
    
    # 2. Remove .0 if it exists
    if s.endswith('.0'):
        s = s[:-2]
        
    # 3. Remove any non-digit characters (optional safety)
    import re
    s = re.sub(r'\D', '', s)
    
    # 4. Pad to 15 digits with leading zeros
    return s.zfill(15)


def generate_bc_pdf(bc):
    buffer = io.BytesIO()
    
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=10 * mm,
        leftMargin=10 * mm,
        topMargin=10 * mm,
        bottomMargin=20 * mm # Increased for footer
    )

    elements = []
    styles = getSampleStyleSheet()
    
    style_normal = styles["Normal"]
    style_normal.fontSize = 8
    style_normal.leading = 10
    
    style_bold = ParagraphStyle(
        'Bold', parent=styles['Normal'], fontSize=8, leading=10, fontName='Helvetica-Bold'
    )
    style_title = ParagraphStyle(
        'Title', parent=styles['Normal'], fontSize=16, leading=20, fontName='Helvetica-Bold', alignment=TA_CENTER
    )
    style_footer = ParagraphStyle(
        'Footer', parent=styles['Normal'], fontSize=7, leading=9, alignment=TA_CENTER, textColor=colors.grey
    )

    is_standard = (bc.bc_type == models.BCType.STANDARD)

    # ==========================
    # 1. HEADER SECTION
    # ==========================
    logo_path = "logo.png" 
    if is_standard:
        if os.path.exists(logo_path):
            logo = Image(logo_path, width=3.5*cm, height=1.5*cm)
        else:
            logo = Paragraph(f"<b>{SIB_NAME}</b>", style_bold)

        header_data = [
            [logo, Paragraph("<b>BON DE COMMANDE</b>", style_title)],
            ["", Paragraph(f"<b>N° BC:</b> {bc.bc_number}", style_bold)]
        ]
        
        t_header = Table(header_data, colWidths=[10*cm, 9*cm])
        t_header.setStyle(TableStyle([
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('ALIGN', (1,0), (1,0), 'RIGHT'),
            ('ALIGN', (1,1), (1,1), 'RIGHT'),
        ]))
        elements.append(t_header)
    else:
        elements.append(Spacer(1, 1*cm))
        elements.append(Paragraph("BON DE COMMANDE", style_title))
        elements.append(Spacer(1, 0.5*cm))
        elements.append(Paragraph(f"<b>N° BC:</b> {bc.bc_number}", style_bold))

    elements.append(Paragraph(f"<b>Date:</b> {bc.created_at.strftime('%d/%m/%Y')}", style_normal))
    elements.append(Spacer(1, 0.5*cm))

    # ==========================
    # 2. ADDRESS BLOCK
    # ==========================
    
    # Supplier Info
    sbc_info = [
        [Paragraph("<b>Fournisseur / Prestataire:</b>", style_bold)],
        [Paragraph(f"{bc.sbc.name}", style_normal)],
        [Paragraph(f"ICE: {format_ice(bc.sbc.ice)}", style_normal)], # <--- FIX HERE
        [Paragraph(f"Tél: {bc.sbc.phone_1 or '-'}", style_normal)],
        [Paragraph(f"Email: {bc.sbc.email or '-'}", style_normal)],
    ]

    # SIB Info (Address included)
    sib_bill_info = [
        [Paragraph("<b>Client / Facturer à:</b>", style_bold)],
        [Paragraph(SIB_NAME, style_normal)],
        [Paragraph(SIB_ADDRESS, style_normal)],
        [Paragraph(f"ICE: {SIB_ICE}", style_normal)],
        [Paragraph(f"Projet: {bc.internal_project.name}", style_normal)],
    ]

    address_table_data = [[
        Table(sbc_info, colWidths=[9*cm]),
        Table(sib_bill_info, colWidths=[9*cm])
    ]]
    
    t_address = Table(address_table_data, colWidths=[9.5*cm, 9.5*cm])
    t_address.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LINEBELOW', (0,0), (-1,-1), 0.5, colors.lightgrey),
    ]))
    elements.append(t_address)
    elements.append(Spacer(1, 0.5*cm))

    # ==========================
    # 3. MAIN ITEMS TABLE
    # ==========================
    if is_standard:
        table_headers = ["L.", "Site Code", "Description", "UOM", "P.U (HT)", "Qty", "Total HT", "TVA"]
        col_widths = [0.6*cm, 2.5*cm, 6.2*cm, 0.9*cm, 2.0*cm, 0.8*cm, 2.0*cm, 1.0*cm]
    else:
        table_headers = ["L.", "Site Code", "Description", "UOM", "P.U", "Qty", "Total"]
        col_widths = [0.8*cm, 2.5*cm, 7.5*cm, 1.0*cm, 2.0*cm, 1.0*cm, 2.2*cm]

    data = [table_headers]
    for idx, item in enumerate(bc.items):
        row = [
            str(idx + 1),
            Paragraph(escape(item.merged_po.site_code or "-"), style_normal),
            Paragraph(escape(item.merged_po.item_description or "-"), style_normal),
            "Unit",
            f"{item.unit_price_sbc:,.2f}",
            str(item.quantity_sbc),
            f"{item.line_amount_sbc:,.2f}",
        ]
        if is_standard:
            row.append(f"{int(item.applied_tax_rate * 100)}%")
        data.append(row)

    t_items = Table(data, colWidths=col_widths, repeatRows=1)
    t_items.setStyle(TableStyle([
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 7),
        ('BACKGROUND', (0,0), (-1,0), colors.whitesmoke),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('ALIGN', (4,0), (-1,-1), 'RIGHT'), 
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ]))
    elements.append(t_items)
    elements.append(Spacer(1, 0.5*cm))

    # ==========================
    # 4. TOTALS
    # ==========================
    if is_standard:
        totals_data = [
            # ["Total HT:", f"{bc.total_amount_ht:,.2f} MAD"],
            # ["Total TVA:", f"{bc.total_tax_amount:,.2f} MAD"],
            [Paragraph("<b>TOTAL HT:</b>", style_bold), Paragraph(f"<b>{bc.total_amount_ht:,.2f} MAD</b>", style_bold)]
        ]
    else:
        totals_data = [["Net à Payer:", f"{bc.total_amount_ht:,.2f} MAD"]]
    
    t_totals = Table(totals_data, colWidths=[4*cm, 4*cm])
    t_totals.setStyle(TableStyle([
        ('ALIGN', (0,0), (-1,-1), 'RIGHT'),
        ('GRID', (0,2), (-1,2), 1, colors.black) if is_standard else ('LINEBELOW', (0,0), (-1,-1), 0.5, colors.grey),
    ]))
    
    elements.append(Table([[None, t_totals]], colWidths=[11*cm, 8*cm]))
    elements.append(Spacer(1, 1*cm))

    # ==========================
    # 5. UPDATED NOTES
    # ==========================
    notes_header = Paragraph("<b>Conditions Générales:</b>", style_bold)
    elements.append(notes_header)
    
    # 1. Framework note
    # 2. NEW: 3 Months Cancellation note
    notes_body = """
    1. Ce bon de commande est régi par les termes du contrat cadre signé entre SIB et le Prestataire.<br/>
    2. SIB se réserve le droit d'annuler toute ligne de commande ou partie de celle-ci si les travaux ne sont pas achevés, réceptionnés et validés dans un délai de 3 mois à compter de la date d'émission du BC.
    """
    elements.append(Paragraph(notes_body, style_normal))
    elements.append(Spacer(1, 2*cm))

    # ==========================
    # 6. SIGNATURE BLOCK
    # ==========================
    sig_data = [[Paragraph("<b>Cachet et Signature Fournisseur</b>", style_normal), 
                 Paragraph("<b>Visa SIB (Direction de Projet)</b>", style_normal)]]
    t_sig = Table(sig_data, colWidths=[9.5*cm, 9.5*cm])
    t_sig.setStyle(TableStyle([('ALIGN', (0,0), (-1,-1), 'CENTER'), ('BOTTOMPADDING', (0,0), (-1,-1), 50)]))
    elements.append(t_sig)

    # ==========================
    # 7. SIB FULL FOOTER
    # ==========================
    def add_footer(canvas, doc):
        canvas.saveState()
        footer_text = f"{SIB_NAME} - {SIB_ADDRESS}<br/>" \
                      f"RC: {SIB_RC} | IF: {SIB_IF} | ICE: {SIB_ICE} | CNSS: {SIB_CNSS} | Web: {SIB_WEB}"
        p_footer = Paragraph(footer_text, style_footer)
        w, h = p_footer.wrap(doc.width, doc.bottomMargin)
        p_footer.drawOn(canvas, doc.leftMargin, 10 * mm)
        canvas.restoreState()

    # Build PDF
    doc.build(elements, onFirstPage=add_footer, onLaterPages=add_footer)
    buffer.seek(0)
    return buffer


def format_currency(value):
    return f"{value:,.2f} MAD"

def generate_act_pdf(act):
    buffer = io.BytesIO()
    
    # 1. Setup Document
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=10 * mm,
        leftMargin=10 * mm,
        topMargin=10 * mm,
        bottomMargin=20 * mm
    )

    elements = []
    styles = getSampleStyleSheet()
    
    # Custom Styles
    style_normal = styles["Normal"]
    style_normal.fontSize = 8
    
    style_bold = ParagraphStyle(
        'Bold', parent=styles['Normal'], fontSize=8, fontName='Helvetica-Bold'
    )
    style_title = ParagraphStyle(
        'Title', parent=styles['Normal'], fontSize=16, leading=20, fontName='Helvetica-Bold', alignment=TA_CENTER
    )
    style_footer = ParagraphStyle(
        'Footer', parent=styles['Normal'], fontSize=7, alignment=TA_CENTER, textColor=colors.grey
    )

    # ==========================
    # 1. HEADER SECTION
    # ==========================
    logo_path = "logo.png" 
    if os.path.exists(logo_path):
        logo = Image(logo_path, width=3.5*cm, height=1.5*cm)
    else:
        logo = Paragraph(f"<b>{SIB_NAME}</b>", style_bold)

    header_data = [
        [logo, Paragraph("<b>SERVICE ACCEPTANCE CERTIFICATE</b>", style_title)],
        ["", Paragraph(f"<b>ACT Number:</b> {act.act_number}", style_bold)],
        ["", Paragraph(f"<b>Date:</b> {act.created_at.strftime('%Y-%m-%d')}", style_normal)],
        ["", Paragraph(f"<b>BC Reference:</b> {act.bc.bc_number}", style_normal)]
    ]
    
    t_header = Table(header_data, colWidths=[10*cm, 9*cm])
    t_header.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('ALIGN', (1,0), (1,-1), 'RIGHT'),
    ]))
    elements.append(t_header)
    elements.append(Spacer(1, 1*cm))

    # ==========================
    # 2. ITEMS TABLE (With Site Code & Wrapping)
    # ==========================
    # Table Columns: Site, Description, Qty, Price, Total
    headers = [
        Paragraph("<b>Site Code</b>", style_bold),
        Paragraph("<b>Item Description</b>", style_bold),
        Paragraph("<b>Qty</b>", style_bold),
        Paragraph("<b>Unit Price</b>", style_bold),
        Paragraph("<b>Total</b>", style_bold)
    ]
    
    data = [headers]
    
    for item in act.items:
        # We use Paragraph for the description to allow automatic text wrapping
        desc_wrapped = Paragraph(escape(item.merged_po.item_description or "-"), style_normal)
        
        row = [
            Paragraph(escape(item.merged_po.site_code or "-"), style_normal),
            desc_wrapped,
            f"{item.quantity_sbc:.2f}",
            format_currency(item.unit_price_sbc),
            format_currency(item.line_amount_sbc)
        ]
        data.append(row)

    # Define specific column widths (Description gets the most space)
    # Total width is roughly 19cm
    col_widths = [4.5*cm, 7.5*cm, 1.5*cm, 3*cm, 3.5*cm]
    
    t_items = Table(data, colWidths=col_widths, repeatRows=1)
    t_items.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.whitesmoke),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('ALIGN', (2,0), (-1,-1), 'RIGHT'), # Align numbers to the right
        ('FONTSIZE', (0,0), (-1,-1), 8),
    ]))
    elements.append(t_items)

    # ==========================
    # 3. TOTALS SECTION
    # ==========================
    totals_data = [
        [Paragraph("<b>TOTAL HT:</b>", style_bold), Paragraph(f"<b>{format_currency(act.total_amount_ht)}</b>", style_bold)]
    ]
    
    t_totals = Table(totals_data, colWidths=[3*cm, 4*cm])
    t_totals.setStyle(TableStyle([
        ('ALIGN', (0,0), (-1,-1), 'RIGHT'),
        ('LINEABOVE', (0,0), (-1,-1), 1, colors.black),
        ('TOPPADDING', (0,0), (-1,-1), 5),
    ]))
    
    # Push totals to the right
    elements.append(Spacer(1, 0.5*cm))
    elements.append(Table([[None, t_totals]], colWidths=[12*cm, 7*cm]))

    # ==========================
    # 4. SIB FULL FOOTER
    # ==========================
    def add_footer(canvas, doc):
        canvas.saveState()
        footer_text = f"{SIB_NAME} - {SIB_ADDRESS}<br/>" \
                      f"RC: {SIB_RC} | IF: {SIB_IF} | ICE: {SIB_ICE} | CNSS: {SIB_CNSS} | Web: {SIB_WEB}"
        p_footer = Paragraph(footer_text, style_footer)
        # Use document width and wrap in bottom margin
        w, h = p_footer.wrap(doc.width, doc.bottomMargin)
        p_footer.drawOn(canvas, doc.leftMargin, 10 * mm)
        canvas.restoreState()

    # 5. Build PDF
    doc.build(elements, onFirstPage=add_footer, onLaterPages=add_footer)
    
    buffer.seek(0)
    return buffer


def generate_expense_pdf(expense):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    elements = []
    styles = getSampleStyleSheet()
    # Define a custom style for table cells (smaller font if needed)
    style_cell = ParagraphStyle('CellStyle', parent=styles['Normal'], fontSize=9, leading=11)
    style_cell_bold = ParagraphStyle('CellStyleBold', parent=styles['Normal'], fontSize=9, leading=11, fontName='Helvetica-Bold')
    # Title
    elements.append(Paragraph(f"PIECE DE CAISSE (Expense) - {expense.id}", styles['Title']))
    elements.append(Spacer(1, 0.5*cm))
    project_name_para = Paragraph(expense.internal_project.name, style_cell_bold)
    beneficiary_para = Paragraph(expense.beneficiary, style_cell_bold)
    act_numbers = ", ".join([act.act_number for act in expense.acts])

    # Info Table
    data = [
        ["Date Request:", expense.created_at.strftime("%d/%m/%Y"), "Project:", expense.internal_project.name],
        ["Type:", expense.exp_type, "ACT References:", Paragraph(act_numbers, styles['Normal'])],
        ["Beneficiary:", expense.beneficiary, "Total Amount:", f"{expense.amount:,.2f} MAD"]
    ]
    t = Table(data, colWidths=[3*cm, 3.5*cm, 2.5*cm, 9.5*cm])
    t.setStyle(TableStyle([
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('VALIGN', (0,0), (-1,-1), 'TOP'), # Align to top so wrapped text looks good
        ('PADDING', (0,0), (-1,-1), 6),    # Add padding
    ]))
    elements.append(t)
    elements.append(Spacer(1, 1*cm))

    # Remarks
    elements.append(Paragraph(f"<b>Remark:</b> {expense.remark or 'N/A'}", styles['Normal']))
    elements.append(Spacer(1, 2*cm))

    # SIGNATURE BLOCKS
    sig_data = [
        ["Requester (PM)", "Approval (PD/Admin)", "Beneficiary (Received By)"],
        ["\n\n_________________\n" + expense.requester.first_name, 
         "\n\n_________________\nValidated", 
         "\n\n_________________\nSignature & Date"]
    ]
    sig_table = Table(sig_data, colWidths=[6*cm, 6*cm, 6*cm])
    sig_table.setStyle(TableStyle([
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('LINEABOVE', (0,1), (-1,1), 0.5, colors.black),
    ]))
    elements.append(sig_table)

    doc.build(elements)
    buffer.seek(0)
    return buffer


def generate_invoice_pdf(invoice):
    """Generate PDF for a given invoice."""
    db = object_session(invoice)

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=10*mm, leftMargin=10*mm, topMargin=10*mm, bottomMargin=10*mm)
    elements = []
    styles = getSampleStyleSheet()
    style_bold = ParagraphStyle(
        'Bold', parent=styles['Normal'], fontSize=8, fontName='Helvetica-Bold'
    )
    # Custom Styles
    style_h = ParagraphStyle('Header', parent=styles['Normal'], fontSize=8, leading=10)
    style_title = ParagraphStyle('Title', parent=styles['Normal'], fontSize=16, leading=20, fontName='Helvetica-Bold', alignment=TA_CENTER)
    supplier_ice = format_ice(invoice.sbc.ice)

    # --- 1. TOP HEADER (Bill To vs Supplier) ---
    bill_to = [
        [
            Paragraph(f"<b>Bill To:</b> {escape(SIB_NAME)}", style_h), 
            Paragraph(f"<b>Supplier:</b> {escape(invoice.sbc.name or 'Unknown SBC')}", style_h)
        ],
        [
            Paragraph(f"SIB Tax ID: {escape(SIB_IF)}", style_h), 
            Paragraph(f"Supplier Tel: {escape(str(invoice.sbc.phone_1 or '-'))}", style_h)
        ],
        [
            Paragraph(f"Address: {escape(SIB_ADDRESS)}", style_h), 
            # Use 'or "-"' to prevent the NoneType crash
            Paragraph(f"Address: {escape(invoice.sbc.address or '-')}", style_h)
        ],
        [
            Paragraph(f"SIB ice: {escape(SIB_ICE)}", style_h), 
            # THE FIX FOR YOUR SPECIFIC ERROR:
            Paragraph(f"Bank: {escape(invoice.sbc.bank_name or '-')}", style_h) 
        ],
        [
            Paragraph("", style_h), 
            Paragraph(f"RIB: {escape(invoice.sbc.rib or '-')}", style_h)
        ],
        [
            Paragraph("", style_h), 
            Paragraph(f"Supplier ICE: {supplier_ice}", style_h) # Cleaned variable
        ],
    ]
    t_top = Table(bill_to, colWidths=[9.5*cm, 9.5*cm])
    elements.append(t_top)
    elements.append(Spacer(1, 1*cm))
    
    elements.append(Paragraph("Invoice / Tax Invoice", style_title))
    elements.append(Spacer(1, 0.5*cm))

    # --- 2. INVOICE INFO ---
    inv_info = [
        [f"Invoice NO: {invoice.invoice_number}"],
        [f"PO Type: {invoice.category.upper()}"],
        [f"Invoice Date: {invoice.created_at.strftime('%Y-%m-%d')}"]
    ]
    t_info = Table(inv_info, colWidths=[19*cm])
    t_info.setStyle(TableStyle([('ALIGN', (0,0), (-1,-1), 'CENTER')]))
    elements.append(t_info)
    elements.append(Spacer(1, 0.5*cm))

    # --- 3. ITEMS TABLE (Aggregated from all ACTs) ---
    headers = ["BC Num", "BC Line", "DUID", "Item Description", "Qty", "Tax Rate", "Unit Price", "Amount (HT)"]

    # 2. Distribute the 19cm available width (A4 is 21cm - 2cm margins)
    # Values are in cm. Adjust according to your needs.
    col_widths = [
        2.2 * cm,  # BC Num
        1.2 * cm,  # PO Line
        3.8 * cm,  # DUID (Increased for values like DC-RABAT...)
        5.5 * cm,  # Item Description (The largest one)
        0.8 * cm,  # Qty
        1.2 * cm,  # Tax Rate
        2.15 * cm, # Unit Price
        2.15 * cm  # Amount (HT)
    ]

    data = [headers]

    # 3. Populate data
    for act in invoice.acts:
        # We want to find the sequence of the items within their BC.
        # Since act.items are the items specifically accepted in this ACT, 
        # we can use their original po_line_no or a relative counter.
        # Most users prefer the actual line number from the printed BC.
        
        for item in act.items:
            data.append([
                act.bc.bc_number,
                item.merged_po.po_line_no, # This is the line ID from the BC/PO
                Paragraph(escape(item.merged_po.site_code or "-"), style_h),
                Paragraph(escape(item.merged_po.item_description or "-"), style_h),
                item.quantity_sbc,
                f"{int(item.applied_tax_rate * 100)}%",
                f"{item.unit_price_sbc:,.2f}",
                f"{item.line_amount_sbc:,.2f}"
            ])


    # 4. Create the table with the new colWidths
    t_items = Table(data, colWidths=col_widths, repeatRows=1)

    # 5. Update TableStyle coordinates
    # Since we added a column, the alignment index for numbers might shift.
    # ('ALIGN', (4,0), (-1,-1), 'RIGHT') means from column 4 (Qty) to the end.
    t_items.setStyle(TableStyle([
        ('GRID', (0,0), (-1,-1), 0.5, colors.black),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('BACKGROUND', (0,0), (-1,0), colors.whitesmoke),
        ('FONTSIZE', (0,0), (-1,-1), 7),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),    # Top align text for multi-line rows
        ('ALIGN', (4,0), (-1,-1), 'RIGHT'),  # Align numbers (Qty, Price, Total) to right
    ]))

    elements.append(t_items)
    adv_balance = crud.get_sbc_unconsumed_balance(db, invoice.sbc_id)

    # --- 4. TOTALS ---
    totals = [
        ["", "Invoice Amount (Excl. Tax):", f"{invoice.total_amount_ht:,.2f}"],
        ["", f"TVA {int((invoice.total_tax_amount/invoice.total_amount_ht)*100) if invoice.total_amount_ht > 0 else 20}%:", f"{invoice.total_tax_amount:,.2f}"],
        ["", "Total Amount (Incl. Tax):", f"{invoice.total_amount_ttc:,.2f} MAD"]
    ]
    if adv_balance > 0:
        net_to_pay = max(0, invoice.total_amount_ttc - adv_balance)
        totals.append(["Less Advances Received:", f"- {adv_balance:,.2f} MAD"])
        totals.append([Paragraph("<b>NET TO PAY:</b>", style_bold), 
                            Paragraph(f"<b>{net_to_pay:,.2f} MAD</b>", style_bold)])

    t_tot = Table(totals, colWidths=[10*cm, 6*cm, 3*cm])
    t_tot.setStyle(TableStyle([('FONTNAME', (1,2), (2,2), 'Helvetica-Bold'), ('ALIGN', (1,0), (2,2), 'RIGHT')]))
    elements.append(t_tot)
    
    doc.build(elements)
    buffer.seek(0)
    return buffer
