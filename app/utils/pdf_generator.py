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

# Assuming you import your models to check the Enum
from .. import models 

# SIB Legal Information constants
SIB_NAME = "SOLUTION INTEGRALE BUILDING (SIB) SARL AU"
SIB_ADDRESS = "Lotissement Mandarona n°142, Sidi Maârouf, Casablanca, Maroc"
SIB_RC = "373413"
SIB_ICE = "001529147000078"
SIB_IF = "20735311"
SIB_CNSS = "4945532"
SIB_WEB = "www.sib.co.ma"

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
        [Paragraph(f"ICE: {bc.sbc.ice or '-'}", style_normal)],
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
            ["Total HT:", f"{bc.total_amount_ht:,.2f} MAD"],
            ["Total TVA:", f"{bc.total_tax_amount:,.2f} MAD"],
            [Paragraph("<b>TOTAL TTC:</b>", style_bold), Paragraph(f"<b>{bc.total_amount_ttc:,.2f} MAD</b>", style_bold)]
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
    """Helper to format currency with 'MAD' suffix"""
    return f"{value:,.2f} MAD"

def generate_act_pdf(act):
    buffer = io.BytesIO()
    p = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    
    # --- HEADER ---
    p.setFont("Helvetica-Bold", 16)
    p.drawString(50, height - 50, "Service Acceptance Certificate")
    
    p.setFont("Helvetica", 10)
    p.drawString(50, height - 80, f"ACT Number: {act.act_number}")
    p.drawString(50, height - 95, f"Date: {act.created_at.strftime('%Y-%m-%d')}")
    p.drawString(50, height - 110, f"BC Reference: {act.bc.bc_number}")
    
    # --- TABLE HEADER ---
    y = height - 160
    p.setFillColorRGB(0.9, 0.9, 0.9) # Light gray background
    p.rect(40, y-5, 515, 20, fill=1, stroke=0)
    p.setFillColorRGB(0, 0, 0) # Back to black text

    p.setFont("Helvetica-Bold", 9)
    p.drawString(50, y, "Item Description")
    p.drawRightString(380, y, "Qty")       # Right-aligned
    p.drawRightString(460, y, "Unit Price") # Right-aligned
    p.drawRightString(540, y, "Total")      # Right-aligned
    
    # --- ITEMS ---
    y -= 25
    p.setFont("Helvetica", 9)
    
    for item in act.items:
        # Simple text truncation
        desc = item.merged_po.item_description
        if len(desc) > 60: desc = desc[:57] + "..."
            
        p.drawString(50, y, desc)
        p.drawRightString(380, y, f"{item.quantity_sbc:.2f}")
        p.drawRightString(460, y, format_currency(item.unit_price_sbc))
        p.drawRightString(540, y, format_currency(item.line_amount_sbc))
        
        y -= 20
        
        if y < 100: # New page logic
            p.showPage()
            y = height - 50

    # --- FOOTER (TOTALS) ---
    y -= 20
    p.setLineWidth(1)
    p.line(40, y+10, 555, y+10)
    
    p.setFont("Helvetica-Bold", 10)
    
    # Total HT
    y -= 20
    p.drawString(380, y, "Total HT:")
    p.drawRightString(540, y, format_currency(act.total_amount_ht))
    
    # Tax
    y -= 20
    tax_percent = (act.applied_tax_rate or 0.0) * 100
    p.drawString(380, y, f"Tax ({tax_percent:.0f}%):")
    p.drawRightString(540, y, format_currency(act.total_tax_amount))
    
    # Total TTC
    y -= 25
    p.setFont("Helvetica-Bold", 12)
    p.drawString(380, y, "Total TTC:")
    p.drawRightString(540, y, format_currency(act.total_amount_ttc))
    
    p.showPage()
    p.save()
    
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
