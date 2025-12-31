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

def generate_bc_pdf(bc):
    # 1. Setup Buffer (Use BytesIO instead of writing to disk for cleaner API response)
    buffer = io.BytesIO()
    
    # 2. Document Setup
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=10 * mm,
        leftMargin=10 * mm,
        topMargin=10 * mm,
        bottomMargin=10 * mm
    )

    elements = []
    styles = getSampleStyleSheet()
    
    # Custom Styles
    style_normal = styles["Normal"]
    style_normal.fontSize = 8
    style_normal.leading = 10
    
    style_bold = ParagraphStyle(
        'Bold', parent=styles['Normal'], fontSize=8, leading=10, fontName='Helvetica-Bold'
    )
    style_title = ParagraphStyle(
        'Title', parent=styles['Normal'], fontSize=16, leading=20, fontName='Helvetica-Bold', alignment=TA_CENTER
    )

    # --- CHECK BC TYPE ---
    is_standard = (bc.bc_type == models.BCType.STANDARD)

    # ==========================
    # 1. HEADER SECTION
    # ==========================
    if is_standard:
        # --- STANDARD HEADER (With Logo & Company Info) ---
        logo_path = "logo.png" 
        if os.path.exists(logo_path):
            logo = Image(logo_path, width=4*cm, height=2*cm)
        else:
            logo = Paragraph("<b>[SIB LOGO]</b>", style_title)

        header_data = [
            [logo, Paragraph("<b>BON DE COMMANDE</b>", style_title)],
            [Paragraph(f"<b>N° BC:</b> {bc.bc_number}", style_bold)]
        ]
        
        t_header = Table(header_data, colWidths=[10*cm, 9*cm])
        t_header.setStyle(TableStyle([
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('ALIGN', (1,0), (1,0), 'RIGHT'),
            ('ALIGN', (1,1), (1,1), 'RIGHT'),
            ('BOTTOMPADDING', (0,0), (-1,-1), 10),
        ]))
        elements.append(t_header)
    else:
        # --- PERSONNE PHYSIQUE HEADER (Minimal / Anonymous) ---
        # No Logo. Just the Title.
        elements.append(Spacer(1, 1*cm))
        elements.append(Paragraph("BON DE COMMANDE", style_title))
        elements.append(Spacer(1, 0.5*cm))
        elements.append(Paragraph(f"<b>N° BC:</b> {bc.bc_number}", style_bold))
        elements.append(Paragraph(f"<b>Date:</b> {bc.created_at.strftime('%d/%m/%Y')}", style_normal))

    elements.append(Spacer(1, 0.5*cm))


    # ==========================
    # 2. ADDRESS BLOCK
    # ==========================
    
    # Supplier Info (Always shown)
    sbc_info = [
        [Paragraph("<b>Fournisseur / Prestataire:</b>", style_bold)],
        [Paragraph(f"<b>Nom:</b> {bc.sbc.name}", style_normal)],
        [Paragraph(f"<b>Contact:</b> {bc.sbc.phone_1 or '-'}", style_normal)],
    ]

    # Bill To Info (Conditioned)
    bill_to_info = []
    if is_standard:
        bill_to_info = [
            [Paragraph("<b>Facturer à:</b>", style_bold)],
            [Paragraph("SOLUTION INTEGRALE BUILDING (SIB)", style_normal)],
            [Paragraph("123 Main St, Casablanca, Morocco", style_normal)],
            [Paragraph("ICE: 0011223344", style_normal)],
        ]
    else:
        # For Personne Physique, we might want to hide the "Bill To" or show minimal info
        # Or just show project info
        bill_to_info = [
            [Paragraph("<b>Détails Projet:</b>", style_bold)],
            [Paragraph(f"{bc.internal_project.name}", style_normal)],
        ]

    # Combine into a 2-column table
    address_table_data = [[
        Table(sbc_info, colWidths=[9*cm]),
        Table(bill_to_info, colWidths=[9*cm])
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
    
    # Define Columns based on Type
    if is_standard:
        # Standard: Include Tax column
        table_headers = ["Line", "Site Code", "Description", "UOM", "Unit Price", "Qty", "Total HT", "Tax"]
        col_widths = [0.8*cm, 2.5*cm, 6.0*cm, 1.0*cm, 2.0*cm, 1.0*cm, 2.2*cm, 1.5*cm]
    else:
        # PP: Remove Tax column, expand Description
        table_headers = ["Line", "Site Code", "Description", "UOM", "Unit Price", "Qty", "Total"]
        col_widths = [0.8*cm, 2.5*cm, 7.5*cm, 1.0*cm, 2.0*cm, 1.0*cm, 2.2*cm]

    data = [table_headers]
    
    for idx, item in enumerate(bc.items):
        raw_desc = item.merged_po.item_description or ""
        clean_desc = escape(raw_desc)
        raw_site = item.merged_po.site_code or ""
        clean_site = escape(raw_site)
        
        row = [
            str(idx + 1),
            Paragraph(clean_site, style_normal),
            Paragraph(clean_desc, style_normal),
            "Unit",
            f"{item.unit_price_sbc:,.2f}",
            str(item.quantity_sbc),
            f"{item.line_amount_sbc:,.2f}",
        ]
        
        # Add Tax column only for Standard
        if is_standard:
            row.append(f"{int(item.applied_tax_rate * 100)}%")
            
        data.append(row)

    t_items = Table(data, colWidths=col_widths, repeatRows=1)
    
    t_items.setStyle(TableStyle([
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 7),
        ('BACKGROUND', (0,0), (-1,0), colors.whitesmoke),
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
        # Adjust alignment for numbers
        ('ALIGN', (4,0), (-1,-1), 'RIGHT'), 
        ('GRID', (0,0), (-1,-1), 0.5, colors.black),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 2),
        ('RIGHTPADDING', (0,0), (-1,-1), 2),
    ]))
    
    elements.append(t_items)
    elements.append(Spacer(1, 0.5*cm))


    # ==========================
    # 4. TOTALS SECTION
    # ==========================
    
    totals_data = []
    
    if is_standard:
        totals_data = [
            ["Total HT:", f"{bc.total_amount_ht:,.2f}"],
            ["Total TVA:", f"{bc.total_tax_amount:,.2f}"],
            ["Total TTC:", f"{bc.total_amount_ttc:,.2f} MAD"]
        ]
    else:
        # Only show the Net Pay for PP
        totals_data = [
            ["Net à Payer:", f"{bc.total_amount_ht:,.2f} MAD"]
        ]
    
    t_totals = Table(totals_data, colWidths=[5*cm, 3*cm])
    t_totals.setStyle(TableStyle([
        ('FONTNAME', (0,0), (-1,-1), 'Helvetica-Bold'),
        ('ALIGN', (0,0), (0,-1), 'RIGHT'),
        ('ALIGN', (1,0), (1,-1), 'RIGHT'),
        ('LINEBELOW', (0,0), (-1,-1), 0.5, colors.lightgrey),
    ]))
    
    # Push to right using a container table
    t_totals_container = Table([[None, t_totals]], colWidths=[11*cm, 8*cm])
    elements.append(t_totals_container)
    elements.append(Spacer(1, 1*cm))


    # ==========================
    # 5. FOOTER & NOTES
    # ==========================
    
    # Only show legal notes if it's a Standard BC
    if is_standard:
        notes_text = """
        <b>Notes:</b><br/>
        1. This Purchase Order is governed by the agreements executed between the Supplier and SIB.<br/>
        2. Payment terms as agreed in the Framework Contract.
        """
        elements.append(Paragraph(notes_text, style_normal))
        
        # Footer
        # (Usually handled by canvas.saveState/restoreState in a page template, 
        # but simplistic approach here for single page)
        elements.append(Spacer(1, 1*cm))
        footer = Paragraph("<i>SIB S.A.R.L - RC: 12345 - ICE: 0011223344 - Casablanca</i>", style_normal)
        elements.append(footer)

    # Build PDF
    doc.build(elements)
    buffer.seek(0)
    return buffer

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
    y = height - 150
    p.setFont("Helvetica-Bold", 9)
    p.drawString(50, y, "Item Description")
    p.drawString(350, y, "Qty")
    p.drawString(400, y, "Unit Price")
    p.drawString(480, y, "Total")
    
    # --- ITEMS ---
    y -= 20
    p.setFont("Helvetica", 9)
    
    total_amount = 0
    
    for item in act.items:
        # Simple text wrapping logic or truncation would go here for long descriptions
        desc = item.merged_po.item_description[:50] 
        p.drawString(50, y, desc)
        p.drawString(350, y, str(item.quantity_sbc))
        p.drawString(400, y, f"{item.unit_price_sbc:,.2f}")
        p.drawString(480, y, f"{item.line_amount_sbc:,.2f}")
        
        total_amount += item.line_amount_sbc
        y -= 20
        
        if y < 50: # New page logic
            p.showPage()
            y = height - 50

    # --- FOOTER ---
    y -= 20
    p.line(50, y+10, 550, y+10)
    p.setFont("Helvetica-Bold", 10)
    p.drawString(350, y, "Total Amount:")
    p.drawString(480, y, f"{total_amount:,.2f}")
    
    p.showPage()
    p.save()
    
    buffer.seek(0)
    return buffer
