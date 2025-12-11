import os
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    SimpleDocTemplate,
    Table,
    TableStyle,
    Paragraph,
    Spacer,
    Image,
)
from reportlab.lib.enums import TA_CENTER
from xml.sax.saxutils import escape  # Import this

def generate_bc_pdf(bc):
    # 1. Setup File Path
    filename = f"generated_bcs/{bc.bc_number}.pdf"
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    # 2. Document Setup
    doc = SimpleDocTemplate(
        filename,
        pagesize=A4,
        rightMargin=10 * mm,
        leftMargin=10 * mm,
        topMargin=10 * mm,
        bottomMargin=10 * mm,
    )

    elements = []
    styles = getSampleStyleSheet()

    # ---------- STYLES ----------
    style_normal = styles["Normal"]
    style_normal.fontSize = 8
    style_normal.leading = 10

    style_bold = ParagraphStyle(
        "Bold",
        parent=styles["Normal"],
        fontSize=8,
        leading=10,
        fontName="Helvetica-Bold",
    )
    style_title = ParagraphStyle(
        "Title",
        parent=styles["Normal"],
        fontSize=16,
        leading=20,
        fontName="Helvetica-Bold",
    )

    # NEW: footer style (small & centered)
    style_footer = ParagraphStyle(
        "Footer",
        parent=styles["Normal"],
        fontSize=6,
        leading=8,
        alignment=TA_CENTER,
        textColor=colors.grey,
    )

    # --- HEADER SECTION ---
    logo_path = "logo.png"  # Make sure this file exists
    if os.path.exists(logo_path):
        logo = Image(logo_path, width=4 * cm, height=2 * cm)
    else:
        logo = Paragraph("<b>[LOGO]</b>", style_title)

    header_data = [
        [logo, Paragraph("<b>Purchase Order</b>", style_title)],
        [
            Paragraph(f"<b>Subcontract No.:</b> {bc.bc_number}", style_bold),
            Paragraph(f"<b>Date:</b> {bc.created_at.strftime('%Y-%m-%d')}", style_bold),
        ],
    ]

    t_header = Table(header_data, colWidths=[10 * cm, 9 * cm])
    t_header.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("ALIGN", (1, 0), (1, 0), "RIGHT"),
                ("ALIGN", (1, 1), (1, 1), "RIGHT"),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
            ]
        )
    )
    elements.append(t_header)
    elements.append(Spacer(1, 0.5 * cm))

    # --- ADDRESS BLOCK ---
    sbc_info = [
        [Paragraph("<b>Ship To / Supplier:</b>", style_bold)],
        [Paragraph(f"<b>Name:</b> {bc.sbc.ceo_name or ''}", style_normal)],
        [Paragraph(f"<b>Company:</b> {bc.sbc.name}", style_normal)],
        [Paragraph(f"<b>Phone:</b> {bc.sbc.phone_1 or ''}", style_normal)],
        [Paragraph(f"<b>Fax:</b> -", style_normal)],
        [Paragraph(f"<b>Currency:</b> MAD", style_normal)],
    ]

    bill_to_info = [
        [Paragraph("<b>Bill To:</b>", style_bold)],
        [Paragraph("<b>Name:</b> Finance Dept", style_normal)],
        [
            Paragraph(
                "<b>Company:</b> SOLUTION INTEGRALE BUILDING (SIB)", style_normal
            )
        ],
        [
            Paragraph(
                "<b>Address:</b> 123 Main St, Casablanca, Morocco", style_normal
            )
        ],
        [Paragraph("<b>Phone:</b> +212 5 22 00 00 00", style_normal)],
        [Paragraph("<b>Tax Rate:</b> See Details", style_normal)],
        [Paragraph(f"<b>Project Info:</b> {bc.internal_project.name}", style_normal)],
        [
            Paragraph(
                f"<b>Created Date:</b> {bc.created_at.strftime('%Y-%m-%d')}",
                style_normal,
            )
        ],
    ]

    address_table_data = [
        [Table(sbc_info, colWidths=[9 * cm]), Table(bill_to_info, colWidths=[9 * cm])]
    ]

    t_address = Table(address_table_data, colWidths=[9.5 * cm, 9.5 * cm])
    t_address.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LINEBELOW", (0, 0), (-1, -1), 0.5, colors.lightgrey),
            ]
        )
    )
    elements.append(t_address)
    elements.append(Spacer(1, 0.5 * cm))

    # --- MAIN ITEMS TABLE ---
    table_headers = [
        "Line",
        "Site Code",
        "Description",
        "UOM",
        "Unit Price",
        "Qty",
        "Sub Total",
        "Tax",
        "Start",
        "End",
    ]

    data = [table_headers]

    for idx, item in enumerate(bc.items):
        start = (
            item.merged_po.publish_date.strftime("%Y-%m-%d")
            if item.merged_po.publish_date
            else "-"
        )
        end = "-"  # TODO: if you have an end date, plug it here

        raw_desc = item.merged_po.item_description or ""
        clean_desc = escape(raw_desc)

        raw_site = item.merged_po.site_code or ""
        clean_site = escape(raw_site)

        row = [
            str(idx + 1),
            Paragraph(clean_site or "", style_normal),
            Paragraph(clean_desc or "", style_normal),
            "Unit",
            f"{item.unit_price_sbc:,.2f}",
            str(item.quantity_sbc),
            f"{item.line_amount_sbc:,.2f}",
            f"{int(item.applied_tax_rate * 100)}%",
            start,
            end,
        ]
        data.append(row)

    col_widths = [
        0.8 * cm,  # Line
        2.5 * cm,  # Site
        5.0 * cm,  # Desc
        1.0 * cm,  # UOM
        2.0 * cm,  # Price
        1.0 * cm,  # Qty
        2.2 * cm,  # SubTotal
        1.0 * cm,  # Tax
        1.8 * cm,  # Start
        1.8 * cm,  # End
    ]

    t_items = Table(data, colWidths=col_widths, repeatRows=1)
    t_items.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 7),
                ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("ALIGN", (4, 0), (7, -1), "RIGHT"),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 2),
                ("RIGHTPADDING", (0, 0), (-1, -1), 2),
            ]
        )
    )

    elements.append(t_items)
    elements.append(Spacer(1, 0.5 * cm))

    # --- TOTALS SECTION ---
    totals_data = [
        ["Total Amount (Exclude Tax):", f"{bc.total_amount_ht:,.2f}"],
        ["Total Tax Amount:", f"{bc.total_tax_amount:,.2f}"],
        ["Total Amount (Include Tax):", f"{bc.total_amount_ttc:,.2f}"],
    ]

    t_totals = Table(totals_data, colWidths=[5 * cm, 3 * cm])
    t_totals.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"),
                ("ALIGN", (0, 0), (0, -1), "RIGHT"),
                ("ALIGN", (1, 0), (1, -1), "RIGHT"),
                ("LINEBELOW", (0, 0), (-1, -1), 0.5, colors.lightgrey),
            ]
        )
    )

    t_totals_container = Table([[None, t_totals]], colWidths=[11 * cm, 8 * cm])
    elements.append(t_totals_container)
    elements.append(Spacer(1, 1 * cm))

    # --- LEGAL NOTES ---
    notes_text = """
    <b>Notes:</b><br/>
    1. This Purchase Order ("PO") is governed by all applicable agreements executed between the Supplier named under this PO and 
    SIB, whether by physical signature or online.<br/>
    2. Within forty-eight (48) hours after receipting this PO, Supplier shall either confirm its acceptance or inquire about it.<br/>
    3. The PO number and the applicable line number(s) shall appear on each invoice.<br/>
    4. Any change made to an existing PO shall be subject to written confirmation.
    """
    elements.append(Paragraph(notes_text, style_normal))
    elements.append(Spacer(1, 0.5 * cm))

    # ---------- NEW: SIB LEGAL FOOTER BLOCK ----------
    legal_block = """
    SOLUTION INTEGRALE BUILDING (SIB) – Adresse complète, Casablanca – Royaume du Maroc<br/>
    ICE : 001234567000012 – IF : 12345678 – RC : 123456 – CNSS : 1234567 – Patente : 12345678<br/>
    Banque : [Nom de la banque] – RIB : [RIB complet] – IBAN : [MA..] – SWIFT : [XXXXMAMC]
    """
    # TODO: remplace les valeurs ci-dessus par les vraies infos SIB

    # fine separator line
    footer_line = Table(
        [[""]],
        colWidths=[18 * cm],
        style=[
            ("LINEABOVE", (0, 0), (-1, 0), 0.5, colors.lightgrey),
            ("TOPPADDING", (0, 0), (-1, 0), 2),
        ],
    )
    elements.append(footer_line)
    elements.append(Spacer(1, 0.2 * cm))
    elements.append(Paragraph(legal_block, style_footer))

    # Build PDF
    doc.build(elements)
    return filename
