import datetime
from io import BytesIO

from reportlab.lib import colors
from reportlab.lib.pagesizes import A3, landscape
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import (
    SimpleDocTemplate,
    Table,
    TableStyle,
    Paragraph,
    Spacer,
)

from app.models.action import Action
from app.services.action_priority_service import enrich_action_priority
from app.services.email_service import send_email


def value_or_dash(value):
    return value if value not in [None, ""] else "—"


def build_weekly_report_pdf(responsable: str, actions: list[Action]) -> bytes:
    buffer = BytesIO()

    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A3),
        rightMargin=12,
        leftMargin=12,
        topMargin=20,
        bottomMargin=20,
    )

    styles = getSampleStyleSheet()
    normal_style = styles["Normal"]
    normal_style.fontSize = 6
    normal_style.leading = 7

    elements = []

    title = Paragraph(
        f"<b>Action Plan Weekly Report</b><br/>{value_or_dash(responsable)}",
        styles["Title"],
    )

    date_text = Paragraph(
        f"Generated on {datetime.date.today().strftime('%Y-%m-%d')}",
        styles["Normal"],
    )

    elements.append(title)
    elements.append(date_text)
    elements.append(Spacer(1, 12))

    headers = [
        "Index priorité",
        "Sujet",
        "Raison demande",
        "Origine",
        "Tâche",
        "Demandeur",
        "Attendus",
        "Responsable",
        "Temps estimé",
        "Compte rendu",
        "Fichier joint",
        "Date fin souhaitée",
        "Statut",
    ]

    data = [headers]

    for action in actions:
        enrich_action_priority(action)

        data.append([
            value_or_dash(action.priority_index),
            Paragraph(value_or_dash(action.titre), normal_style),
            Paragraph(value_or_dash(action.description), normal_style),
            "—",
            Paragraph(value_or_dash(action.titre), normal_style),
            "—",
            "—",
            Paragraph(value_or_dash(action.responsable), normal_style),
            f"{action.estimated_duration_days or 2} j",
            "—",
            "—",
            str(value_or_dash(action.due_date)),
            value_or_dash(action.status),
        ])

    col_widths = [
        55,   # Index priorité
        130,  # Sujet
        180,  # Raison demande
        65,   # Origine
        130,  # Tâche
        70,   # Demandeur
        70,   # Attendus
        120,  # Responsable
        65,   # Temps estimé
        85,   # Compte rendu
        70,   # Fichier joint
        85,   # Date fin souhaitée
        65,   # Statut
    ]

    table = Table(data, colWidths=col_widths, repeatRows=1)

    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1d4ed8")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 7),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),

        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 6),
        ("LEADING", (0, 1), (-1, -1), 7),

        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d1d5db")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),

        ("ALIGN", (0, 1), (0, -1), "CENTER"),
        ("ALIGN", (8, 1), (8, -1), "CENTER"),
        ("ALIGN", (11, 1), (12, -1), "CENTER"),

        ("BACKGROUND", (0, 1), (-1, -1), colors.white),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [
            colors.white,
            colors.HexColor("#f8fafc"),
        ]),

        ("TEXTCOLOR", (12, 1), (12, -1), colors.HexColor("#111827")),
        ("FONTNAME", (12, 1), (12, -1), "Helvetica-Bold"),
    ]))

    elements.append(table)
    doc.build(elements)

    pdf = buffer.getvalue()
    buffer.close()

    return pdf


def build_weekly_report_email(responsable: str, action_count: int):
    return f"""
    <html>
      <body style="font-family:Arial,sans-serif;background:#f3f4f6;margin:0;padding:24px;">
        <div style="max-width:700px;margin:auto;background:white;border-radius:16px;overflow:hidden;">
          <div style="background:#1d4ed8;color:white;padding:24px;">
            <h2 style="margin:0;">Action Plan Weekly Report</h2>
            <p style="margin:6px 0 0;color:#dbeafe;">Weekly PDF report attached</p>
          </div>

          <div style="padding:24px;color:#1f2937;">
            <p>Hello <strong>{responsable or ""}</strong>,</p>

            <p>
              Please find attached your weekly Action Plan report.
              It contains your current actions in the requested Excel-style format.
            </p>

            <div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:12px;padding:16px;margin:18px 0;">
              <div style="font-size:12px;color:#1d4ed8;font-weight:bold;">TOTAL ACTIONS</div>
              <div style="font-size:28px;font-weight:bold;color:#1e3a8a;">{action_count}</div>
            </div>

            <p style="color:#64748b;font-size:13px;">
              This is an automated message from the Action Plan system.
            </p>
          </div>
        </div>
      </body>
    </html>
    """


async def send_test_weekly_responsable_reports_service(db, test_email: str):
    actions = (
        db.query(Action)
        .filter(Action.status != "closed")
        .filter(Action.email_responsable.isnot(None))
        .order_by(Action.priority_index.desc().nullslast(), Action.due_date.asc())
        .all()
    )

    grouped = {}

    for action in actions:
        grouped.setdefault(action.email_responsable, []).append(action)

    sent_count = 0

    for responsable_email, responsable_actions in list(grouped.items())[:1]:
        responsable_name = responsable_actions[0].responsable

        pdf_bytes = build_weekly_report_pdf(
            responsable=responsable_name,
            actions=responsable_actions,
        )

        html_body = build_weekly_report_email(
            responsable=responsable_name,
            action_count=len(responsable_actions),
        )

        sent = send_email(
            to_email=test_email,
            subject=f"[TEST] Action Plan Weekly Report - {responsable_name}",
            html_body=html_body,
            attachments=[
                (
                    f"action_plan_weekly_report_{responsable_name or 'responsable'}.pdf",
                    pdf_bytes,
                )
            ],
        )

        if sent:
            sent_count += 1

    return {
        "message": "Test weekly report sent successfully",
        "test_email": test_email,
        "matched_responsables": len(grouped),
        "sent_emails": sent_count,
    }


async def send_weekly_responsable_reports_service(db):
    actions = (
        db.query(Action)
        .filter(Action.status != "closed")
        .filter(Action.email_responsable.isnot(None))
        .order_by(Action.priority_index.desc().nullslast(), Action.due_date.asc())
        .all()
    )

    grouped = {}

    for action in actions:
        grouped.setdefault(action.email_responsable, []).append(action)

    sent_count = 0

    for responsable_email, responsable_actions in grouped.items():
        responsable_name = responsable_actions[0].responsable

        pdf_bytes = build_weekly_report_pdf(
            responsable=responsable_name,
            actions=responsable_actions,
        )

        html_body = build_weekly_report_email(
            responsable=responsable_name,
            action_count=len(responsable_actions),
        )

        sent = send_email(
            to_email=responsable_email,
            subject=f"[Action Plan] Weekly Report - {responsable_name}",
            html_body=html_body,
            attachments=[
                (
                    f"action_plan_weekly_report_{responsable_name or 'responsable'}.pdf",
                    pdf_bytes,
                )
            ],
        )

        if sent:
            sent_count += 1

    return {
        "message": "Weekly reports sent successfully",
        "matched_responsables": len(grouped),
        "sent_emails": sent_count,
    }