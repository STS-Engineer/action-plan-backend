import datetime
from app.models.action import Action
from app.services.email_service import send_email
from app.services.action_priority_service import enrich_action_priority


def build_reminder_email(action):
    return f"""
    <div style="font-family: Arial, sans-serif; color: #1f2937;">
      <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background: #f3f4f6;">
        <tr>
          <td align="center" style="padding: 20px;">
            <table width="100%" max-width="600" cellpadding="0" cellspacing="0" border="0" style="max-width: 600px; width: 100%; background: #ffffff; border-radius: 12px;">
              <tr>
                <td style="background: linear-gradient(135deg, #2563eb, #1e40af); padding: 32px 24px; text-align: center;">
                  <h2 style="color: #ffffff; margin: 0 0 8px 0; font-size: 28px;">📋 Action Plan Reminder</h2>
                  <p style="color: #dbeafe; margin: 0; font-size: 14px;">Action requiring your attention</p>
                </td>
              </tr>
              <tr>
                <td style="padding: 32px 24px;">
                  <p style="color: #374151; font-size: 16px; margin-bottom: 24px;">
                    Hello <strong style="color: #2563eb;">{action.responsable or ''}</strong>,
                  </p>
                  
                  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background: #fef3c7; border-left: 4px solid #f59e0b; margin-bottom: 24px;">
                    <tr>
                      <td style="padding: 16px;">
                        <p style="margin: 0; color: #92400e; font-size: 14px;">
                          ⚠️ This action is approaching or has passed its due date.
                        </p>
                      </td>
                    </tr>
                  </table>
                  
                  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background: #f8fafc; border-radius: 12px;">
                    <tr>
                      <td style="padding: 16px; border-bottom: 1px solid #e2e8f0;">
                        <div style="font-size: 11px; font-weight: bold; color: #64748b;">ACTION TITLE</div>
                        <div style="font-size: 16px; font-weight: bold; color: #1e293b;">{action.titre or '-'}</div>
                      </td>
                    </tr>
                    <tr>
                      <td style="padding: 16px; border-bottom: 1px solid #e2e8f0;">
                        <div style="font-size: 11px; font-weight: bold; color: #64748b;">DESCRIPTION</div>
                        <div style="font-size: 14px; color: #475569;">{action.description or '-'}</div>
                      </td>
                    </tr>
                    <tr>
                      <td style="padding: 16px; border-bottom: 1px solid #e2e8f0;">
                        <div style="font-size: 11px; font-weight: bold; color: #64748b;">STATUS</div>
                        <span style="display: inline-block; background: #f97316; color: white; padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: bold;">{action.status or '-'}</span>
                      </td>
                    </tr>
                    <tr>
                      <td style="padding: 16px; border-bottom: 1px solid #e2e8f0;">
                        <div style="font-size: 11px; font-weight: bold; color: #64748b;">DUE DATE</div>
                        <div style="font-size: 14px; font-weight: bold; color: #dc2626;">{action.due_date or '-'}</div>
                      </td>
                    </tr>
                    <tr>
                      <td style="padding: 16px; border-bottom: 1px solid #e2e8f0;">
                        <div style="font-size: 11px; font-weight: bold; color: #64748b;">IMPORTANCE</div>
                        <div style="font-size: 14px; font-weight: bold; color: #2563eb;">{action.importance or '-'}</div>
                      </td>
                    </tr>
                    <tr>
                      <td style="padding: 16px;">
                        <div style="font-size: 11px; font-weight: bold; color: #64748b;">URGENCY</div>
                        <div style="font-size: 14px; font-weight: bold; color: #dc2626;">{action.urgency or '-'}</div>
                      </td>
                    </tr>
                  </table>
                  
                  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background: #f0fdf4; border-radius: 8px; margin: 24px 0;">
                    <tr>
                      <td style="padding: 16px; text-align: center;">
                        <div style="font-size: 11px; color: #166534;">PRIORITY INDEX</div>
                        <div style="font-size: 32px; font-weight: bold; color: #15803d;">{action.priority_index or '-'}</div>
                      </td>
                    </tr>
                  </table>
                  
                  <div style="text-align: center;">
                    <a href="http://127.0.0.1:8000/api/action_plan_action/actions/{action.id}/mark-closed-from-email" 
                       style="display: inline-block; background: #2563eb; color: white; padding: 14px 28px; text-decoration: none; border-radius: 8px; font-weight: bold;">
                      ✨ Mark as Complete
                    </a>
                  </div>
                  
                  <hr style="border: none; border-top: 1px solid #e5e7eb; margin: 24px 0;">
                  
                  <p style="color: #6b7280; font-size: 12px; text-align: center;">
                    Best regards,<br/>
                    <strong>Action Plan System</strong>
                  </p>
                </td>
              </tr>
            </table>
          </td>
        </tr>
      </table>
    </div>
    """


def build_responsable_summary_email(responsable, reminder_action_ids, actions):
    rows = ""

    for action in actions:
        is_reminder = action.id in reminder_action_ids

        row_bg = "#fff7ed" if is_reminder else "#ffffff"

        urgency_color = "#dc2626" if action.urgency == "urgent" else "#2563eb"
        status_color = "#16a34a" if action.status == "closed" else "#f97316"

        complete_url = f"http://127.0.0.1:8000/api/action_plan_action/actions/{action.id}/mark-closed-from-email"

        rows += f"""
        <tr style="background:{row_bg};">
          <td style="padding:10px;border-bottom:1px solid #e5e7eb;font-weight:700;text-align:center;">
            {action.priority_index or "-"}
          </td>

          <td style="padding:10px;border-bottom:1px solid #e5e7eb;font-weight:600;color:#0f172a;">
            {action.titre or "-"}
          </td>

          <td style="padding:10px;border-bottom:1px solid #e5e7eb;color:#475569;">
            {action.description or "-"}
          </td>

          <td style="padding:10px;border-bottom:1px solid #e5e7eb;">
            {action.responsable or "-"}
          </td>

          <td style="padding:10px;border-bottom:1px solid #e5e7eb;font-weight:600;">
            {action.due_date or "-"}
          </td>

          <td style="padding:10px;border-bottom:1px solid #e5e7eb;text-align:center;">
            <span style="background:{status_color};color:white;padding:4px 10px;border-radius:999px;font-size:11px;font-weight:700;">
              {action.status or "-"}
            </span>
          </td>

          <td style="padding:10px;border-bottom:1px solid #e5e7eb;text-align:center;">
            <span style="background:{urgency_color};color:white;padding:4px 10px;border-radius:999px;font-size:11px;font-weight:700;">
              {action.urgency or "-"}
            </span>
          </td>

          <td style="padding:10px;border-bottom:1px solid #e5e7eb;text-align:center;">
            <a href="{complete_url}"
               style="background:#2563eb;color:white;padding:8px 12px;text-decoration:none;border-radius:8px;font-size:11px;font-weight:700;">
               Complete
            </a>
          </td>
        </tr>
        """

    total_actions = len(actions)
    reminder_count = len(reminder_action_ids)

    return f"""
    <html>
    <body style="margin:0;background:#f3f4f6;font-family:Arial,sans-serif;">

      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td align="center" style="padding:20px;">

            <table width="100%" style="max-width:900px;background:white;border-radius:12px;overflow:hidden;">

              <!-- HEADER -->
              <tr>
                <td style="background:#1d4ed8;color:white;padding:24px;">
                  <h2 style="margin:0;">Action Plan - Daily Follow-up</h2>
                  <p style="margin:5px 0 0;font-size:13px;color:#dbeafe;">
                    {responsable}
                  </p>
                </td>
              </tr>

              <!-- STATS -->
              <tr>
                <td style="padding:20px;">
                  <table width="100%">
                    <tr>
                      <td style="background:#eff6ff;padding:15px;border-radius:10px;text-align:center;">
                        <div style="font-size:12px;color:#1d4ed8;">TOTAL</div>
                        <div style="font-size:26px;font-weight:bold;color:#1e3a8a;">{total_actions}</div>
                      </td>

                      <td style="width:10px;"></td>

                      <td style="background:#fff7ed;padding:15px;border-radius:10px;text-align:center;">
                        <div style="font-size:12px;color:#c2410c;">DUE / LATE</div>
                        <div style="font-size:26px;font-weight:bold;color:#9a3412;">{reminder_count}</div>
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>

              <!-- TABLE -->
              <tr>
                <td style="padding:0 20px 20px 20px;">
                  <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e5e7eb;">

                    <tr style="background:#f8fafc;">
                      <th style="padding:10px;font-size:11px;text-align:center;">Priority</th>
                      <th style="padding:10px;font-size:11px;text-align:left;">Action</th>
                      <th style="padding:10px;font-size:11px;text-align:left;">Description</th>
                      <th style="padding:10px;font-size:11px;">Responsible</th>
                      <th style="padding:10px;font-size:11px;">Due date</th>
                      <th style="padding:10px;font-size:11px;">Status</th>
                      <th style="padding:10px;font-size:11px;">Urgency</th>
                      <th style="padding:10px;font-size:11px;">Update</th>
                    </tr>

                    {rows}

                  </table>
                </td>
              </tr>

              <!-- FOOTER -->
              <tr>
                <td style="padding:20px;text-align:center;font-size:11px;color:#9ca3af;">
                  Automated Action Plan System
                </td>
              </tr>

            </table>

          </td>
        </tr>
      </table>

    </body>
    </html>
    """

async def send_due_date_reminders_service(db):
    today = datetime.date.today()
    reminder_limit = today + datetime.timedelta(days=2)

    actions = (
        db.query(Action)
        .filter(Action.status != "closed")
        .filter(Action.email_responsable.isnot(None))
        .filter(Action.due_date.isnot(None))
        .filter(Action.due_date <= reminder_limit)
        .all()
    )

    sent_count = 0

    for action in actions:
        enrich_action_priority(action)

        subject = f"[Action Plan] Reminder - {action.titre}"
        html_body = build_reminder_email(action)

        sent = send_email(
            to_email=action.email_responsable,
            subject=subject,
            html_body=html_body,
        )

        if sent:
            action.last_reminder_sent_at = datetime.datetime.now(datetime.timezone.utc)
            sent_count += 1

    db.commit()

    return {
        "message": "Reminder emails processed successfully",
        "matched_actions": len(actions),
        "sent_emails": sent_count,
    }


async def send_test_due_date_reminders_service(db, test_email: str):
    today = datetime.date.today()
    reminder_limit = today + datetime.timedelta(days=2)

    reminder_actions = (
        db.query(Action)
        .filter(Action.status != "closed")
        .filter(Action.email_responsable.isnot(None))
        .filter(Action.due_date.isnot(None))
        .filter(Action.due_date <= reminder_limit)
        .order_by(Action.priority_index.desc().nullslast(), Action.due_date.asc())
        .all()
    )

    responsables = {}

    for action in reminder_actions:
        responsables.setdefault(action.email_responsable, []).append(action)

    sent_count = 0

    for responsable_email, highlighted_actions in list(responsables.items())[:3]:
        all_responsable_actions = (
            db.query(Action)
            .filter(Action.status != "closed")
            .filter(Action.email_responsable == responsable_email)
            .order_by(Action.priority_index.desc().nullslast(), Action.due_date.asc())
            .all()
        )

        for action in all_responsable_actions:
            enrich_action_priority(action)

        responsable_name = highlighted_actions[0].responsable

        subject = f"[TEST] Action Plan Dashboard - {responsable_name}"

        html_body = build_responsable_summary_email(
            responsable=responsable_name,
            reminder_action_ids=[action.id for action in highlighted_actions],
            actions=all_responsable_actions,
        )

        sent = send_email(
            to_email=test_email,
            subject=subject,
            html_body=html_body,
        )

        if sent:
            sent_count += 1

    return {
        "message": "Test summary emails sent successfully",
        "test_email": test_email,
        "matched_responsables": len(responsables),
        "sent_emails": sent_count,
    }


async def send_grouped_due_date_reminders_service(db):
    today = datetime.date.today()
    reminder_limit = today + datetime.timedelta(days=2)

    start_of_today = datetime.datetime.combine(
        today,
        datetime.time.min,
        tzinfo=datetime.timezone.utc,
    )

    reminder_actions = (
        db.query(Action)
        .filter(Action.status != "closed")
        .filter(Action.email_responsable.isnot(None))
        .filter(Action.due_date.isnot(None))
        .filter(Action.due_date <= reminder_limit)
        .filter(
            (Action.last_reminder_sent_at.is_(None))
            | (Action.last_reminder_sent_at < start_of_today)
        )
        .order_by(Action.priority_index.desc().nullslast(), Action.due_date.asc())
        .all()
    )

    responsables = {}

    for action in reminder_actions:
        responsables.setdefault(action.email_responsable, []).append(action)

    sent_count = 0

    for responsable_email, highlighted_actions in responsables.items():
        all_responsable_actions = (
            db.query(Action)
            .filter(Action.status != "closed")
            .filter(Action.email_responsable == responsable_email)
            .order_by(Action.priority_index.desc().nullslast(), Action.due_date.asc())
            .all()
        )

        for action in all_responsable_actions:
            enrich_action_priority(action)

        responsable_name = highlighted_actions[0].responsable

        subject = f"[Action Plan] Dashboard - {responsable_name}"

        html_body = build_responsable_summary_email(
            responsable=responsable_name,
            reminder_action_ids=[action.id for action in highlighted_actions],
            actions=all_responsable_actions,
        )

        sent = send_email(
            to_email=responsable_email,
            subject=subject,
            html_body=html_body,
        )

        if sent:
            now = datetime.datetime.now(datetime.timezone.utc)

            for action in highlighted_actions:
                action.last_reminder_sent_at = now

            sent_count += 1

    db.commit()

    return {
        "message": "Grouped reminder emails sent successfully",
        "matched_responsables": len(responsables),
        "sent_emails": sent_count,
    }