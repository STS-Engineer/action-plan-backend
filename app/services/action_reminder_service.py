import datetime
import logging
from html import escape

from fastapi import HTTPException
from sqlalchemy import func
from app.models.action import Action
from app.services.email_service import (
    get_smtp_config_diagnostics,
    send_email,
    send_email_with_diagnostics,
)
from app.services.action_priority_service import enrich_action_priority
from app.services.action_status_logic_service import (
    get_action_active_predicate,
    normalize_action_status,
)
from app.utils.action_links import build_action_frontend_url

DEMO_ACTION_LINK_RECIPIENT = "olivier.spicker@avocarbon.com"
REMINDER_ACTIVE_STATUSES = {"open", "blocked"}
REMINDER_DUE_SOON_DAYS = 2
logger = logging.getLogger(__name__)


def value_or_dash(value):
    return value if value not in [None, ""] else "-"


def escape_email_value(value):
    return escape(str(value_or_dash(value)))


def normalize_reminder_email(email: str | None) -> str:
    return (email or "").strip().lower()


def get_start_of_today_utc(today: datetime.date | None = None):
    today = today or datetime.date.today()
    return datetime.datetime.combine(
        today,
        datetime.time.min,
        tzinfo=datetime.timezone.utc,
    )


def was_reminded_today(action, start_of_today=None) -> bool:
    start_of_today = start_of_today or get_start_of_today_utc()
    last_sent = getattr(action, "last_reminder_sent_at", None)

    if not last_sent:
        return False

    if last_sent.tzinfo is None:
        last_sent = last_sent.replace(tzinfo=datetime.timezone.utc)

    return last_sent >= start_of_today


def get_daily_reminder_exclusion_reason(
    action,
    target_email: str | None = None,
    start_of_today=None,
    respect_sent_today: bool = True,
):
    if bool(getattr(action, "is_deleted", False)):
        return "deleted"

    email_responsable = normalize_reminder_email(
        getattr(action, "email_responsable", None)
    )

    if not email_responsable:
        return "missing_email_responsable"

    if target_email and email_responsable != normalize_reminder_email(target_email):
        return "different_responsible"

    canonical_status = normalize_action_status(getattr(action, "status", None))

    if canonical_status not in REMINDER_ACTIVE_STATUSES:
        return f"status_{canonical_status or 'empty'}"

    if respect_sent_today and was_reminded_today(action, start_of_today):
        return "already_reminded_today"

    return None


def build_reminder_action_debug_payload(action, target_email: str | None = None):
    exclusion_reason = get_daily_reminder_exclusion_reason(action, target_email)

    return {
        "id": action.id,
        "titre": action.titre,
        "raw_status": action.status,
        "canonical_status": normalize_action_status(action.status),
        "due_date": action.due_date.isoformat() if action.due_date else None,
        "email_responsable": action.email_responsable,
        "is_deleted": bool(getattr(action, "is_deleted", False)),
        "last_reminder_sent_at": (
            action.last_reminder_sent_at.isoformat()
            if action.last_reminder_sent_at
            else None
        ),
        "eligible": exclusion_reason is None,
        "exclusion_reason": exclusion_reason,
    }


def get_daily_reminder_candidate_actions(db):
    return (
        db.query(Action)
        .filter(get_action_active_predicate(Action))
        .filter(Action.email_responsable.isnot(None))
        .order_by(Action.priority_index.desc().nullslast(), Action.due_date.asc())
        .all()
    )


def filter_daily_reminder_actions(actions):
    start_of_today = get_start_of_today_utc()
    return [
        action
        for action in actions
        if (
            get_daily_reminder_exclusion_reason(
                action,
                start_of_today=start_of_today,
            )
            is None
        )
    ]


def filter_daily_active_assigned_actions(actions):
    return [
        action
        for action in actions
        if (
            get_daily_reminder_exclusion_reason(
                action,
                respect_sent_today=False,
            )
            is None
        )
    ]


def group_actions_by_responsable(actions):
    responsables = {}

    for action in actions:
        email = normalize_reminder_email(action.email_responsable)
        if email:
            responsables.setdefault(email, []).append(action)

    return responsables


def get_due_section(action, today: datetime.date | None = None):
    today = today or datetime.date.today()
    due_date = getattr(action, "due_date", None)

    if not due_date:
        return "no_due_date", "Active actions without due date"

    if due_date < today:
        return "overdue", "Overdue actions"

    if due_date <= today + datetime.timedelta(days=REMINDER_DUE_SOON_DAYS):
        return "due_soon", "Due soon"

    return "active", "Other active actions"


def build_demo_action_link_email(action, action_url):
    safe_action_url = escape(action_url, quote=True)

    return f"""
    <div style="font-family: Arial, sans-serif; color: #1f2937;">
      <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background: #f3f4f6;">
        <tr>
          <td align="center" style="padding: 20px;">
            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width: 600px; width: 100%; background: #ffffff; border-radius: 12px; overflow: hidden;">
              <tr>
                <td style="background: linear-gradient(135deg, #2563eb, #1e40af); padding: 32px 24px; text-align: center;">
                  <h2 style="color: #ffffff; margin: 0 0 8px 0; font-size: 28px;">Action Plan Direct Link</h2>
                  <p style="color: #dbeafe; margin: 0; font-size: 14px;">Demo email for one action</p>
                </td>
              </tr>

              <tr>
                <td style="padding: 32px 24px;">
                  <p style="color: #374151; font-size: 16px; margin: 0 0 24px 0;">
                    Hello,
                  </p>

                  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background: #eff6ff; border-left: 4px solid #2563eb; margin-bottom: 24px;">
                    <tr>
                      <td style="padding: 16px;">
                        <p style="margin: 0; color: #1e40af; font-size: 14px;">
                          Use the button below to open this exact action in the Action Plan app.
                        </p>
                      </td>
                    </tr>
                  </table>

                  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background: #f8fafc; border-radius: 12px;">
                    <tr>
                      <td style="padding: 16px; border-bottom: 1px solid #e2e8f0;">
                        <div style="font-size: 11px; font-weight: bold; color: #64748b;">ACTION TITLE</div>
                        <div style="font-size: 16px; font-weight: bold; color: #1e293b;">{escape_email_value(action.titre)}</div>
                      </td>
                    </tr>
                    <tr>
                      <td style="padding: 16px; border-bottom: 1px solid #e2e8f0;">
                        <div style="font-size: 11px; font-weight: bold; color: #64748b;">STATUS</div>
                        <span style="display: inline-block; background: #f97316; color: white; padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: bold;">{escape_email_value(action.status)}</span>
                      </td>
                    </tr>
                    <tr>
                      <td style="padding: 16px;">
                        <div style="font-size: 11px; font-weight: bold; color: #64748b;">DUE DATE</div>
                        <div style="font-size: 14px; font-weight: bold; color: #dc2626;">{escape_email_value(action.due_date)}</div>
                      </td>
                    </tr>
                  </table>

                  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background: #f0fdf4; border-radius: 8px; margin: 24px 0;">
                    <tr>
                      <td style="padding: 16px; text-align: center;">
                        <div style="font-size: 11px; color: #166534;">PRIORITY INDEX</div>
                        <div style="font-size: 32px; font-weight: bold; color: #15803d;">{escape_email_value(action.priority_index)}</div>
                      </td>
                    </tr>
                  </table>

                  <div style="text-align: center;">
                    <a href="{safe_action_url}"
                       style="display: inline-block; background: #2563eb; color: white; padding: 14px 28px; text-decoration: none; border-radius: 8px; font-weight: bold;">
                      View action
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


async def send_demo_action_link_to_olivier_service(
    action_id: int,
    db,
    test_email: str | None = None,
):
    action = (
        db.query(Action)
        .filter(Action.id == action_id)
        .filter(get_action_active_predicate(Action))
        .first()
    )

    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    recipient = test_email or DEMO_ACTION_LINK_RECIPIENT
    action_url = build_action_frontend_url(action.id)
    html_body = build_demo_action_link_email(action, action_url)

    send_error = None

    try:
        sent = send_email(
            to_email=recipient,
            subject="[DEMO] Action Plan - Direct action link",
            html_body=html_body,
        )
    except Exception as exc:
        sent = False
        send_error = str(exc)

    response = {
        "sent": sent,
        "to_email": recipient,
        "action_id": action.id,
        "generated_url": action_url,
    }

    if send_error:
        response["error"] = send_error

    return response


def build_reminder_email(action):
    action_url = build_action_frontend_url(action.id)

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
                    <a href="{action_url}" 
                       style="display: inline-block; background: #0f172a; color: white; padding: 14px 28px; text-decoration: none; border-radius: 8px; font-weight: bold; margin: 0 6px 10px;">
                      View action
                    </a>

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
    today = datetime.date.today()
    section_order = {
        "overdue": 0,
        "due_soon": 1,
        "no_due_date": 2,
        "active": 3,
    }

    def sort_key(action):
        section_key, _ = get_due_section(action, today)
        due_value = action.due_date or datetime.date.max
        priority_value = action.priority_index or 0
        return (section_order.get(section_key, 99), due_value, -priority_value)

    last_section_key = None

    for action in sorted(actions, key=sort_key):
        section_key, section_label = get_due_section(action, today)

        if section_key != last_section_key:
            rows += f"""
            <tr>
              <td colspan="8" style="background:#f1f5f9;color:#334155;padding:9px 10px;font-size:12px;font-weight:700;border-bottom:1px solid #e5e7eb;">
                {escape_email_value(section_label)}
              </td>
            </tr>
            """
            last_section_key = section_key

        is_reminder = action.id in reminder_action_ids
        row_bg = "#fff7ed" if is_reminder else "#ffffff"
        canonical_status = normalize_action_status(action.status)
        urgency_color = "#dc2626" if (action.urgency or "").lower() == "urgent" else "#2563eb"
        status_color = "#16a34a" if canonical_status == "closed" else "#f97316"
        complete_url = f"http://127.0.0.1:8000/api/action_plan_action/actions/{action.id}/mark-closed-from-email"
        action_url = escape(build_action_frontend_url(action.id), quote=True)

        rows += f"""
        <tr style="background:{row_bg};">
          <td style="padding:10px;border-bottom:1px solid #e5e7eb;font-weight:700;text-align:center;">
            {escape_email_value(action.priority_index)}
          </td>

          <td style="padding:10px;border-bottom:1px solid #e5e7eb;font-weight:600;color:#0f172a;">
            {escape_email_value(action.titre)}
          </td>

          <td style="padding:10px;border-bottom:1px solid #e5e7eb;color:#475569;">
            {escape_email_value(action.description)}
          </td>

          <td style="padding:10px;border-bottom:1px solid #e5e7eb;">
            {escape_email_value(action.responsable)}
          </td>

          <td style="padding:10px;border-bottom:1px solid #e5e7eb;font-weight:600;">
            {escape_email_value(action.due_date)}
          </td>

          <td style="padding:10px;border-bottom:1px solid #e5e7eb;text-align:center;">
            <span style="background:{status_color};color:white;padding:4px 10px;border-radius:999px;font-size:11px;font-weight:700;">
              {escape_email_value(action.status)}
            </span>
          </td>

          <td style="padding:10px;border-bottom:1px solid #e5e7eb;text-align:center;">
            <span style="background:{urgency_color};color:white;padding:4px 10px;border-radius:999px;font-size:11px;font-weight:700;">
              {escape_email_value(action.urgency)}
            </span>
          </td>

          <td style="padding:10px;border-bottom:1px solid #e5e7eb;text-align:center;">
            <a href="{action_url}"
               style="background:#0f172a;color:white;padding:8px 12px;text-decoration:none;border-radius:8px;font-size:11px;font-weight:700;display:inline-block;margin-bottom:6px;">
               View action
            </a>

            <a href="{complete_url}"
               style="background:#2563eb;color:white;padding:8px 12px;text-decoration:none;border-radius:8px;font-size:11px;font-weight:700;display:inline-block;">
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
                    {escape_email_value(responsable)}
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
    actions = filter_daily_reminder_actions(get_daily_reminder_candidate_actions(db))

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
        else:
            logger.error(
                "Daily individual reminder failed recipient=%s action_id=%s",
                action.email_responsable,
                action.id,
            )

    db.commit()

    return {
        "message": "Reminder emails processed successfully",
        "matched_actions": len(actions),
        "sent_emails": sent_count,
    }


async def send_test_due_date_reminders_service(db, test_email: str):
    reminder_actions = filter_daily_reminder_actions(
        get_daily_reminder_candidate_actions(db)
    )
    responsables = group_actions_by_responsable(reminder_actions)

    sent_count = 0

    for responsable_email, highlighted_actions in list(responsables.items())[:3]:
        all_responsable_actions = get_daily_active_actions_for_responsable(
            db,
            responsable_email,
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
    return await run_daily_grouped_reminders_service(
        db,
        dry_run=False,
        test_email=None,
    )


def get_daily_active_actions_for_responsable(db, email: str):
    normalized_email = normalize_reminder_email(email)
    actions = (
        db.query(Action)
        .filter(get_action_active_predicate(Action))
        .filter(func.lower(func.coalesce(Action.email_responsable, "")) == normalized_email)
        .order_by(Action.priority_index.desc().nullslast(), Action.due_date.asc())
        .all()
    )

    return filter_daily_active_assigned_actions(actions)


async def debug_daily_reminders_for_user_service(db, email: str):
    normalized_email = normalize_reminder_email(email)

    actions = (
        db.query(Action)
        .filter(func.lower(func.coalesce(Action.email_responsable, "")) == normalized_email)
        .order_by(Action.priority_index.desc().nullslast(), Action.due_date.asc())
        .all()
    )

    debug_actions = [
        build_reminder_action_debug_payload(action, normalized_email)
        for action in actions
    ]

    return {
        "email": normalized_email,
        "actions_count": len(debug_actions),
        "eligible_count": sum(1 for action in debug_actions if action["eligible"]),
        "actions": debug_actions,
        "smtp_config": get_smtp_config_diagnostics(),
    }


async def run_daily_grouped_reminders_service(
    db,
    dry_run: bool = True,
    test_email: str | None = None,
):
    trigger_actions = filter_daily_reminder_actions(
        get_daily_reminder_candidate_actions(db)
    )
    trigger_groups = group_actions_by_responsable(trigger_actions)

    recipients = []

    for responsable_email, highlighted_actions in trigger_groups.items():
        included_actions = get_daily_active_actions_for_responsable(
            db,
            responsable_email,
        )
        recipients.append(
            {
                "email": responsable_email,
                "send_to": test_email or responsable_email,
                "actions_count": len(included_actions),
                "action_ids": [action.id for action in included_actions],
                "trigger_action_ids": [action.id for action in highlighted_actions],
            }
        )

    response = {
        "message": "Daily reminder run prepared",
        "dry_run": dry_run,
        "test_email": test_email,
        "matched_recipients": len(recipients),
        "recipients": recipients,
        "smtp_config": get_smtp_config_diagnostics(),
        "sent_emails": 0,
        "failed_emails": 0,
        "errors": [],
    }

    if dry_run:
        logger.info(
            "Daily reminder dry-run recipients=%s action_count=%s",
            len(recipients),
            sum(recipient["actions_count"] for recipient in recipients),
        )
        return response

    for recipient in recipients:
        responsable_email = recipient["email"]
        send_to = recipient["send_to"]
        included_actions = get_daily_active_actions_for_responsable(
            db,
            responsable_email,
        )

        if not included_actions:
            continue

        for action in included_actions:
            enrich_action_priority(action)

        responsable_name = included_actions[0].responsable or responsable_email
        subject_prefix = "[TEST] " if test_email else ""
        subject = f"{subject_prefix}[Action Plan] Daily Follow-up - {responsable_name}"
        html_body = build_responsable_summary_email(
            responsable=responsable_name,
            reminder_action_ids=recipient["trigger_action_ids"],
            actions=included_actions,
        )

        logger.info(
            "Daily reminder send recipient=%s send_to=%s action_count=%s",
            responsable_email,
            send_to,
            len(included_actions),
        )

        send_result = send_email_with_diagnostics(
            to_email=send_to,
            subject=subject,
            html_body=html_body,
        )
        sent = bool(send_result.get("success"))

        if sent:
            now = datetime.datetime.now(datetime.timezone.utc)

            for action in included_actions:
                action.last_reminder_sent_at = now

            db.commit()
            response["sent_emails"] += 1
            logger.info(
                "Daily reminder send success recipient=%s action_count=%s",
                responsable_email,
                len(included_actions),
            )
            continue

        response["failed_emails"] += 1
        db.rollback()
        response["errors"].append(
            {
                "email": responsable_email,
                "message": "SMTP send failed",
                "error_type": send_result.get("error_type"),
                "error_detail": send_result.get("error_detail"),
                "smtp_code": send_result.get("smtp_code"),
                "smtp_response": send_result.get("smtp_response"),
                "diagnostics": send_result.get("diagnostics"),
                "suggestion": send_result.get("suggestion"),
            }
        )
        logger.error(
            "Daily reminder send failed recipient=%s action_count=%s",
            responsable_email,
            len(included_actions),
        )

    response["message"] = "Daily reminder run completed"
    return response
