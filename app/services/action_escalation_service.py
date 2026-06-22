import datetime
import logging
import os
from html import escape

from sqlalchemy import func

from app.models.action import Action
from app.models.action_event_log import ActionEventLog
from app.services.action_event_log_service import log_action_event
from app.services.action_status_logic_service import (
    get_action_active_predicate,
    normalize_action_status,
)
from app.services.directory_service import get_manager_chain, normalize_email
from app.services.email_service import send_email_with_diagnostics
from app.utils.action_links import build_action_frontend_url


logger = logging.getLogger(__name__)
ESCALATION_EMAIL_EVENT_TYPE = "action_escalation_email_sent"


def _read_escalation_emails_enabled() -> bool:
    return os.getenv("ESCALATION_EMAILS_ENABLED", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def _start_of_day_utc(today: datetime.date | None = None):
    today = today or datetime.date.today()
    return datetime.datetime.combine(
        today,
        datetime.time.min,
        tzinfo=datetime.timezone.utc,
    )


def _is_overdue_action(action, today: datetime.date | None = None) -> bool:
    today = today or datetime.date.today()
    status = normalize_action_status(getattr(action, "status", None))
    due_date = getattr(action, "due_date", None)

    return status in {"overdue", "late"} or bool(due_date and due_date < today)


def _has_escalation_email_today(db, action_id: int, escalation_level: int, today=None) -> bool:
    return (
        db.query(ActionEventLog.id)
        .filter(ActionEventLog.action_id == action_id)
        .filter(ActionEventLog.event_type == ESCALATION_EMAIL_EVENT_TYPE)
        .filter(ActionEventLog.new_value == str(escalation_level))
        .filter(ActionEventLog.created_at >= _start_of_day_utc(today))
        .first()
        is not None
    )


def _member_email(member) -> str | None:
    return normalize_email(getattr(member, "email", None))


def _manager_chain(directory_db, email: str | None):
    if not directory_db or not email:
        return []

    try:
        return get_manager_chain(directory_db, email)
    except Exception as exc:
        logger.info("Escalation manager lookup unavailable email=%s reason=%s", email, exc)
        return []


def _resolve_escalation_recipients(action, directory_db) -> tuple[str | None, list[str], list[str]]:
    level = int(getattr(action, "escalation_level", None) or 0)
    responsible_email = normalize_email(getattr(action, "email_responsable", None))
    requester_email = normalize_email(getattr(action, "email_demandeur", None))
    warnings: list[str] = []

    if level <= 0:
        return None, [], ["escalation_level_zero"]

    if level == 1:
        if not responsible_email:
            return None, [], ["missing_responsible_email"]

        return responsible_email, [], warnings

    responsible_chain = _manager_chain(directory_db, responsible_email)

    if level == 2:
        cc_email = _member_email(responsible_chain[0]) if responsible_chain else None

        if responsible_email and not cc_email:
            warnings.append("missing_responsible_manager")

        if not requester_email:
            return None, [email for email in [cc_email] if email], ["missing_requester_email", *warnings]

        return requester_email, [email for email in [cc_email] if email], warnings

    requester_chain = _manager_chain(directory_db, requester_email)
    requester_manager_index = level - 3
    responsible_manager_index = level - 2
    to_email = (
        _member_email(requester_chain[requester_manager_index])
        if requester_manager_index < len(requester_chain)
        else None
    )
    cc_email = (
        _member_email(responsible_chain[responsible_manager_index])
        if responsible_manager_index < len(responsible_chain)
        else None
    )

    if requester_email and not to_email:
        warnings.append("missing_requester_manager_chain")

    if responsible_email and not cc_email:
        warnings.append("missing_responsible_manager_chain")

    return to_email, [email for email in [cc_email] if email], warnings


def _escape(value):
    return escape(str(value if value not in [None, ""] else "-"))


def build_escalation_email(action) -> str:
    action_url = escape(build_action_frontend_url(action.id), quote=True)

    return f"""
    <div style="font-family:Arial,sans-serif;color:#1f2937;">
      <table width="100%" cellpadding="0" cellspacing="0" style="background:#f3f4f6;">
        <tr>
          <td align="center" style="padding:20px;">
            <table width="100%" cellpadding="0" cellspacing="0" style="max-width:760px;background:#ffffff;border-radius:12px;overflow:hidden;">
              <tr>
                <td style="background:#991b1b;color:#ffffff;padding:24px;">
                  <h2 style="margin:0;font-size:24px;">Action Plan Escalation</h2>
                  <p style="margin:6px 0 0;color:#fee2e2;">Escalation level {_escape(action.escalation_level)}</p>
                </td>
              </tr>
              <tr>
                <td style="padding:24px;">
                  <p style="margin:0 0 16px;">
                    This action is overdue and requires escalation follow-up.
                  </p>
                  <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e5e7eb;border-radius:10px;overflow:hidden;">
                    <tr><td style="padding:12px;background:#f8fafc;font-weight:700;">Action</td><td style="padding:12px;">{_escape(action.titre)}</td></tr>
                    <tr><td style="padding:12px;background:#f8fafc;font-weight:700;">Responsible</td><td style="padding:12px;">{_escape(action.responsable)} ({_escape(action.email_responsable)})</td></tr>
                    <tr><td style="padding:12px;background:#f8fafc;font-weight:700;">Requester</td><td style="padding:12px;">{_escape(action.demandeur)} ({_escape(action.email_demandeur)})</td></tr>
                    <tr><td style="padding:12px;background:#f8fafc;font-weight:700;">Due date</td><td style="padding:12px;">{_escape(action.due_date)}</td></tr>
                    <tr><td style="padding:12px;background:#f8fafc;font-weight:700;">Priority</td><td style="padding:12px;">{_escape(action.priority_index)}</td></tr>
                  </table>
                  <p style="margin:20px 0;text-align:center;">
                    <a href="{action_url}" style="display:inline-block;background:#1d4ed8;color:#ffffff;text-decoration:none;padding:12px 18px;border-radius:8px;font-weight:700;">
                      View action
                    </a>
                  </p>
                </td>
              </tr>
            </table>
          </td>
        </tr>
      </table>
    </div>
    """


async def send_due_escalation_notifications_service(db, directory_db=None, today=None):
    today = today or datetime.date.today()
    emails_enabled = _read_escalation_emails_enabled()
    actions = (
        db.query(Action)
        .filter(get_action_active_predicate(Action))
        .filter(func.coalesce(Action.escalation_level, 0) > 0)
        .order_by(Action.escalation_level.desc(), Action.priority_index.desc().nullslast())
        .all()
    )

    response = {
        "enabled": emails_enabled,
        "checked_actions": len(actions),
        "eligible_actions": 0,
        "sent_emails": 0,
        "skipped_already_notified": 0,
        "skipped_not_overdue": 0,
        "missing_recipient": 0,
        "failed_emails": 0,
        "warnings": [],
        "errors": [],
    }

    for action in actions:
        level = int(action.escalation_level or 0)

        if normalize_action_status(action.status) == "closed":
            continue

        if not _is_overdue_action(action, today):
            response["skipped_not_overdue"] += 1
            continue

        response["eligible_actions"] += 1

        if _has_escalation_email_today(db, action.id, level, today):
            response["skipped_already_notified"] += 1
            continue

        to_email, cc_emails, warnings = _resolve_escalation_recipients(action, directory_db)

        for warning in warnings:
            logger.warning(
                "Action escalation warning action_id=%s level=%s warning=%s",
                action.id,
                level,
                warning,
            )
            response["warnings"].append({
                "action_id": action.id,
                "level": level,
                "warning": warning,
            })

        if not to_email:
            response["missing_recipient"] += 1
            log_action_event(
                db=db,
                action_id=action.id,
                event_type="action_escalation_recipient_missing",
                old_value=None,
                new_value=str(level),
                details="Escalation email recipient could not be resolved.",
                created_by="system",
            )
            db.commit()
            continue

        if not emails_enabled:
            continue

        send_result = send_email_with_diagnostics(
            to_email=to_email,
            cc_emails=cc_emails,
            subject=f"[Action Plan] Escalation level {level} - {action.titre}",
            html_body=build_escalation_email(action),
        )

        if send_result.get("success"):
            log_action_event(
                db=db,
                action_id=action.id,
                event_type=ESCALATION_EMAIL_EVENT_TYPE,
                old_value=None,
                new_value=str(level),
                details=(
                    f"Escalation email sent to {to_email}"
                    + (f" cc {', '.join(cc_emails)}" if cc_emails else "")
                ),
                created_by="system",
            )
            db.commit()
            response["sent_emails"] += 1
            continue

        db.rollback()
        response["failed_emails"] += 1
        response["errors"].append({
            "action_id": action.id,
            "level": level,
            "to_email": to_email,
            "error_type": send_result.get("error_type"),
            "error_detail": send_result.get("error_detail"),
        })

    return response
