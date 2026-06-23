import datetime
import json
import logging
import os
from html import escape

from sqlalchemy import func

from app.config.organisation_database import OrganisationSessionLocal
from app.models.action import Action
from app.models.action_event_log import ActionEventLog
from app.services.action_event_log_service import log_action_event
from app.services.action_status_logic_service import (
    get_action_active_predicate,
    normalize_action_status,
)
from app.services.email_service import send_email_with_diagnostics
from app.services.organisation_hierarchy_service import (
    normalize_email,
    resolve_escalation_recipients,
)
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


def _escape(value):
    return escape(str(value if value not in [None, ""] else "-"))


def _json_safe(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return str(value)


def _compact_chain(chain_result):
    chain = []
    for entry in (chain_result or {}).get("chain") or []:
        person = entry.get("person") or entry
        chain.append({
            "level": entry.get("level"),
            "people_id": person.get("people_id"),
            "personne": person.get("personne") or person.get("display_name"),
            "email": normalize_email(person.get("email")),
            "role": person.get("role") or person.get("job_title"),
            "role_cible": person.get("role_cible"),
            "hr_site": person.get("hr_site") or person.get("site"),
            "manager_hierarchique": person.get("manager_hierarchique"),
            "manager_fonctionnel": person.get("manager_fonctionnel"),
            "role_parent": person.get("role_parent"),
        })
    return chain


def _build_escalation_event_metadata(action, resolution):
    return {
        "hierarchy_source_used": resolution.get("hierarchy_source_used"),
        "fallback_used": resolution.get("fallback_used"),
        "to": resolution.get("to_email"),
        "cc": resolution.get("cc_emails") or [],
        "escalation_level": resolution.get("level"),
        "missing_reason": resolution.get("missing_reason"),
        "responsible_chain": _compact_chain(resolution.get("responsible_chain")),
        "requester_chain": _compact_chain(resolution.get("requester_chain")),
        "organisation_warnings": resolution.get("organisation", {}).get("warnings", []),
        "fallback_warnings": (resolution.get("fallback") or {}).get("warnings", []),
    }


def _build_sent_event_details(to_email, cc_emails, resolution, action):
    first_line = f"Escalation email sent to {to_email}"
    if cc_emails:
        first_line += f" cc {', '.join(cc_emails)}"

    metadata = _build_escalation_event_metadata(action, resolution)
    return (
        first_line
        + "\nmetadata: "
        + json.dumps(metadata, default=_json_safe, ensure_ascii=False)
    )


def _build_missing_event_details(resolution, action):
    metadata = _build_escalation_event_metadata(action, resolution)
    return (
        "Escalation email recipient could not be resolved."
        + "\nmetadata: "
        + json.dumps(metadata, default=_json_safe, ensure_ascii=False)
    )


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


async def send_due_escalation_notifications_service(
    db,
    directory_db=None,
    organisation_db=None,
    today=None,
):
    today = today or datetime.date.today()
    emails_enabled = _read_escalation_emails_enabled()
    owned_organisation_db = None
    if organisation_db is None and OrganisationSessionLocal is not None:
        owned_organisation_db = OrganisationSessionLocal()
        organisation_db = owned_organisation_db

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

    try:
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

            resolution = resolve_escalation_recipients(
                action,
                organisation_db=organisation_db,
                directory_db=directory_db,
            )
            to_email = resolution.get("to_email")
            cc_emails = resolution.get("cc_emails") or []

            for warning in resolution.get("warnings") or []:
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

            if resolution.get("fallback_used"):
                log_action_event(
                    db=db,
                    action_id=action.id,
                    event_type="action_escalation_hierarchy_fallback",
                    old_value=None,
                    new_value=str(level),
                    details=json.dumps(
                        _build_escalation_event_metadata(action, resolution),
                        default=_json_safe,
                        ensure_ascii=False,
                    ),
                    created_by="system",
                )

            if not to_email:
                response["missing_recipient"] += 1
                log_action_event(
                    db=db,
                    action_id=action.id,
                    event_type="action_escalation_recipient_missing",
                    old_value=None,
                    new_value=str(level),
                    details=_build_missing_event_details(resolution, action),
                    created_by="system",
                )
                db.commit()
                continue

            if not emails_enabled:
                db.rollback()
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
                    details=_build_sent_event_details(to_email, cc_emails, resolution, action),
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
    finally:
        if owned_organisation_db is not None:
            owned_organisation_db.close()

    return response
