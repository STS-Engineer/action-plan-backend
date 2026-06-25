import datetime
import json
import logging
import os
from collections import defaultdict

from sqlalchemy import func

from app.config.organisation_database import OrganisationSessionLocal
from app.models.action import Action
from app.models.action_escalation_notification import ActionEscalationNotification
from app.services.action_event_log_service import log_action_event
from app.services.action_status_logic_service import (
    get_action_active_predicate,
    normalize_action_status,
)
from app.services.email_service import send_email_with_diagnostics
from app.services.organisation_hierarchy_service import (
    ORGANISATION_SOURCE,
    UNAVAILABLE_SOURCE,
    normalize_email,
    resolve_escalation_recipients,
)


logger = logging.getLogger(__name__)
ESCALATION_EMAIL_EVENT_TYPE = "action_escalation_summary_email_sent"
ESCALATION_HIERARCHY_ERROR_EVENT_TYPE = "action_escalation_hierarchy_error"
PENDING_STATUS = "pending"


def _read_escalation_emails_enabled() -> bool:
    return os.getenv("ESCALATION_EMAILS_ENABLED", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def _now_utc():
    return datetime.datetime.now(datetime.timezone.utc)


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


def _json_safe(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return str(value)


def _compact_chain(chain_result):
    chain = []
    for entry in (chain_result or {}).get("chain") or []:
        person = entry.get("person") or {}
        chain.append({
            "level": entry.get("level"),
            "people_id": person.get("people_id"),
            "personne": person.get("personne"),
            "email": normalize_email(person.get("email")),
            "role": person.get("role"),
            "role_cible": person.get("role_cible"),
            "hr_site": person.get("hr_site"),
            "manager_hierarchique": person.get("manager_hierarchique"),
            "manager_fonctionnel": person.get("manager_fonctionnel"),
            "role_parent": person.get("role_parent"),
        })
    return chain


def _resolution_metadata(action, resolution):
    return {
        "hierarchy_source_used": resolution.get("hierarchy_source_used"),
        "fallback_used": False,
        "fallback_source_removed": True,
        "to": resolution.get("to_email"),
        "cc": resolution.get("cc_emails") or [],
        "escalation_level": resolution.get("level"),
        "missing_reason": resolution.get("missing_reason"),
        "responsible_chain": _compact_chain(resolution.get("responsible_chain")),
        "requester_chain": _compact_chain(resolution.get("requester_chain")),
        "warnings": resolution.get("warnings") or [],
        "action_id": getattr(action, "id", None),
    }


def _build_summary_frontend_url():
    base_url = (
        os.getenv("FRONTEND_URL")
        or os.getenv("FRONTEND_BASE_URL")
        or "http://localhost:5173"
    ).rstrip("/")

    return f"{base_url}/home?tab=escalations"


def _build_summary_email(pending_count: int):
    escalation_url = _build_summary_frontend_url()

    return f"""
    <div style="font-family:Arial,sans-serif;color:#1f2937;">
      <p>Hello,</p>
      <p>You have <strong>{pending_count}</strong> pending escalations in Action Plan.</p>
      <p>Please review them here:</p>
      <p>
        <a href="{escalation_url}" style="color:#1d4ed8;font-weight:700;">
          {escalation_url}
        </a>
      </p>
    </div>
    """


def _build_summary_event_details(notification, recipient_email: str, pending_count: int):
    return json.dumps(
        {
            "hierarchy_source_used": notification.hierarchy_source_used,
            "fallback_used": False,
            "fallback_source_removed": True,
            "to": normalize_email(recipient_email),
            "cc": [],
            "escalation_level": notification.escalation_level,
            "missing_reason": None,
            "responsible_chain": notification.responsible_chain or [],
            "requester_chain": notification.requester_chain or [],
            "summary_pending_count": pending_count,
            "notification_id": notification.id,
        },
        default=_json_safe,
        ensure_ascii=False,
    )


def _get_existing_pending_notification(db, action_id: int, recipient_email: str, level: int):
    return (
        db.query(ActionEscalationNotification)
        .filter(ActionEscalationNotification.action_id == action_id)
        .filter(ActionEscalationNotification.recipient_email == recipient_email)
        .filter(ActionEscalationNotification.escalation_level == level)
        .filter(ActionEscalationNotification.status == PENDING_STATUS)
        .first()
    )


def _upsert_pending_notification(db, action, resolution):
    recipient_email = normalize_email(resolution.get("to_email"))
    if not recipient_email:
        return None, False

    level = int(resolution.get("level") or 0)
    notification = _get_existing_pending_notification(
        db,
        action.id,
        recipient_email,
        level,
    )
    created = notification is None

    if notification is None:
        notification = ActionEscalationNotification(
            action_id=action.id,
            recipient_email=recipient_email,
            escalation_level=level,
            status=PENDING_STATUS,
        )
        db.add(notification)

    notification.cc_emails = resolution.get("cc_emails") or []
    notification.hierarchy_source_used = ORGANISATION_SOURCE
    notification.responsible_chain = _compact_chain(resolution.get("responsible_chain"))
    notification.requester_chain = _compact_chain(resolution.get("requester_chain"))
    notification.updated_at = _now_utc()

    return notification, created


def _log_hierarchy_error(db, action, resolution):
    details = json.dumps(
        _resolution_metadata(action, resolution),
        default=_json_safe,
        ensure_ascii=False,
    )
    log_action_event(
        db=db,
        action_id=action.id,
        event_type=ESCALATION_HIERARCHY_ERROR_EVENT_TYPE,
        old_value=None,
        new_value=str(resolution.get("level")),
        details=details,
        created_by="system",
    )


def _summary_email_sent_today(notification, today: datetime.date):
    sent_at = notification.last_summary_email_sent_at
    if not sent_at:
        return False

    if sent_at.tzinfo is None:
        sent_at = sent_at.replace(tzinfo=datetime.timezone.utc)

    return sent_at >= _start_of_day_utc(today)


def _group_pending_notifications(db):
    pending = (
        db.query(ActionEscalationNotification)
        .join(Action, Action.id == ActionEscalationNotification.action_id)
        .filter(ActionEscalationNotification.status == PENDING_STATUS)
        .filter(get_action_active_predicate(Action))
        .order_by(ActionEscalationNotification.recipient_email.asc())
        .all()
    )
    grouped = defaultdict(list)

    for notification in pending:
        grouped[normalize_email(notification.recipient_email)].append(notification)

    return grouped


async def send_due_escalation_notifications_service(
    db,
    organisation_db=None,
    today=None,
    **_ignored,
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
        "hierarchy_source": ORGANISATION_SOURCE if organisation_db is not None else UNAVAILABLE_SOURCE,
        "fallback_enabled": False,
        "checked_actions": len(actions),
        "eligible_actions": 0,
        "pending_created": 0,
        "pending_updated": 0,
        "hierarchy_errors": 0,
        "missing_recipient": 0,
        "summary_recipients": 0,
        "summary_emails_sent": 0,
        "summary_emails_skipped_already_sent_today": 0,
        "summary_emails_failed": 0,
        "skipped_not_overdue": 0,
        "warnings": [],
        "errors": [],
    }

    try:
        for action in actions:
            if normalize_action_status(action.status) == "closed":
                continue

            if not _is_overdue_action(action, today):
                response["skipped_not_overdue"] += 1
                continue

            response["eligible_actions"] += 1
            resolution = resolve_escalation_recipients(action, organisation_db=organisation_db)

            if resolution.get("hierarchy_source_used") == UNAVAILABLE_SOURCE:
                response["hierarchy_errors"] += 1
                _log_hierarchy_error(db, action, resolution)
                db.commit()
                continue

            for warning in resolution.get("warnings") or []:
                logger.warning(
                    "Action escalation warning action_id=%s level=%s warning=%s",
                    action.id,
                    action.escalation_level,
                    warning,
                )
                response["warnings"].append({
                    "action_id": action.id,
                    "level": action.escalation_level,
                    "warning": warning,
                })

            if not resolution.get("to_email"):
                response["missing_recipient"] += 1
                _log_hierarchy_error(db, action, resolution)
                db.commit()
                continue

            _, created = _upsert_pending_notification(db, action, resolution)
            if created:
                response["pending_created"] += 1
            else:
                response["pending_updated"] += 1

            db.commit()

        grouped = _group_pending_notifications(db)
        response["summary_recipients"] = len(grouped)

        if not emails_enabled:
            return response

        for recipient_email, notifications in grouped.items():
            if not recipient_email:
                continue

            if all(_summary_email_sent_today(notification, today) for notification in notifications):
                response["summary_emails_skipped_already_sent_today"] += 1
                continue

            send_result = send_email_with_diagnostics(
                to_email=recipient_email,
                subject=f"Action Plan - You have {len(notifications)} pending escalations",
                html_body=_build_summary_email(len(notifications)),
            )

            if send_result.get("success"):
                sent_at = _now_utc()
                for notification in notifications:
                    notification.last_summary_email_sent_at = sent_at
                    notification.updated_at = sent_at
                    log_action_event(
                        db=db,
                        action_id=notification.action_id,
                        event_type=ESCALATION_EMAIL_EVENT_TYPE,
                        old_value=None,
                        new_value=str(notification.escalation_level),
                        details=_build_summary_event_details(
                            notification,
                            recipient_email,
                            len(notifications),
                        ),
                        created_by="system",
                    )

                db.commit()
                response["summary_emails_sent"] += 1
                continue

            db.rollback()
            response["summary_emails_failed"] += 1
            response["errors"].append({
                "recipient_email": recipient_email,
                "error_type": send_result.get("error_type"),
                "error_detail": send_result.get("error_detail"),
            })
    finally:
        if owned_organisation_db is not None:
            owned_organisation_db.close()

    return response
