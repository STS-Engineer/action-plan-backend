import datetime
import json
import os
import re

from sqlalchemy import text

from app.config.organisation_database import is_organisation_db_configured
from app.models.action import Action
from app.models.action_escalation_notification import ActionEscalationNotification
from app.models.action_event_log import ActionEventLog
from app.services.action_escalation_service import ESCALATION_EMAIL_EVENT_TYPE
from app.services.organisation_hierarchy_service import (
    OLIVIER_EMAIL,
    ORGANISATION_SOURCE,
    UNAVAILABLE_SOURCE,
    normalize_email,
    resolve_escalation_recipients,
)


PRODUCTION_HIERARCHY_SOURCE = {
    "database": "Organisation_DB",
    "view": "public.v_personne_complete",
    "person_lookup": "lower(trim(email))",
    "manager_lookup": "manager_hierarchique name -> personne",
}
DETAILED_ESCALATION_EMAIL_EVENT_TYPE = "action_escalation_email_sent"

_SENT_DETAILS_RE = re.compile(
    r"Escalation email sent to\s+(?P<to>[^\s]+)(?:\s+cc\s+(?P<cc>[^\n]+))?",
    flags=re.IGNORECASE,
)


def _json_value(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return value


def _action_to_dict(action: Action | None):
    if not action:
        return None

    return {
        "id": action.id,
        "titre": action.titre,
        "status": action.status,
        "responsable": action.responsable,
        "email_responsable": normalize_email(action.email_responsable),
        "demandeur": action.demandeur,
        "email_demandeur": normalize_email(action.email_demandeur),
        "due_date": _json_value(action.due_date),
        "escalation_level": action.escalation_level,
        "priority_index": action.priority_index,
        "is_deleted": action.is_deleted,
    }


def _event_to_dict(event: ActionEventLog):
    return {
        "id": event.id,
        "event_type": event.event_type,
        "old_value": event.old_value,
        "new_value": event.new_value,
        "details": event.details,
        "created_by": event.created_by,
        "created_at": _json_value(event.created_at),
    }


def _parse_sent_recipients(details: str | None):
    if not details:
        return {"sent_to": None, "cc": []}

    match = _SENT_DETAILS_RE.search(details)
    if not match:
        return {"sent_to": None, "cc": []}

    cc_raw = match.group("cc") or ""
    return {
        "sent_to": normalize_email(match.group("to")),
        "cc": [
            normalize_email(item.strip())
            for item in cc_raw.split(",")
            if normalize_email(item.strip())
        ],
    }


def _parse_event_metadata(details: str | None):
    if not details:
        return None

    marker = "metadata:"
    if marker in details:
        details = details.split(marker, 1)[1].strip()

    if not details.startswith("{"):
        return None

    try:
        return json.loads(details)
    except json.JSONDecodeError:
        return None


def _summary_frontend_link():
    base_url = (
        os.getenv("FRONTEND_URL")
        or os.getenv("FRONTEND_BASE_URL")
        or "http://localhost:5173"
    ).rstrip("/")

    return f"{base_url}/home?tab=escalations"


def _sent_at_group_key(value):
    if not value:
        return None

    if isinstance(value, datetime.datetime):
        return value.replace(microsecond=0).isoformat()

    return str(value)


def _recipient_involvement(details, resolution, target_email=OLIVIER_EMAIL):
    target_email = normalize_email(target_email)
    recipients = _parse_sent_recipients(details)
    metadata = _parse_event_metadata(details) or {}
    reasons = []

    if target_email and recipients["sent_to"] == target_email:
        reasons.append("event_log_sent_to")
    if target_email and target_email in recipients["cc"]:
        reasons.append("event_log_cc")
    if target_email and normalize_email(metadata.get("to")) == target_email:
        reasons.append("event_metadata_to")
    if target_email and target_email in [normalize_email(item) for item in metadata.get("cc") or []]:
        reasons.append("event_metadata_cc")
    if target_email and normalize_email(resolution.get("to_email")) == target_email:
        reasons.append("current_resolution_to")
    if target_email and target_email in [normalize_email(item) for item in resolution.get("cc_emails") or []]:
        reasons.append("current_resolution_cc")

    return {
        "involved": bool(reasons),
        "reasons": reasons,
        "sent_to": recipients["sent_to"],
        "cc": recipients["cc"],
        "metadata": metadata,
    }


def _warning_types(warnings):
    return sorted(
        {
            warning.get("type")
            for warning in warnings or []
            if isinstance(warning, dict) and warning.get("type")
        }
    )


def _chain_values(chain_result, field_name):
    values = []

    lookup_selected = ((chain_result or {}).get("lookup") or {}).get("selected") or {}
    if lookup_selected.get(field_name):
        values.append({
            "level": 0,
            "personne": lookup_selected.get("personne"),
            "email": normalize_email(lookup_selected.get("email")),
            field_name: lookup_selected.get(field_name),
        })

    for entry in (chain_result or {}).get("chain") or []:
        person = entry.get("person") or {}
        if person.get(field_name):
            values.append({
                "level": entry.get("level"),
                "personne": person.get("personne"),
                "email": normalize_email(person.get("email")),
                field_name: person.get(field_name),
            })

    return values


def _combined_chain_values(resolution, field_name):
    return [
        *[
            {"side": "responsible", **item}
            for item in _chain_values(resolution.get("responsible_chain"), field_name)
        ],
        *[
            {"side": "requester", **item}
            for item in _chain_values(resolution.get("requester_chain"), field_name)
        ],
    ]


def get_escalation_source_status_service(organisation_db):
    last_error = None
    connection_ok = False
    view_accessible = False

    if not is_organisation_db_configured() or organisation_db is None:
        last_error = "organisation_db_unavailable"
    else:
        try:
            organisation_db.execute(text("SELECT 1")).scalar()
            connection_ok = True
            organisation_db.execute(
                text("SELECT 1 FROM public.v_personne_complete LIMIT 1")
            ).first()
            view_accessible = True
        except Exception as exc:
            last_error = {
                "error_type": type(exc).__name__,
                "error_detail": str(exc),
            }

    return {
        "organisation_db_configured": is_organisation_db_configured(),
        "organisation_db_connection_ok": connection_ok,
        "view": "public.v_personne_complete",
        "view_accessible": view_accessible,
        "fallback_enabled": False,
        "last_error": last_error,
    }


def get_escalation_hierarchy_debug_service(db, organisation_db, action_id: int):
    action = db.query(Action).filter(Action.id == action_id).first()

    if not action:
        return {
            "found": False,
            "action_id": action_id,
            "hierarchy_source_used": UNAVAILABLE_SOURCE,
            "fallback_used": False,
            "fallback_source_removed": True,
            "source_table_view_used_by_production": PRODUCTION_HIERARCHY_SOURCE,
        }

    resolution = resolve_escalation_recipients(action, organisation_db=organisation_db)

    return {
        "found": True,
        "action": _action_to_dict(action),
        "source_table_view_used_by_production": PRODUCTION_HIERARCHY_SOURCE,
        "hierarchy_source_used": resolution.get("hierarchy_source_used"),
        "fallback_used": False,
        "fallback_source_removed": True,
        "responsible_lookup": resolution.get("responsible_lookup"),
        "requester_lookup": resolution.get("requester_lookup"),
        "responsible_chain": resolution.get("responsible_chain"),
        "requester_chain": resolution.get("requester_chain"),
        "selected_recipients": {
            "to": resolution.get("to_email"),
            "cc": resolution.get("cc_emails") or [],
            "escalation_level": resolution.get("level"),
            "hierarchy_source_used": resolution.get("hierarchy_source_used"),
            "fallback_used": False,
            "missing_reason": resolution.get("missing_reason"),
        },
        "warnings": resolution.get("warnings") or [],
        "warning_types": _warning_types(resolution.get("warnings")),
        "manager_hierarchique_values": _combined_chain_values(
            resolution,
            "manager_hierarchique",
        ),
        "manager_fonctionnel_values": _combined_chain_values(
            resolution,
            "manager_fonctionnel",
        ),
        "role_parent_values": _combined_chain_values(resolution, "role_parent"),
        "olivier_reached": {
            "selected_recipient": bool(
                normalize_email(resolution.get("to_email")) == OLIVIER_EMAIL
                or OLIVIER_EMAIL in [
                    normalize_email(item)
                    for item in resolution.get("cc_emails") or []
                ]
            ),
            "organisation_chain": bool(resolution.get("target_reached")),
        },
        "raw_resolution": resolution,
    }


def get_olivier_escalation_audit_service(db, organisation_db):
    rows = (
        db.query(ActionEventLog, Action)
        .outerjoin(Action, Action.id == ActionEventLog.action_id)
        .filter(ActionEventLog.event_type.ilike("action_escalation%"))
        .order_by(ActionEventLog.created_at.desc(), ActionEventLog.id.desc())
        .all()
    )
    events = []

    for event, action in rows:
        if not action:
            continue

        resolution = resolve_escalation_recipients(action, organisation_db=organisation_db)
        involvement = _recipient_involvement(event.details, resolution)
        if not involvement["involved"]:
            continue

        level = event.new_value or getattr(action, "escalation_level", None)
        subject = (
            f"Action Plan - You have pending escalations"
            if event.event_type == ESCALATION_EMAIL_EVENT_TYPE
            else None
        )

        events.append({
            "action_id": action.id,
            "action_title": action.titre,
            "responsable": action.responsable,
            "email_responsable": normalize_email(action.email_responsable),
            "demandeur": action.demandeur,
            "email_demandeur": normalize_email(action.email_demandeur),
            "escalation_level": level,
            "sent_to": involvement["sent_to"],
            "cc": involvement["cc"],
            "subject": subject,
            "sent_at": _json_value(event.created_at),
            "event_type": event.event_type,
            "event_payload": _event_to_dict(event),
            "event_metadata": involvement["metadata"],
            "reason_olivier_was_included": involvement["reasons"],
            "resolved_hierarchy_chain": {
                "responsible": resolution.get("responsible_chain"),
                "requester": resolution.get("requester_chain"),
            },
            "hierarchy_source_used": resolution.get("hierarchy_source_used"),
            "fallback_used": False,
            "fallback_source_removed": True,
        })

    return {
        "target_email": OLIVIER_EMAIL,
        "production_hierarchy_source": PRODUCTION_HIERARCHY_SOURCE,
        "fallback_enabled": False,
        "fallback_source_removed": True,
        "audit_basis": (
            "Escalation events are read from action_event_log. New production "
            "escalations use in-app notification rows and summary emails."
        ),
        "total_escalation_events_scanned": len(rows),
        "events_count": len(events),
        "emails_count": sum(1 for item in events if item["event_type"] == ESCALATION_EMAIL_EVENT_TYPE),
        "events": events,
        "diagnosis": {
            "production_source": ORGANISATION_SOURCE,
            "valid_hierarchy_source_values": [ORGANISATION_SOURCE, UNAVAILABLE_SOURCE],
            "fallback_enabled": False,
        },
    }


def get_escalation_email_audit_service(db, limit: int = 100):
    summary_events = (
        db.query(ActionEventLog)
        .filter(ActionEventLog.event_type == ESCALATION_EMAIL_EVENT_TYPE)
        .order_by(ActionEventLog.created_at.desc(), ActionEventLog.id.desc())
        .limit(limit)
        .all()
    )
    detailed_events = (
        db.query(ActionEventLog)
        .filter(ActionEventLog.event_type == DETAILED_ESCALATION_EMAIL_EVENT_TYPE)
        .order_by(ActionEventLog.created_at.desc(), ActionEventLog.id.desc())
        .limit(limit)
        .all()
    )
    sent_notifications = (
        db.query(ActionEscalationNotification)
        .filter(ActionEscalationNotification.last_summary_email_sent_at.isnot(None))
        .order_by(
            ActionEscalationNotification.last_summary_email_sent_at.desc(),
            ActionEscalationNotification.id.desc(),
        )
        .limit(limit)
        .all()
    )
    link = _summary_frontend_link()
    email_groups = {}

    for event in summary_events:
        metadata = _parse_event_metadata(event.details) or {}
        recipient_email = normalize_email(metadata.get("to"))
        notification_id = metadata.get("notification_id")
        pending_count = int(metadata.get("summary_pending_count") or 0)
        key = (
            recipient_email,
            _sent_at_group_key(event.created_at),
        )

        group = email_groups.setdefault(key, {
            "recipient_email": recipient_email,
            "pending_count": pending_count,
            "email_subject": f"Action Plan - You have {pending_count} pending escalations",
            "link": link,
            "sent_at": _json_value(event.created_at),
            "notification_ids": [],
            "action_ids": [],
            "event_ids": [],
            "event_type": ESCALATION_EMAIL_EVENT_TYPE,
            "detailed_content_in_email": False,
        })

        if notification_id and notification_id not in group["notification_ids"]:
            group["notification_ids"].append(notification_id)
        if event.action_id and event.action_id not in group["action_ids"]:
            group["action_ids"].append(event.action_id)
        group["event_ids"].append(event.id)

    for notification in sent_notifications:
        recipient_email = normalize_email(notification.recipient_email)
        key = (
            recipient_email,
            _sent_at_group_key(notification.last_summary_email_sent_at),
        )

        group = email_groups.setdefault(key, {
            "recipient_email": recipient_email,
            "pending_count": 0,
            "email_subject": None,
            "link": link,
            "sent_at": _json_value(notification.last_summary_email_sent_at),
            "notification_ids": [],
            "action_ids": [],
            "event_ids": [],
            "event_type": ESCALATION_EMAIL_EVENT_TYPE,
            "detailed_content_in_email": False,
        })

        if notification.id not in group["notification_ids"]:
            group["notification_ids"].append(notification.id)
        if notification.action_id not in group["action_ids"]:
            group["action_ids"].append(notification.action_id)

    emails = []
    for group in email_groups.values():
        if not group["pending_count"]:
            group["pending_count"] = len(group["notification_ids"]) or len(group["action_ids"])
        if not group["email_subject"]:
            group["email_subject"] = (
                f"Action Plan - You have {group['pending_count']} pending escalations"
            )
        group["notification_ids"] = sorted(group["notification_ids"])
        group["action_ids"] = sorted(group["action_ids"])
        emails.append(group)

    emails.sort(key=lambda item: str(item.get("sent_at") or ""), reverse=True)

    return {
        "summary_event_type": ESCALATION_EMAIL_EVENT_TYPE,
        "detailed_action_email_event_type": DETAILED_ESCALATION_EMAIL_EVENT_TYPE,
        "detailed_action_email_events_count": len(detailed_events),
        "latest_detailed_action_email_events": [
            _event_to_dict(event)
            for event in detailed_events[:10]
        ],
        "summary_email_events_count": len(summary_events),
        "notification_rows_with_summary_sent_count": len(sent_notifications),
        "detailed_content_in_email": False,
        "emails_count": len(emails),
        "emails": emails,
    }
