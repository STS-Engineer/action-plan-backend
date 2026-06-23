import datetime
import json
import re

from app.models.action import Action
from app.models.action_event_log import ActionEventLog
from app.services.action_escalation_service import ESCALATION_EMAIL_EVENT_TYPE
from app.services.organisation_hierarchy_service import (
    COMPANY_MEMBERS_SOURCE,
    OLIVIER_EMAIL,
    ORGANISATION_SOURCE,
    normalize_email,
    resolve_escalation_recipients,
    resolve_with_company_members,
    resolve_with_organisation,
)


PRODUCTION_HIERARCHY_SOURCE = {
    "database": "Organisation_DB",
    "view": "public.v_personne_complete",
    "person_lookup": "lower(trim(email))",
    "manager_lookup": "manager_hierarchique name -> personne",
}
FALLBACK_HIERARCHY_SOURCE = {
    "database_env": "DIRECTORY_DB_NAME",
    "table": "company_members",
    "person_lookup": "company_members.email",
    "manager_lookup": "company_members.manager_email -> company_members.email",
}

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
            for item in _chain_values(
                resolution.get("organisation", {}).get("responsible_chain"),
                field_name,
            )
        ],
        *[
            {"side": "requester", **item}
            for item in _chain_values(
                resolution.get("organisation", {}).get("requester_chain"),
                field_name,
            )
        ],
    ]


def _debug_response_for_action(action, organisation_db, directory_db):
    organisation_resolution = resolve_with_organisation(action, organisation_db)
    old_company_members_resolution = resolve_with_company_members(action, directory_db)
    final_resolution = resolve_escalation_recipients(
        action,
        organisation_db=organisation_db,
        directory_db=directory_db,
    )

    return {
        "action": _action_to_dict(action),
        "source_table_view_used_by_production": PRODUCTION_HIERARCHY_SOURCE,
        "fallback_source": FALLBACK_HIERARCHY_SOURCE,
        "responsible_lookup": organisation_resolution.get("responsible_lookup"),
        "requester_lookup": organisation_resolution.get("requester_lookup"),
        "responsible_chain": organisation_resolution.get("responsible_chain"),
        "requester_chain": organisation_resolution.get("requester_chain"),
        "selected_recipients": {
            "to": final_resolution.get("to_email"),
            "cc": final_resolution.get("cc_emails") or [],
            "escalation_level": final_resolution.get("level"),
            "hierarchy_source_used": final_resolution.get("hierarchy_source_used"),
            "fallback_used": final_resolution.get("fallback_used"),
            "missing_reason": final_resolution.get("missing_reason"),
        },
        "fallback_used": final_resolution.get("fallback_used"),
        "old_company_members_chain_for_comparison": {
            "responsible_chain": old_company_members_resolution.get("responsible_chain"),
            "requester_chain": old_company_members_resolution.get("requester_chain"),
            "selected_to": old_company_members_resolution.get("to_email"),
            "selected_cc": old_company_members_resolution.get("cc_emails") or [],
            "missing_reason": old_company_members_resolution.get("missing_reason"),
        },
        "warnings": final_resolution.get("warnings") or [],
        "warning_types": _warning_types(final_resolution.get("warnings")),
        "manager_hierarchique_values": _combined_chain_values(
            final_resolution,
            "manager_hierarchique",
        ),
        "manager_fonctionnel_values": _combined_chain_values(
            final_resolution,
            "manager_fonctionnel",
        ),
        "role_parent_values": _combined_chain_values(final_resolution, "role_parent"),
        "olivier_reached": {
            "production_resolution": bool(
                normalize_email(final_resolution.get("to_email")) == OLIVIER_EMAIL
                or OLIVIER_EMAIL in [
                    normalize_email(item)
                    for item in final_resolution.get("cc_emails") or []
                ]
            ),
            "organisation_chain": bool(organisation_resolution.get("target_reached")),
            "old_company_members_chain": bool(old_company_members_resolution.get("target_reached")),
        },
        "raw_resolution": final_resolution,
    }


def get_escalation_hierarchy_debug_service(
    db,
    directory_db,
    organisation_db,
    action_id: int,
):
    action = db.query(Action).filter(Action.id == action_id).first()

    if not action:
        return {
            "found": False,
            "action_id": action_id,
            "source_table_view_used_by_production": PRODUCTION_HIERARCHY_SOURCE,
            "fallback_source": FALLBACK_HIERARCHY_SOURCE,
        }

    return {
        "found": True,
        **_debug_response_for_action(action, organisation_db, directory_db),
    }


def get_olivier_escalation_audit_service(db, directory_db, organisation_db):
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

        final_resolution = resolve_escalation_recipients(
            action,
            organisation_db=organisation_db,
            directory_db=directory_db,
        )
        involvement = _recipient_involvement(event.details, final_resolution)
        if not involvement["involved"]:
            continue

        level = event.new_value or getattr(action, "escalation_level", None)
        subject = (
            f"[Action Plan] Escalation level {level} - {action.titre}"
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
                "responsible": final_resolution.get("responsible_chain"),
                "requester": final_resolution.get("requester_chain"),
            },
            "hierarchy_source_used": final_resolution.get("hierarchy_source_used"),
            "fallback_used": final_resolution.get("fallback_used"),
        })

    return {
        "target_email": OLIVIER_EMAIL,
        "production_hierarchy_source": PRODUCTION_HIERARCHY_SOURCE,
        "fallback_hierarchy_source": FALLBACK_HIERARCHY_SOURCE,
        "audit_basis": (
            "Escalation emails are read from action_event_log. New events include "
            "structured metadata; older events are parsed from the legacy details text."
        ),
        "total_escalation_events_scanned": len(rows),
        "events_count": len(events),
        "emails_count": sum(1 for item in events if item["event_type"] == ESCALATION_EMAIL_EVENT_TYPE),
        "events": events,
        "diagnosis": {
            "production_source": ORGANISATION_SOURCE,
            "fallback_source": COMPANY_MEMBERS_SOURCE,
            "note": (
                "Production now resolves escalation chains through public.v_personne_complete "
                "and uses company_members only when the Organisation source cannot resolve "
                "the required recipient."
            ),
        },
    }
