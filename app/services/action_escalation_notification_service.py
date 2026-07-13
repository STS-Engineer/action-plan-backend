import datetime

from fastapi import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import joinedload

from app.models.action import Action
from app.models.action_escalation_notification import ActionEscalationNotification
from app.models.sujet import Sujet
from app.services.action_event_log_service import log_action_event
from app.services.action_status_logic_service import get_action_closed_state_predicate
from app.services.auth_service import is_admin
from app.services.organisation_hierarchy_service import normalize_email


PENDING_STATUS = "pending"
VALID_STATUS_TRANSITIONS = {"seen", "dismissed", "resolved"}


def _normalized_recipient_column():
    return func.lower(func.trim(func.coalesce(ActionEscalationNotification.recipient_email, "")))


def _now_utc():
    return datetime.datetime.now(datetime.timezone.utc)


def _date_value(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return value


def _topic_path(sujet):
    if not sujet:
        return None

    parts = []
    current = sujet
    visited = set()

    while current and current.id not in visited:
        visited.add(current.id)
        parts.append(current.titre)
        current = current.parent

    return " > ".join(reversed([part for part in parts if part]))


def serialize_escalation(notification: ActionEscalationNotification):
    action = notification.action
    sujet = getattr(action, "sujet", None) if action else None

    return {
        "id": notification.id,
        "action_id": notification.action_id,
        "action_title": getattr(action, "titre", None),
        "description": getattr(action, "description", None),
        "topic": getattr(sujet, "titre", None),
        "topic_path": _topic_path(sujet),
        "requester": getattr(action, "demandeur", None),
        "email_demandeur": normalize_email(getattr(action, "email_demandeur", None)),
        "responsible": getattr(action, "responsable", None),
        "email_responsable": normalize_email(getattr(action, "email_responsable", None)),
        "due_date": _date_value(getattr(action, "due_date", None)),
        "status": getattr(action, "status", None),
        "priority_index": getattr(action, "priority_index", None),
        "escalation_level": notification.escalation_level,
        "hierarchy_source_used": notification.hierarchy_source_used,
        "created_at": _date_value(notification.created_at),
        "updated_at": _date_value(notification.updated_at),
        "seen_at": _date_value(notification.seen_at),
        "resolved_at": _date_value(notification.resolved_at),
        "notification_status": notification.status,
    }


def _notification_rank_key(notification: ActionEscalationNotification):
    return (
        int(notification.escalation_level or 0),
        notification.updated_at or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc),
        notification.created_at or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc),
        notification.id or 0,
    )


def _dedupe_current_work_queue_notifications(notifications):
    current_by_key = {}
    for notification in notifications:
        key = (
            notification.action_id,
            normalize_email(notification.recipient_email),
        )
        existing = current_by_key.get(key)
        if existing is None or _notification_rank_key(notification) > _notification_rank_key(existing):
            current_by_key[key] = notification

    return sorted(
        current_by_key.values(),
        key=lambda notification: (
            int(notification.escalation_level or 0),
            getattr(notification.action, "priority_index", None) or -1,
            notification.created_at or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc),
            notification.id or 0,
        ),
        reverse=True,
    )


def list_my_escalations_service(db, current_user, include_all: bool = False):
    user_email = normalize_email(getattr(current_user, "email", None))
    query = (
        db.query(ActionEscalationNotification)
        .join(Action, Action.id == ActionEscalationNotification.action_id)
        .options(
            joinedload(ActionEscalationNotification.action)
            .joinedload(Action.sujet)
            .joinedload(Sujet.parent)
        )
        .filter(ActionEscalationNotification.status == PENDING_STATUS)
        .filter(Action.is_deleted.is_(False))
        .filter(~get_action_closed_state_predicate(Action))
    )

    if include_all:
        if not is_admin(current_user):
            raise HTTPException(status_code=403, detail="Administrator access required.")
    else:
        query = query.filter(ActionEscalationNotification.recipient_email == user_email)

    raw_notifications = (
        query
        .order_by(
            ActionEscalationNotification.escalation_level.desc(),
            Action.priority_index.desc().nullslast(),
            ActionEscalationNotification.updated_at.desc(),
            ActionEscalationNotification.created_at.desc(),
            ActionEscalationNotification.id.desc(),
        )
        .all()
    )
    notifications = _dedupe_current_work_queue_notifications(raw_notifications)

    return {
        "count": len(notifications),
        "all": bool(include_all and is_admin(current_user)),
        "escalations": [serialize_escalation(notification) for notification in notifications],
    }


def get_visible_escalation(db, notification_id: int, current_user):
    user_email = normalize_email(getattr(current_user, "email", None))
    notification = (
        db.query(ActionEscalationNotification)
        .options(joinedload(ActionEscalationNotification.action))
        .filter(ActionEscalationNotification.id == notification_id)
        .first()
    )

    if not notification:
        raise HTTPException(status_code=404, detail="Escalation notification not found.")

    if not is_admin(current_user) and normalize_email(notification.recipient_email) != user_email:
        raise HTTPException(status_code=403, detail="You do not have access to this escalation.")

    return notification


def update_escalation_status_service(db, notification_id: int, status: str, current_user):
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in VALID_STATUS_TRANSITIONS:
        raise HTTPException(status_code=400, detail="Unsupported escalation status.")

    notification = get_visible_escalation(db, notification_id, current_user)
    now = _now_utc()
    pending_group = [notification]

    if normalized_status in {"dismissed", "resolved"}:
        pending_group = (
            db.query(ActionEscalationNotification)
            .filter(ActionEscalationNotification.action_id == notification.action_id)
            .filter(_normalized_recipient_column() == normalize_email(notification.recipient_email))
            .filter(ActionEscalationNotification.status == PENDING_STATUS)
            .all()
        )

    for pending_notification in pending_group:
        old_status = pending_notification.status
        pending_notification.status = normalized_status
        pending_notification.updated_at = now

        if normalized_status == "seen":
            pending_notification.seen_at = now
        elif normalized_status == "resolved":
            pending_notification.resolved_at = now

        log_action_event(
            db=db,
            action_id=pending_notification.action_id,
            event_type=f"action_escalation_{normalized_status}",
            old_value=old_status,
            new_value=normalized_status,
            details=(
                f"Escalation notification {pending_notification.id} marked "
                f"{normalized_status} by {getattr(current_user, 'email', None)}."
                if len(pending_group) == 1
                else (
                    f"Escalation notification {pending_notification.id} marked "
                    f"{normalized_status} by {getattr(current_user, 'email', None)} "
                    f"as part of current work-queue group transition for "
                    f"action {notification.action_id} and recipient "
                    f"{normalize_email(notification.recipient_email)}."
                )
            ),
            created_by=getattr(current_user, "email", None),
        )
    db.commit()
    db.refresh(notification)

    return {
        "updated": True,
        "escalation": serialize_escalation(notification),
    }
