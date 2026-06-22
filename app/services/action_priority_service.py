import datetime

from sqlalchemy import and_, func

from app.models.action import Action
from app.models.action_event_log import ActionEventLog
from app.services.action_event_log_service import log_action_event
from app.services.action_status_logic_service import (
    CLOSED_HOME_BUCKET,
    get_action_active_predicate,
    is_action_hidden_from_home,
    normalize_action_status,
)


UNKNOWN_URGENCY = "Unknown"
URGENT_URGENCY = "Urgent"
FLEXIBLE_URGENCY = "Flexible"
SECONDARY_URGENCY = "Secondaire"
CLOSED_PRIORITY_INDEX = 0
DEFAULT_IMPORTANCE = "Moyenne"
HIGH_IMPORTANCE = "Haute"
MEDIUM_IMPORTANCE = "Moyenne"
LOW_IMPORTANCE = "Basse"
CRITICAL_IMPORTANCE = HIGH_IMPORTANCE
DEFAULT_ESTIMATED_DURATION_DAYS = 2

SKIPPED_STATUSES = {CLOSED_HOME_BUCKET, "archived", "hidden"}

IMPORTANCE_SCORE = {
    HIGH_IMPORTANCE: 3,
    MEDIUM_IMPORTANCE: 2,
    LOW_IMPORTANCE: 1,
}

URGENCY_SCORE = {
    URGENT_URGENCY: 3,
    FLEXIBLE_URGENCY: 2,
    SECONDARY_URGENCY: 1,
}

REACTION_TIME_BY_IMPORTANCE = {
    HIGH_IMPORTANCE: 2,
    MEDIUM_IMPORTANCE: 5,
    LOW_IMPORTANCE: 10,
}

IMPORTANCE_ALIASES = {
    "critical": HIGH_IMPORTANCE,
    "critique": HIGH_IMPORTANCE,
    "high": HIGH_IMPORTANCE,
    "haute": HIGH_IMPORTANCE,
    "important": HIGH_IMPORTANCE,
    "medium": MEDIUM_IMPORTANCE,
    "moyenne": MEDIUM_IMPORTANCE,
    "moyen": MEDIUM_IMPORTANCE,
    "average": MEDIUM_IMPORTANCE,
    "normal": MEDIUM_IMPORTANCE,
    "low": LOW_IMPORTANCE,
    "faible": LOW_IMPORTANCE,
    "basse": LOW_IMPORTANCE,
}

URGENCY_ALIASES = {
    "urgent": URGENT_URGENCY,
    "urgente": URGENT_URGENCY,
    "asap": URGENT_URGENCY,
    "flexible": FLEXIBLE_URGENCY,
    "normal": FLEXIBLE_URGENCY,
    "secondary": SECONDARY_URGENCY,
    "secondaire": SECONDARY_URGENCY,
    "low": SECONDARY_URGENCY,
}


def normalize_importance(value):
    normalized = str(value or "").strip().lower()
    return IMPORTANCE_ALIASES.get(normalized, DEFAULT_IMPORTANCE)


def normalize_urgency(value):
    normalized = str(value or "").strip().lower()
    return URGENCY_ALIASES.get(normalized, FLEXIBLE_URGENCY)


def normalize_estimated_duration_days(value) -> int:
    try:
        duration = int(value)
    except (TypeError, ValueError):
        return DEFAULT_ESTIMATED_DURATION_DAYS

    return duration if duration > 0 else DEFAULT_ESTIMATED_DURATION_DAYS


def get_today():
    return datetime.date.today()


def coerce_date(value):
    if isinstance(value, datetime.datetime):
        return value.date()

    if isinstance(value, datetime.date):
        return value

    if isinstance(value, str):
        try:
            return datetime.date.fromisoformat(value[:10])
        except ValueError:
            return None

    return None


def get_days_until_due(action, today=None):
    due_date = coerce_date(getattr(action, "due_date", None))

    if not due_date:
        return None

    today = today or get_today()
    return (due_date - today).days


def get_overdue_days(action, today=None):
    days_until_due = get_days_until_due(action, today)

    if days_until_due is None or days_until_due >= 0:
        return 0

    return abs(days_until_due)


def should_skip_priority_recalculation(action, today=None):
    today = today or get_today()
    status = normalize_action_status(getattr(action, "status", None))
    closed_date = getattr(action, "closed_date", None)

    if status in {"archived", "hidden"}:
        return True

    if closed_date and closed_date < (today - datetime.timedelta(days=7)):
        return True

    if is_action_hidden_from_home(action, today):
        return True

    return False


def derive_urgency_from_due_date(
    status,
    due_date,
    estimated_duration_days=None,
    today=None,
):
    today = today or get_today()
    due_date = coerce_date(due_date)
    normalized_status = normalize_action_status(status)
    duration_days = normalize_estimated_duration_days(estimated_duration_days)

    if normalized_status == CLOSED_HOME_BUCKET:
        return SECONDARY_URGENCY

    if normalized_status in {"overdue", "late"}:
        return URGENT_URGENCY

    if due_date is None:
        return FLEXIBLE_URGENCY

    remaining_days = (due_date - today).days

    if remaining_days <= duration_days:
        return URGENT_URGENCY

    return FLEXIBLE_URGENCY


def calculate_action_urgency(action):
    return derive_urgency_from_due_date(
        normalize_action_status(getattr(action, "status", None)),
        getattr(action, "due_date", None),
        getattr(action, "estimated_duration_days", None),
    )


def derive_due_date_score_and_escalation(
    status,
    due_date,
    today=None,
    importance=None,
):
    normalized_status = normalize_action_status(status)

    if normalized_status == CLOSED_HOME_BUCKET:
        return 0, 0

    due_date = coerce_date(due_date)

    if due_date is None:
        return 0, 1

    today = today or get_today()
    overdue_days = (today - due_date).days

    if normalized_status not in {"overdue", "late"} and overdue_days <= 0:
        return 0, 1

    overdue_days = max(overdue_days, 1)
    reaction_days = get_reaction_time_days(importance)
    escalation_level = 1 + max((overdue_days - 1) // reaction_days, 0)

    return 0, escalation_level


def calculate_action_escalation_level(action):
    _, escalation_level = derive_due_date_score_and_escalation(
        normalize_action_status(getattr(action, "status", None)),
        getattr(action, "due_date", None),
        importance=getattr(action, "importance", None),
    )
    return escalation_level


def get_importance_score(importance):
    return IMPORTANCE_SCORE.get(
        normalize_importance(importance),
        IMPORTANCE_SCORE[DEFAULT_IMPORTANCE],
    )


def get_urgency_score(urgency):
    return URGENCY_SCORE.get(
        normalize_urgency(urgency),
        URGENCY_SCORE[FLEXIBLE_URGENCY],
    )


def get_overdue_bonus(action):
    return 0


def calculate_action_priority_index(action):
    return calculate_priority_fields(
        getattr(action, "status", None),
        getattr(action, "due_date", None),
        getattr(action, "importance", None),
        getattr(action, "urgency", None),
        getattr(action, "estimated_duration_days", None),
    )["priority_index"]


def derive_priorite_from_priority_index(priority_index: int | None) -> int:
    return max(int(priority_index or 0), 0)


def calculate_priority_fields(
    status,
    due_date,
    importance,
    urgency,
    estimated_duration_days=None,
    today=None,
):
    today = today or get_today()
    normalized_status = normalize_action_status(status) or "open"
    normalized_importance = normalize_importance(importance)
    duration_days = normalize_estimated_duration_days(estimated_duration_days)

    if normalized_status == CLOSED_HOME_BUCKET:
        return {
            "importance": normalized_importance,
            "urgency": SECONDARY_URGENCY,
            "estimated_duration_days": duration_days,
            "escalation_level": 0,
            "priority_index": CLOSED_PRIORITY_INDEX,
            "priorite": 0,
        }

    due_date_value = coerce_date(due_date)
    remaining_days = (due_date_value - today).days if due_date_value else None
    is_overdue = normalized_status in {"overdue", "late"} or (
        remaining_days is not None and remaining_days < 0
    )

    if is_overdue:
        normalized_urgency = URGENT_URGENCY
    elif str(urgency or "").strip():
        normalized_urgency = normalize_urgency(urgency)
    else:
        normalized_urgency = derive_urgency_from_due_date(
            normalized_status,
            due_date_value,
            duration_days,
            today,
        )

    _, escalation_level = derive_due_date_score_and_escalation(
        normalized_status,
        due_date_value,
        today,
        normalized_importance,
    )
    escalation_level = max(int(escalation_level or 1), 1)
    priority_index = (
        get_importance_score(normalized_importance)
        * get_urgency_score(normalized_urgency)
        * escalation_level
    )

    return {
        "importance": normalized_importance,
        "urgency": normalized_urgency,
        "estimated_duration_days": duration_days,
        "escalation_level": escalation_level,
        "priority_index": priority_index,
        "priorite": derive_priorite_from_priority_index(priority_index),
    }


def get_priority_field_snapshot(action):
    return {
        "status": getattr(action, "status", None),
        "due_date": str(getattr(action, "due_date", None)) if getattr(action, "due_date", None) else None,
        "estimated_duration_days": getattr(action, "estimated_duration_days", None),
        "importance": getattr(action, "importance", None),
        "urgency": getattr(action, "urgency", None),
        "escalation_level": getattr(action, "escalation_level", None),
        "priority_index": getattr(action, "priority_index", None),
        "priorite": getattr(action, "priorite", None),
    }


def normalize_status_for_priority(status):
    return normalize_action_status(status) or "open"


def calculate_action_priority_after(action, today=None):
    normalized_status = normalize_status_for_priority(getattr(action, "status", None))
    priority_fields = calculate_priority_fields(
        normalized_status,
        getattr(action, "due_date", None),
        getattr(action, "importance", None),
        getattr(action, "urgency", None),
        getattr(action, "estimated_duration_days", None),
        today=today,
    )
    return {
        "status": normalized_status,
        **priority_fields,
    }


def apply_priority_fields(action, today=None, normalize_status_value: bool = True):
    after = calculate_action_priority_after(action, today=today)
    changed = False

    if normalize_status_value and getattr(action, "status", None) != after["status"]:
        action.status = after["status"]
        changed = True

    for field_name in [
        "importance",
        "urgency",
        "estimated_duration_days",
        "escalation_level",
        "priority_index",
        "priorite",
    ]:
        new_value = after[field_name]

        if getattr(action, field_name, None) != new_value:
            setattr(action, field_name, new_value)
            changed = True

    return changed


def recalculate_action_priority(action):
    return apply_priority_fields(action)


def reset_closed_action_priority(action):
    return apply_priority_fields(action)


def recalculate_action_priority_for_status_change(action):
    status = normalize_action_status(getattr(action, "status", None))

    if status == CLOSED_HOME_BUCKET:
        return reset_closed_action_priority(action)

    return recalculate_action_priority(action)


def enrich_action_priority(action):
    apply_priority_fields(action)
    return action


def _count_null_priority_fields(before: dict) -> int:
    return sum(
        1
        for field_name in [
            "estimated_duration_days",
            "importance",
            "urgency",
            "escalation_level",
            "priority_index",
            "priorite",
        ]
        if before.get(field_name) is None
    )


def _has_escalation_notification_today(db, action_id: int, escalation_level: int, today=None) -> bool:
    today = today or get_today()
    start_of_day = datetime.datetime.combine(
        today,
        datetime.time.min,
        tzinfo=datetime.timezone.utc,
    )

    return (
        db.query(ActionEventLog.id)
        .filter(ActionEventLog.action_id == action_id)
        .filter(ActionEventLog.event_type == "action_escalation_email_sent")
        .filter(ActionEventLog.new_value == str(escalation_level))
        .filter(ActionEventLog.created_at >= start_of_day)
        .first()
        is not None
    )


async def recalculate_all_priorities_service(
    db,
    dry_run: bool = False,
    sample_limit: int = 10,
    notify_escalations: bool = False,
    directory_db=None,
    include_deleted: bool = False,
):
    query = db.query(Action)

    if not include_deleted:
        query = query.filter(get_action_active_predicate(Action))

    actions = query.all()

    total_updates_needed = 0
    null_fields_fixed = 0
    urgent_count = 0
    escalation_changes = []
    samples = []
    today = get_today()

    for action in actions:
        before = get_priority_field_snapshot(action)
        after = calculate_action_priority_after(action, today=today)
        changed_fields = [
            field_name
            for field_name in [
                "status",
                "estimated_duration_days",
                "importance",
                "urgency",
                "escalation_level",
                "priority_index",
                "priorite",
            ]
            if before.get(field_name) != after.get(field_name)
        ]

        if after["urgency"] == URGENT_URGENCY:
            urgent_count += 1

        if changed_fields:
            total_updates_needed += 1
            null_fields_fixed += _count_null_priority_fields(before)

            if len(samples) < sample_limit:
                samples.append({
                    "id": action.id,
                    "titre": action.titre,
                    "changed_fields": changed_fields,
                    "before": before,
                    "after": after,
                })

            before_escalation = before.get("escalation_level")
            after_escalation = after.get("escalation_level")

            if before_escalation != after_escalation:
                escalation_change = {
                    "id": action.id,
                    "titre": action.titre,
                    "old_level": before_escalation,
                    "new_level": after_escalation,
                }
                escalation_changes.append(escalation_change)

            if not dry_run:
                apply_priority_fields(action, today=today)

                if before.get("escalation_level") != after.get("escalation_level"):
                    log_action_event(
                        db=db,
                        action_id=action.id,
                        event_type="action_escalation_level_changed",
                        old_value=str(before.get("escalation_level")),
                        new_value=str(after.get("escalation_level")),
                        details=(
                            "Escalation level recalculated from "
                            f"{before.get('escalation_level')} to {after.get('escalation_level')}."
                        ),
                        created_by="system",
                    )

    if dry_run:
        db.rollback()
    else:
        db.commit()

    response = {
        "message": (
            "Action priority recalculation dry run completed"
            if dry_run
            else "Action priorities recalculated successfully"
        ),
        "dry_run": dry_run,
        "include_deleted": include_deleted,
        "total_actions_checked": len(actions),
        "total_updates_needed": total_updates_needed,
        "actions_would_update": total_updates_needed if dry_run else 0,
        "updated_actions": 0 if dry_run else total_updates_needed,
        "null_fields_fixed": null_fields_fixed,
        "urgent_actions": urgent_count,
        "escalation_changes": {
            "count": len(escalation_changes),
            "items": escalation_changes[:sample_limit],
        },
        "sample_before_after": samples,
        "sample_changes": samples,
    }

    if notify_escalations:
        from app.services.action_escalation_service import send_due_escalation_notifications_service

        response["escalation_notifications"] = await send_due_escalation_notifications_service(
            db,
            directory_db=directory_db,
            today=today,
        )

    return response


async def recalculate_all_action_priorities_service(db):
    return await recalculate_all_priorities_service(db)


def calculate_urgency(due_date, estimated_duration_days=None):
    return derive_urgency_from_due_date(
        "open",
        due_date,
        estimated_duration_days,
    )


def calculate_priority_index(importance, urgency, escalation_level):
    normalized_level = max(int(escalation_level or 1), 1)
    return (
        get_importance_score(importance)
        * get_urgency_score(urgency)
        * normalized_level
    )


def get_reaction_time_days(importance):
    normalized = normalize_importance(importance)
    return REACTION_TIME_BY_IMPORTANCE.get(normalized, 5)


def calculate_reaction_deadline(due_date, importance):
    due_date = coerce_date(due_date)

    if not due_date:
        return None

    reaction_days = get_reaction_time_days(importance)
    return due_date + datetime.timedelta(days=reaction_days)


def is_escalation_ready(action):
    if should_skip_priority_recalculation(action):
        return False

    return calculate_action_escalation_level(action) > 1
