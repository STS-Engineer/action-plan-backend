import datetime

from app.models.action import Action
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
DEFAULT_IMPORTANCE = "moyenne"
CRITICAL_IMPORTANCE = "critique"

SKIPPED_STATUSES = {CLOSED_HOME_BUCKET, "archived", "hidden"}

IMPORTANCE_SCORE = {
    "haute": 10,
    "critique": 15,
    "moyenne": 5,
    "faible": 2,
}

URGENCY_SCORE = {
    URGENT_URGENCY: 8,
    FLEXIBLE_URGENCY: 3,
    SECONDARY_URGENCY: 1,
}

REACTION_TIME_BY_IMPORTANCE = {
    "critique": 1,
    "haute": 2,
    "moyenne": 5,
    "faible": 10,
}

IMPORTANCE_ALIASES = {
    "critical": CRITICAL_IMPORTANCE,
    "critique": CRITICAL_IMPORTANCE,
    "high": "haute",
    "haute": "haute",
    "important": "haute",
    "medium": "moyenne",
    "moyenne": "moyenne",
    "moyen": "moyenne",
    "average": "moyenne",
    "normal": "moyenne",
    "low": "faible",
    "faible": "faible",
    "basse": "faible",
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


def get_today():
    return datetime.date.today()


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

    if status in SKIPPED_STATUSES:
        return True

    if closed_date and closed_date < (today - datetime.timedelta(days=7)):
        return True

    if is_action_hidden_from_home(action, today):
        return True

    return False


def calculate_action_urgency(action):
    return derive_urgency_from_due_date(
        normalize_action_status(getattr(action, "status", None)),
        getattr(action, "due_date", None),
    )


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


def derive_due_date_score_and_escalation(status, due_date, today=None):
    today = today or get_today()
    due_date = coerce_date(due_date)
    normalized_status = normalize_action_status(status)
    legacy_overdue_status = normalized_status in {"overdue", "late"}

    if due_date is None:
        return (18, 1) if legacy_overdue_status else (0, 0)

    days_until_due = (due_date - today).days

    if legacy_overdue_status and days_until_due >= 0:
        return 18, 1

    if days_until_due > 30:
        return 1, 0

    if 15 <= days_until_due <= 30:
        return 3, 0

    if 8 <= days_until_due <= 14:
        return 5, 0

    if 3 <= days_until_due <= 7:
        return 8, 0

    if 1 <= days_until_due <= 2:
        return 12, 0

    if days_until_due == 0:
        return 15, 0

    overdue_days = abs(days_until_due)

    if overdue_days <= 3:
        return 18, 1

    if overdue_days <= 7:
        return 22, 2

    return 28, 3


def derive_urgency_from_due_date(status, due_date, today=None):
    today = today or get_today()
    due_date = coerce_date(due_date)
    normalized_status = normalize_action_status(status)

    if normalized_status == CLOSED_HOME_BUCKET:
        return SECONDARY_URGENCY

    if normalized_status in {"overdue", "late"}:
        return URGENT_URGENCY

    if due_date is None:
        return FLEXIBLE_URGENCY

    days_until_due = (due_date - today).days

    if days_until_due <= 2:
        return URGENT_URGENCY

    if days_until_due <= 14:
        return FLEXIBLE_URGENCY

    return SECONDARY_URGENCY


def calculate_action_escalation_level(action):
    _, escalation_level = derive_due_date_score_and_escalation(
        normalize_action_status(getattr(action, "status", None)),
        getattr(action, "due_date", None),
    )
    return escalation_level


def get_importance_score(importance):
    return IMPORTANCE_SCORE.get(normalize_importance(importance), IMPORTANCE_SCORE[DEFAULT_IMPORTANCE])


def get_urgency_score(urgency):
    return URGENCY_SCORE.get(normalize_urgency(urgency), URGENCY_SCORE[FLEXIBLE_URGENCY])


def get_overdue_bonus(action):
    overdue_days = get_overdue_days(action)

    if overdue_days > 14:
        return 10

    if overdue_days > 7:
        return 5

    if overdue_days > 0:
        return 2

    return 0


def calculate_action_priority_index(action):
    return calculate_priority_fields(
        getattr(action, "status", None),
        getattr(action, "due_date", None),
        getattr(action, "importance", None),
        getattr(action, "urgency", None),
    )["priority_index"]


def derive_priorite_from_priority_index(priority_index: int | None) -> int:
    return max(int(priority_index or 0), 0)


def calculate_priority_fields(status, due_date, importance, urgency, today=None):
    today = today or get_today()
    normalized_status = normalize_action_status(status) or "open"
    normalized_importance = normalize_importance(importance)

    if normalized_status == CLOSED_HOME_BUCKET:
        return {
            "importance": normalized_importance,
            "urgency": SECONDARY_URGENCY,
            "escalation_level": 0,
            "priority_index": CLOSED_PRIORITY_INDEX,
            "priorite": 0,
        }

    normalized_urgency = (
        normalize_urgency(urgency)
        if str(urgency or "").strip()
        else derive_urgency_from_due_date(normalized_status, due_date, today)
    )
    due_date_score, escalation_level = derive_due_date_score_and_escalation(
        normalized_status,
        due_date,
        today,
    )
    priority_index = max(
        due_date_score
        + get_importance_score(normalized_importance)
        + get_urgency_score(normalized_urgency)
        + (escalation_level * 3),
        0,
    )

    return {
        "importance": normalized_importance,
        "urgency": normalized_urgency,
        "escalation_level": escalation_level,
        "priority_index": priority_index,
        "priorite": derive_priorite_from_priority_index(priority_index),
    }


def get_priority_field_snapshot(action):
    return {
        "status": getattr(action, "status", None),
        "due_date": str(getattr(action, "due_date", None)) if getattr(action, "due_date", None) else None,
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

    for field_name in ["importance", "urgency", "escalation_level", "priority_index", "priorite"]:
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


async def recalculate_all_priorities_service(db, dry_run: bool = False, sample_limit: int = 10):
    actions = (
        db.query(Action)
        .filter(get_action_active_predicate(Action))
        .all()
    )

    updated_count = 0
    urgent_count = 0
    escalated_count = 0
    processed_count = 0
    samples = []
    today = get_today()

    for action in actions:
        processed_count += 1
        before = get_priority_field_snapshot(action)
        after = calculate_action_priority_after(action, today=today)
        changed = any(
            before.get(field_name) != after.get(field_name)
            for field_name in ["status", "importance", "urgency", "escalation_level", "priority_index", "priorite"]
        )

        if changed:
            updated_count += 1

            if len(samples) < sample_limit:
                samples.append({
                    "id": action.id,
                    "titre": action.titre,
                    "before": before,
                    "after": after,
                })

            if not dry_run:
                apply_priority_fields(action, today=today)

        effective_urgency = after["urgency"] if changed or dry_run else action.urgency
        effective_escalation = after["escalation_level"] if changed or dry_run else action.escalation_level

        if effective_urgency == URGENT_URGENCY:
            urgent_count += 1

        if (effective_escalation or 0) > 0:
            escalated_count += 1

    if dry_run:
        db.rollback()
    else:
        db.commit()

    return {
        "message": (
            "Action priority recalculation dry run completed"
            if dry_run
            else "Action priorities recalculated successfully"
        ),
        "dry_run": dry_run,
        "processed_actions": processed_count,
        "actions_would_update": updated_count if dry_run else 0,
        "updated_actions": 0 if dry_run else updated_count,
        "urgent_actions": urgent_count,
        "escalated_actions": escalated_count,
        "sample_changes": samples,
    }


async def recalculate_all_action_priorities_service(db):
    return await recalculate_all_priorities_service(db)


def calculate_urgency(due_date, estimated_duration_days=None):
    action_like = type("ActionLike", (), {"due_date": due_date})()
    return calculate_action_urgency(action_like)


def calculate_priority_index(importance, urgency, escalation_level):
    return max(
        get_importance_score(importance)
        + get_urgency_score(urgency)
        + ((escalation_level or 0) * 3),
        0,
    )


def get_reaction_time_days(importance):
    normalized = normalize_importance(importance)
    return REACTION_TIME_BY_IMPORTANCE.get(normalized, 5)


def calculate_reaction_deadline(due_date, importance):
    if not due_date:
        return None

    reaction_days = get_reaction_time_days(importance)
    return due_date + datetime.timedelta(days=reaction_days)


def is_escalation_ready(action):
    if should_skip_priority_recalculation(action):
        return False

    return calculate_action_escalation_level(action) > 0
