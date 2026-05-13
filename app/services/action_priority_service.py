import datetime

from sqlalchemy import func, or_

from app.models.action import Action
from app.services.action_status_logic_service import (
    CLOSED_HOME_BUCKET,
    is_action_hidden_from_home,
    normalize_action_status,
)


UNKNOWN_URGENCY = "Unknown"
URGENT_URGENCY = "Urgent"
FLEXIBLE_URGENCY = "Flexible"
SECONDARY_URGENCY = "Secondaire"

SKIPPED_STATUSES = {CLOSED_HOME_BUCKET, "archived", "hidden"}

IMPORTANCE_SCORE = {
    "high": 10,
    "haute": 10,
    "important": 10,
    "critique": 10,
    "medium": 5,
    "moyenne": 5,
    "moyen": 5,
    "low": 2,
    "faible": 2,
    "basse": 2,
}

URGENCY_SCORE = {
    "urgent": 6,
    "urgente": 6,
    "flexible": 3,
    "secondaire": 1,
    "secondary": 1,
    "unknown": 0,
}

REACTION_TIME_BY_IMPORTANCE = {
    "high": 2,
    "haute": 2,
    "important": 2,
    "critique": 2,
    "medium": 5,
    "moyenne": 5,
    "moyen": 5,
    "low": 10,
    "faible": 10,
    "basse": 10,
    "unknown": 5,
}


def normalize_importance(value):
    normalized = str(value or "").strip().lower()

    if normalized in {"high", "haute", "important", "critique"}:
        return "high"

    if normalized in {"medium", "moyenne", "moyen"}:
        return "medium"

    if normalized in {"low", "faible", "basse"}:
        return "low"

    return "unknown"


def normalize_urgency(value):
    normalized = str(value or "").strip().lower()

    if normalized in {"urgent", "urgente"}:
        return "urgent"

    if normalized == "flexible":
        return "flexible"

    if normalized in {"secondaire", "secondary"}:
        return "secondaire"

    return "unknown"


def get_today():
    return datetime.date.today()


def get_days_until_due(action, today=None):
    due_date = getattr(action, "due_date", None)

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
    days_until_due = get_days_until_due(action)

    if days_until_due is None:
        return UNKNOWN_URGENCY

    if days_until_due < 0:
        return URGENT_URGENCY

    if days_until_due <= 2:
        return URGENT_URGENCY

    if days_until_due <= 7:
        return FLEXIBLE_URGENCY

    return SECONDARY_URGENCY


def calculate_action_escalation_level(action):
    overdue_days = get_overdue_days(action)

    if overdue_days >= 14:
        return 3

    if overdue_days >= 7:
        return 2

    if overdue_days >= 1:
        return 1

    return 0


def get_importance_score(importance):
    return IMPORTANCE_SCORE.get(normalize_importance(importance), 0)


def get_urgency_score(urgency):
    return URGENCY_SCORE.get(normalize_urgency(urgency), 0)


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
    urgency = calculate_action_urgency(action)
    escalation_level = calculate_action_escalation_level(action)

    # Verification example:
    # importance=moyenne, urgency=urgent, escalation=1, overdue>0
    # => 5 + 6 + 3 + 2 = 16.
    priority_index = (
        get_importance_score(getattr(action, "importance", None))
        + get_urgency_score(urgency)
        + (escalation_level * 3)
        + get_overdue_bonus(action)
    )

    return max(priority_index, 0)


def recalculate_action_priority(action):
    urgency = calculate_action_urgency(action)
    escalation_level = calculate_action_escalation_level(action)
    priority_index = calculate_action_priority_index(action)

    changed = (
        action.urgency != urgency
        or action.escalation_level != escalation_level
        or action.priority_index != priority_index
    )

    action.urgency = urgency
    action.escalation_level = escalation_level
    action.priority_index = priority_index

    return changed


def enrich_action_priority(action):
    if should_skip_priority_recalculation(action):
        return action

    recalculate_action_priority(action)

    return action


async def recalculate_all_priorities_service(db):
    actions = (
        db.query(Action)
        .filter(
            or_(
                Action.status.is_(None),
                func.lower(Action.status).notin_(SKIPPED_STATUSES),
            )
        )
        .all()
    )

    updated_count = 0
    urgent_count = 0
    escalated_count = 0
    processed_count = 0
    today = get_today()

    for action in actions:
        if should_skip_priority_recalculation(action, today):
            continue

        processed_count += 1
        changed = recalculate_action_priority(action)

        if changed:
            updated_count += 1

        if action.urgency == URGENT_URGENCY:
            urgent_count += 1

        if (action.escalation_level or 0) > 0:
            escalated_count += 1

    db.commit()

    return {
        "message": "Action priorities recalculated successfully",
        "processed_actions": processed_count,
        "updated_actions": updated_count,
        "urgent_actions": urgent_count,
        "escalated_actions": escalated_count,
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
