import datetime
from app.models.action import Action


IMPORTANCE_SCORE = {
    "haute": 3,
    "moyenne": 2,
    "basse": 1,
}

URGENCY_SCORE = {
    "urgent": 3,
    "flexible": 2,
    "secondaire": 1,
}
REACTION_TIME_BY_IMPORTANCE = {
    "haute": 2,
    "moyenne": 5,
    "basse": 10,
}

def normalize_importance(value):
    if not value:
        return "moyenne"

    value = value.lower().strip()

    if value in IMPORTANCE_SCORE:
        return value

    return "moyenne"


def calculate_urgency(due_date, estimated_duration_days):
    if not due_date:
        return "secondaire"

    today = datetime.date.today()
    remaining_days = (due_date - today).days
    duration = estimated_duration_days or 2

    if remaining_days <= duration:
        return "urgent"

    if remaining_days <= duration * 2:
        return "flexible"

    return "secondaire"


def calculate_priority_index(importance, urgency, escalation_level):
    importance = normalize_importance(importance)
    urgency = urgency or "secondaire"
    escalation_level = escalation_level or 1

    return (
        IMPORTANCE_SCORE.get(importance, 2)
        * URGENCY_SCORE.get(urgency, 1)
        * escalation_level
    )


def enrich_action_priority(action):
    estimated_duration_days = action.estimated_duration_days or 2
    importance = normalize_importance(action.importance)
    escalation_level = action.escalation_level or 1
    urgency = calculate_urgency(action.due_date, estimated_duration_days)
    priority_index = calculate_priority_index(
        importance,
        urgency,
        escalation_level,
    )

    action.estimated_duration_days = estimated_duration_days
    action.importance = importance
    action.escalation_level = escalation_level
    action.urgency = urgency
    action.priority_index = priority_index

    return action


async def recalculate_all_action_priorities_service(db):
    actions = (
        db.query(Action)
        .filter(Action.status != "closed")
        .all()
    )

    updated_count = 0

    for action in actions:
        enrich_action_priority(action)
        updated_count += 1

    db.commit()

    return {
        "message": "Action priorities recalculated successfully",
        "updated_actions": updated_count,
    }
def get_reaction_time_days(importance):
    importance = normalize_importance(importance)
    return REACTION_TIME_BY_IMPORTANCE.get(importance, 5)


def calculate_reaction_deadline(due_date, importance):
    if not due_date:
        return None

    reaction_days = get_reaction_time_days(importance)
    return due_date + datetime.timedelta(days=reaction_days)


def is_escalation_ready(action):
    if not action.due_date:
        return False

    if action.status == "closed":
        return False

    importance = normalize_importance(action.importance)
    reaction_deadline = calculate_reaction_deadline(action.due_date, importance)

    if not reaction_deadline:
        return False

    return datetime.date.today() > reaction_deadline