from collections import Counter
from datetime import date

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.action import Action
from app.models.company_member import CompanyMember
from app.services.action_priority_service import enrich_action_priority, is_escalation_ready
from app.services.action_status_logic_service import (
    CLOSED_HOME_BUCKET,
    IN_PROGRESS_HOME_BUCKET,
    OVERDUE_HOME_BUCKET,
    get_action_home_bucket,
)
from app.services.directory_service import get_all_underlings, normalize_email


SUPPORTED_SCOPES = {"my", "team", "global"}
PRIORITY_HIGH_THRESHOLD = 9
TOP_LIMIT = 10
TOP_CRITICAL_LIMIT = 12


def normalize_dimension(value):
    return str(value).strip() if value not in [None, ""] else "Unknown"


def normalize_urgency_bucket(value):
    normalized = normalize_dimension(value).lower()

    if normalized == "unknown":
        return "Unknown"

    if "urgent" in normalized:
        return "Urgent"

    if "flex" in normalized:
        return "Flexible"

    if "second" in normalized or "secondaire" in normalized:
        return "Secondary / Secondaire"

    return "Unknown"


def get_priority_bucket(priority_index):
    if priority_index is None:
        return "Unknown"

    if priority_index >= 18:
        return "Critical"

    if priority_index >= 9:
        return "High"

    if priority_index >= 4:
        return "Medium"

    if priority_index >= 1:
        return "Low"

    return "Unknown"


def build_pareto(counter, value_key, limit=TOP_LIMIT):
    items = sorted(
        counter.items(),
        key=lambda item: (-item[1], item[0]),
    )[:limit]

    total = sum(value for _, value in items)
    cumulative = 0
    result = []

    for name, value in items:
        cumulative += value
        cumulative_percent = round((cumulative / total) * 100, 1) if total else 0

        result.append({
            "name": name,
            value_key: value,
            "count": value,
            "cumulative_percent": cumulative_percent,
        })

    return result


def get_members_by_email(directory_db: Session):
    members = directory_db.query(CompanyMember).all()

    return {
        member.email.lower(): member
        for member in members
        if member.email
    }


def get_actions_for_scope(db: Session, directory_db: Session, email: str | None, scope: str):
    normalized_email = normalize_email(email)

    if scope == "global":
        return db.query(Action).all()

    if not normalized_email:
        return []

    if scope == "team":
        underlings = get_all_underlings(directory_db, normalized_email)
        underling_emails = [
            member.email.lower()
            for member in underlings
            if member.email
        ]

        if not underling_emails:
            return []

        return (
            db.query(Action)
            .filter(func.lower(Action.email_responsable).in_(underling_emails))
            .all()
        )

    return (
        db.query(Action)
        .filter(func.lower(Action.email_responsable) == normalized_email)
        .all()
    )


def serialize_action(action, member, bucket, escalation_ready):
    return {
        "id": action.id,
        "titre": action.titre,
        "responsable": action.responsable,
        "email_responsable": action.email_responsable,
        "due_date": str(action.due_date) if action.due_date else None,
        "status": action.status,
        "status_bucket": bucket,
        "priority_index": action.priority_index,
        "urgency": action.urgency,
        "importance": action.importance,
        "escalation_ready": escalation_ready,
        "site": normalize_dimension(member.site if member else None),
        "department": normalize_dimension(member.department if member else None),
        "country": normalize_dimension(member.country if member else None),
    }


async def get_dashboard_overview_service(
    db: Session,
    directory_db: Session,
    email: str | None = None,
    scope: str = "global",
):
    normalized_scope = (scope or "global").strip().lower()

    if normalized_scope not in SUPPORTED_SCOPES:
        normalized_scope = "global"

    today = date.today()
    actions = get_actions_for_scope(db, directory_db, email, normalized_scope)
    members_by_email = get_members_by_email(directory_db)

    visible_actions = []
    top_critical_actions = []
    people_late_counter = Counter()
    urgency_counter = Counter()
    priority_counter = Counter()
    department_overdue_counter = Counter()
    site_overdue_counter = Counter()

    status_counts = {
        IN_PROGRESS_HOME_BUCKET: 0,
        OVERDUE_HOME_BUCKET: 0,
        CLOSED_HOME_BUCKET: 0,
    }

    high_priority = 0
    escalation_ready_count = 0

    for action in actions:
        enrich_action_priority(action)

        bucket = get_action_home_bucket(action, today)

        if bucket is None:
            continue

        responsible_email = action.email_responsable.lower() if action.email_responsable else None
        member = members_by_email.get(responsible_email) if responsible_email else None
        escalation_ready = is_escalation_ready(action)

        visible_actions.append(action)
        status_counts[bucket] += 1

        if (action.priority_index or 0) >= PRIORITY_HIGH_THRESHOLD:
            high_priority += 1

        if escalation_ready:
            escalation_ready_count += 1

        urgency_counter[normalize_urgency_bucket(action.urgency)] += 1
        priority_counter[get_priority_bucket(action.priority_index)] += 1

        if bucket == OVERDUE_HOME_BUCKET:
            person_name = normalize_dimension(action.responsable or action.email_responsable)
            people_late_counter[person_name] += 1
            department_overdue_counter[normalize_dimension(member.department if member else None)] += 1
            site_overdue_counter[normalize_dimension(member.site if member else None)] += 1

        if bucket != CLOSED_HOME_BUCKET and (
            escalation_ready or (action.priority_index or 0) >= PRIORITY_HIGH_THRESHOLD
        ):
            top_critical_actions.append(
                serialize_action(action, member, bucket, escalation_ready)
            )

    top_critical_actions = sorted(
        top_critical_actions,
        key=lambda item: (
            item["status_bucket"] != OVERDUE_HOME_BUCKET,
            -(item["priority_index"] or 0),
            item["due_date"] or "9999-12-31",
        ),
    )[:TOP_CRITICAL_LIMIT]

    priority_distribution = [
        {"name": name, "count": priority_counter.get(name, 0)}
        for name in ["Critical", "High", "Medium", "Low", "Unknown"]
    ]

    urgency_pareto = build_pareto(urgency_counter, "count")

    return {
        "scope": normalized_scope,
        "supported_scopes": ["my", "team", "global"],
        "generated_at": date.today().isoformat(),
        "global": {
            "total_actions": len(visible_actions),
            "in_progress": status_counts[IN_PROGRESS_HOME_BUCKET],
            "overdue": status_counts[OVERDUE_HOME_BUCKET],
            "completed": status_counts[CLOSED_HOME_BUCKET],
            "high_priority": high_priority,
            "escalation_ready": escalation_ready_count,
        },
        "status_distribution": [
            {"name": "In progress", "value": status_counts[IN_PROGRESS_HOME_BUCKET]},
            {"name": "Overdue", "value": status_counts[OVERDUE_HOME_BUCKET]},
            {"name": "Completed", "value": status_counts[CLOSED_HOME_BUCKET]},
        ],
        "people_late_pareto": build_pareto(people_late_counter, "overdue"),
        "urgency_pareto": urgency_pareto,
        "priority_distribution": priority_distribution,
        "department_overdue": [
            {"name": name, "overdue": value}
            for name, value in sorted(
                department_overdue_counter.items(),
                key=lambda item: (-item[1], item[0]),
            )[:TOP_LIMIT]
        ],
        "site_overdue": [
            {"name": name, "overdue": value}
            for name, value in sorted(
                site_overdue_counter.items(),
                key=lambda item: (-item[1], item[0]),
            )[:TOP_LIMIT]
        ],
        "top_critical_actions": top_critical_actions,
    }
