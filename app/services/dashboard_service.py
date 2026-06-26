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
    get_action_active_predicate,
    normalize_action_status,
)
from app.services.directory_service import get_all_underlings, normalize_email
from app.services.auth_service import is_admin_role


SUPPORTED_SCOPES = {"my", "team", "global", "all"}
SUPPORTED_CHART_KEYS = {
    "priority_distribution",
    "status_distribution",
    "people_late_pareto",
    "urgency_pareto",
    "department_overdue",
    "site_overdue",
    "late_vs_in_progress",
}
PRIORITY_HIGH_THRESHOLD = 9
TOP_LIMIT = 10
TOP_CRITICAL_LIMIT = 12


def is_dashboard_deleted_action(action) -> bool:
    return bool(getattr(action, "is_deleted", False))


def is_dashboard_completed_action(action) -> bool:
    return (
        normalize_action_status(getattr(action, "status", None)) == CLOSED_HOME_BUCKET
        or getattr(action, "closed_date", None) is not None
    )


def is_dashboard_overdue_action(action, today: date | None = None) -> bool:
    today = today or date.today()

    if is_dashboard_deleted_action(action) or is_dashboard_completed_action(action):
        return False

    status = normalize_action_status(getattr(action, "status", None))
    due_date = getattr(action, "due_date", None)

    return status in {"overdue", "late"} or bool(due_date and due_date < today)


def get_dashboard_action_bucket(action, today: date | None = None) -> str | None:
    if is_dashboard_deleted_action(action):
        return None

    if is_dashboard_completed_action(action):
        return CLOSED_HOME_BUCKET

    if is_dashboard_overdue_action(action, today):
        return OVERDUE_HOME_BUCKET

    return IN_PROGRESS_HOME_BUCKET


def normalize_dimension(value):
    return str(value).strip() if value not in [None, ""] else "Unknown"


def normalize_bucket_key(value):
    normalized = normalize_dimension(value).lower()
    return "".join(character if character.isalnum() else "_" for character in normalized).strip("_")


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


def get_status_bucket_from_chart_bucket(bucket):
    normalized_bucket = normalize_bucket_key(bucket)

    if normalized_bucket in {"late", "overdue"}:
        return OVERDUE_HOME_BUCKET

    if normalized_bucket in {"in_progress", "inprogress", "open"}:
        return IN_PROGRESS_HOME_BUCKET

    if normalized_bucket in {"completed", "complete", "closed", "done"}:
        return CLOSED_HOME_BUCKET

    return None


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


def normalize_dashboard_scope(scope: str | None) -> str:
    normalized_scope = (scope or "my").strip().lower()

    if normalized_scope == "global":
        return "all"

    if normalized_scope not in SUPPORTED_SCOPES:
        return "my"

    return normalized_scope


def get_dashboard_supported_scopes(user_role: str | None):
    scopes = ["my", "team"]

    if is_admin_role(user_role):
        scopes.append("all")

    return scopes


def get_actions_for_scope(
    db: Session,
    directory_db: Session,
    email: str | None,
    scope: str,
    user_role: str | None = None,
):
    normalized_email = normalize_email(email)
    active_query = db.query(Action).filter(get_action_active_predicate(Action))

    if scope == "all":
        if not is_admin_role(user_role):
            return []

        return active_query.all()

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
            active_query
            .filter(func.lower(Action.email_responsable).in_(underling_emails))
            .all()
        )

    return (
        active_query
        .filter(func.lower(Action.email_responsable) == normalized_email)
        .all()
    )


def distinct_actions_by_id(actions):
    return list({
        action.id: action
        for action in actions
        if getattr(action, "id", None) is not None
    }.values())


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


def build_topic_path(sujet):
    parts = []
    visited_ids = set()
    current = sujet

    while current and current.id not in visited_ids:
        parts.append(current.titre or current.code or f"Sujet {current.id}")
        visited_ids.add(current.id)
        current = current.parent

    return " > ".join(reversed(parts)) if parts else None


def serialize_drilldown_action(action, bucket):
    return {
        "id": action.id,
        "titre": action.titre,
        "status": action.status,
        "canonical_status": bucket,
        "importance": action.importance,
        "urgency": action.urgency,
        "priority_index": action.priority_index,
        "responsable": action.responsable,
        "email_responsable": action.email_responsable,
        "demandeur": action.demandeur,
        "email_demandeur": action.email_demandeur,
        "due_date": action.due_date.isoformat() if action.due_date else None,
        "topic_path": build_topic_path(action.sujet),
    }


def action_matches_drilldown_bucket(action, member, status_bucket, chart, bucket):
    requested_bucket = normalize_bucket_key(bucket)

    if chart == "priority_distribution":
        return normalize_bucket_key(get_priority_bucket(action.priority_index)) == requested_bucket

    if chart == "urgency_pareto":
        return normalize_bucket_key(normalize_urgency_bucket(action.urgency)) == requested_bucket

    if chart == "people_late_pareto":
        if status_bucket != OVERDUE_HOME_BUCKET:
            return False

        person_name = normalize_dimension(action.responsable or action.email_responsable)
        person_email = normalize_dimension(action.email_responsable)

        return requested_bucket in {
            normalize_bucket_key(person_name),
            normalize_bucket_key(person_email),
        }

    if chart == "department_overdue":
        if status_bucket != OVERDUE_HOME_BUCKET:
            return False

        return normalize_bucket_key(member.department if member else None) == requested_bucket

    if chart == "site_overdue":
        if status_bucket != OVERDUE_HOME_BUCKET:
            return False

        return normalize_bucket_key(member.site if member else None) == requested_bucket

    if chart in {"status_distribution", "late_vs_in_progress"}:
        expected_status_bucket = get_status_bucket_from_chart_bucket(bucket)
        return expected_status_bucket is not None and status_bucket == expected_status_bucket

    return False


async def get_dashboard_overview_service(
    db: Session,
    directory_db: Session,
    email: str | None = None,
    scope: str = "my",
    user_role: str | None = None,
):
    normalized_scope = normalize_dashboard_scope(scope)

    today = date.today()
    actions = get_actions_for_scope(
        db,
        directory_db,
        email,
        normalized_scope,
        user_role=user_role,
    )
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

    for action in distinct_actions_by_id(actions):
        enrich_action_priority(action)

        bucket = get_dashboard_action_bucket(action, today)

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
        "supported_scopes": get_dashboard_supported_scopes(user_role),
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


async def get_dashboard_drilldown_service(
    db: Session,
    directory_db: Session,
    email: str,
    scope: str,
    chart: str,
    bucket: str,
    user_role: str | None = None,
):
    normalized_scope = normalize_dashboard_scope(scope)
    normalized_chart = (chart or "").strip().lower()

    if normalized_chart not in SUPPORTED_CHART_KEYS:
        return {
            "chart": normalized_chart,
            "bucket": bucket,
            "count": 0,
            "actions": [],
        }

    today = date.today()
    actions = get_actions_for_scope(
        db,
        directory_db,
        email,
        normalized_scope,
        user_role=user_role,
    )
    members_by_email = get_members_by_email(directory_db)
    drilldown_actions = []

    for action in distinct_actions_by_id(actions):
        enrich_action_priority(action)

        status_bucket = get_dashboard_action_bucket(action, today)

        if status_bucket is None:
            continue

        responsible_email = action.email_responsable.lower() if action.email_responsable else None
        member = members_by_email.get(responsible_email) if responsible_email else None

        if not action_matches_drilldown_bucket(
            action=action,
            member=member,
            status_bucket=status_bucket,
            chart=normalized_chart,
            bucket=bucket,
        ):
            continue

        drilldown_actions.append(serialize_drilldown_action(action, status_bucket))

    drilldown_actions = sorted(
        drilldown_actions,
        key=lambda item: (
            item["due_date"] or "9999-12-31",
            item["responsable"] or "",
            item["titre"] or "",
            item["id"],
        ),
    )

    return {
        "chart": normalized_chart,
        "bucket": bucket,
        "count": len(drilldown_actions),
        "actions": drilldown_actions,
    }


def _dashboard_action_bucket_reason(action, today: date | None = None):
    today = today or date.today()
    status = normalize_action_status(getattr(action, "status", None))
    due_date = getattr(action, "due_date", None)

    if is_dashboard_deleted_action(action):
        return None, "excluded_deleted_action"

    if is_dashboard_completed_action(action):
        if getattr(action, "closed_date", None) is not None and status != CLOSED_HOME_BUCKET:
            return CLOSED_HOME_BUCKET, "completed_because_closed_date_is_set"
        return CLOSED_HOME_BUCKET, "completed_by_canonical_status"

    if status in {"overdue", "late"}:
        return OVERDUE_HOME_BUCKET, "overdue_by_canonical_status"

    if due_date and due_date < today:
        return OVERDUE_HOME_BUCKET, "overdue_because_due_date_is_before_today"

    return IN_PROGRESS_HOME_BUCKET, "active_not_closed_not_overdue"


def get_dashboard_diagnostics_service(db: Session):
    today = date.today()
    actions = db.query(Action).all()
    distinct_actions = distinct_actions_by_id(actions)
    bucket_members = {
        CLOSED_HOME_BUCKET: set(),
        OVERDUE_HOME_BUCKET: set(),
        IN_PROGRESS_HOME_BUCKET: set(),
    }
    excluded_deleted_ids = []

    for action in distinct_actions:
        bucket = get_dashboard_action_bucket(action, today)

        if bucket is None:
            if is_dashboard_deleted_action(action):
                excluded_deleted_ids.append(action.id)
            continue

        bucket_members[bucket].add(action.id)

    completed_ids = bucket_members[CLOSED_HOME_BUCKET]
    overdue_ids = bucket_members[OVERDUE_HOME_BUCKET]
    in_progress_ids = bucket_members[IN_PROGRESS_HOME_BUCKET]
    overlap_ids = sorted(
        (completed_ids & overdue_ids)
        | (completed_ids & in_progress_ids)
        | (overdue_ids & in_progress_ids)
    )
    incorrectly_overdue_closed_ids = sorted(
        action.id
        for action in distinct_actions
        if is_dashboard_completed_action(action) and action.id in overdue_ids
    )
    deleted_counted_ids = sorted(
        action.id
        for action in distinct_actions
        if is_dashboard_deleted_action(action)
        and action.id in (completed_ids | overdue_ids | in_progress_ids)
    )

    return {
        "generated_at": today.isoformat(),
        "source_tables_used": ["action"],
        "counting_method": "distinct action.id from current action rows only",
        "total_rows_scanned": len(actions),
        "total_distinct_actions_scanned": len(distinct_actions),
        "duplicate_count_risk": len(actions) != len(distinct_actions),
        "total_active_actions": len(completed_ids | overdue_ids | in_progress_ids),
        "completed_count": len(completed_ids),
        "in_progress_count": len(in_progress_ids),
        "overdue_count": len(overdue_ids),
        "completed_action_ids": sorted(completed_ids),
        "in_progress_action_ids": sorted(in_progress_ids),
        "overdue_action_ids": sorted(overdue_ids),
        "deleted_action_ids_excluded": sorted(excluded_deleted_ids),
        "actions_counted_in_more_than_one_bucket": overlap_ids,
        "closed_completed_actions_incorrectly_counted_as_overdue": incorrectly_overdue_closed_ids,
        "deleted_actions_counted": deleted_counted_ids,
        "history_tables_used_for_counts": False,
        "event_tables_used_for_counts": False,
    }


def get_dashboard_action_status_debug_service(db: Session, action_id: int):
    action = db.query(Action).filter(Action.id == action_id).first()

    if not action:
        return {
            "found": False,
            "action_id": action_id,
            "computed_bucket": None,
            "why": "action_not_found",
            "dashboard_bucket_membership": {
                CLOSED_HOME_BUCKET: False,
                OVERDUE_HOME_BUCKET: False,
                IN_PROGRESS_HOME_BUCKET: False,
            },
        }

    today = date.today()
    bucket, reason = _dashboard_action_bucket_reason(action, today)

    return {
        "found": True,
        "action_id": action.id,
        "raw_status": action.status,
        "canonical_status": normalize_action_status(action.status),
        "due_date": action.due_date.isoformat() if action.due_date else None,
        "closed_date": action.closed_date.isoformat() if action.closed_date else None,
        "is_deleted": bool(action.is_deleted),
        "computed_bucket": bucket,
        "why": reason,
        "dashboard_bucket_membership": {
            CLOSED_HOME_BUCKET: bucket == CLOSED_HOME_BUCKET,
            OVERDUE_HOME_BUCKET: bucket == OVERDUE_HOME_BUCKET,
            IN_PROGRESS_HOME_BUCKET: bucket == IN_PROGRESS_HOME_BUCKET,
        },
    }
