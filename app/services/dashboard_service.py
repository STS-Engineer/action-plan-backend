from collections import defaultdict
from datetime import date
from sqlalchemy.orm import Session

from app.models.action import Action
from app.models.company_member import CompanyMember
from app.services.action_priority_service import enrich_action_priority, is_escalation_ready


def empty_counter():
    return {
        "open": 0,
        "closed": 0,
        "blocked": 0,
        "overdue": 0,
        "late": 0,
        "total": 0,
        "escalation_ready": 0,
    }


def normalize_status(status):
    return (status or "open").lower()


def normalize_value(value):
    return value if value not in [None, ""] else "Unknown"


def increment_bucket(bucket, status, is_late, escalation_ready):
    bucket["total"] += 1

    if status in bucket:
        bucket[status] += 1

    if is_late:
        bucket["late"] += 1

    if escalation_ready:
        bucket["escalation_ready"] += 1


async def get_dashboard_overview_service(db: Session, directory_db: Session):
    actions = db.query(Action).all()
    members = directory_db.query(CompanyMember).all()

    members_by_email = {
        member.email.lower(): member
        for member in members
        if member.email
    }

    global_stats = empty_counter()

    by_responsable = defaultdict(empty_counter)
    by_site = defaultdict(empty_counter)
    by_department = defaultdict(empty_counter)
    by_country = defaultdict(empty_counter)
    by_urgency = defaultdict(int)
    by_importance = defaultdict(int)

    top_late_actions = []
    today = date.today()

    for action in actions:
        enrich_action_priority(action)

        status = normalize_status(action.status)
        is_late = bool(status != "closed" and action.due_date and action.due_date < today)
        escalation_ready = is_escalation_ready(action)

        responsible_email = action.email_responsable.lower() if action.email_responsable else None
        member = members_by_email.get(responsible_email) if responsible_email else None

        responsable_name = normalize_value(action.responsable)
        site = normalize_value(member.site if member else None)
        department = normalize_value(member.department if member else None)
        country = normalize_value(member.country if member else None)

        increment_bucket(global_stats, status, is_late, escalation_ready)
        increment_bucket(by_responsable[responsable_name], status, is_late, escalation_ready)
        increment_bucket(by_site[site], status, is_late, escalation_ready)
        increment_bucket(by_department[department], status, is_late, escalation_ready)
        increment_bucket(by_country[country], status, is_late, escalation_ready)

        by_urgency[normalize_value(action.urgency)] += 1
        by_importance[normalize_value(action.importance)] += 1

        if escalation_ready:
            top_late_actions.append({
                "id": action.id,
                "titre": action.titre,
                "responsable": action.responsable,
                "email_responsable": action.email_responsable,
                "due_date": str(action.due_date) if action.due_date else None,
                "status": action.status,
                "priority_index": action.priority_index,
                "urgency": action.urgency,
                "importance": action.importance,
                "site": site,
                "department": department,
                "country": country,
            })

    top_late_actions = sorted(
        top_late_actions,
        key=lambda x: x["priority_index"] or 0,
        reverse=True,
    )[:20]

    top_responsables = sorted(
        [
            {
                "name": key,
                **value
            }
            for key, value in by_responsable.items()
        ],
        key=lambda x: x["late"],
        reverse=True
    )[:15]

    top_sites = sorted(
        [
            {
                "site": key,
                **value
            }
            for key, value in by_site.items()
        ],
        key=lambda x: x["late"],
        reverse=True
    )[:10]

    top_departments = sorted(
        [
            {
                "department": key,
                **value
            }
            for key, value in by_department.items()
        ],
        key=lambda x: x["late"],
        reverse=True
    )[:10]

    return {
        "global": global_stats,
        "top_responsables": top_responsables,
        "top_sites": top_sites,
        "top_departments": top_departments,
        "by_urgency": dict(by_urgency),
        "by_importance": dict(by_importance),
        "top_late_actions": top_late_actions,
    }