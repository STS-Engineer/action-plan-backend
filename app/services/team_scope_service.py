from collections import deque

from sqlalchemy import func, text

from app.models.action import Action
from app.services.action_status_logic_service import get_action_active_predicate
from app.services.organisation_hierarchy_service import (
    ORGANISATION_SOURCE,
    PERSON_SELECT,
    find_person_by_email,
    is_valid_email,
    normalize_email,
    normalize_name,
)


TEAM_SCOPE_SOURCE = ORGANISATION_SOURCE


def _row_to_person(row) -> dict:
    data = dict(row)
    data["email"] = normalize_email(data.get("email"))
    data["normalized_name"] = normalize_name(data.get("personne"))
    return data


def _direct_report_sort_key(person: dict):
    assignment_id = person.get("assignment_id")
    if assignment_id is None:
        assignment_id = 999999999999

    return (
        0 if person.get("is_primary") else 1,
        0 if is_valid_email(person.get("email")) else 1,
        str(person.get("personne") or ""),
        assignment_id,
    )


def _dedupe_people_by_email_or_assignment(people: list[dict]) -> list[dict]:
    seen = set()
    result = []

    for person in sorted(people, key=_direct_report_sort_key):
        email = normalize_email(person.get("email"))
        if not is_valid_email(email):
            continue

        key = f"email:{email}" if email else f"assignment:{person.get('assignment_id')}"

        if key in seen:
            continue

        seen.add(key)
        result.append(person)

    return result


def _query_people_with_manager(organisation_db) -> list[dict]:
    if organisation_db is None:
        return []

    result = organisation_db.execute(
        text(
            PERSON_SELECT
            + """
WHERE nullif(trim(coalesce(boss_email, '')), '') IS NOT NULL
ORDER BY is_primary DESC NULLS LAST, personne ASC NULLS LAST, assignment_id ASC NULLS LAST
"""
        )
    )
    return [_row_to_person(row) for row in result.mappings().all()]


def get_direct_reports_for_manager_email(organisation_db, manager_email: str | None) -> dict:
    normalized_email = normalize_email(manager_email)

    if organisation_db is None:
        return {
            "source": TEAM_SCOPE_SOURCE,
            "company_members_used": False,
            "manager_email": normalized_email,
            "selected_person": None,
            "direct_reports": [],
            "direct_report_emails": [],
            "warnings": [{"type": "organisation_db_unavailable"}],
        }

    lookup = find_person_by_email(organisation_db, normalized_email)
    selected_person = lookup.get("selected")
    warnings = [*lookup.get("warnings", [])]

    if not selected_person or not normalized_email:
        return {
            "source": TEAM_SCOPE_SOURCE,
            "company_members_used": False,
            "manager_email": normalized_email,
            "selected_person": selected_person,
            "direct_reports": [],
            "direct_report_emails": [],
            "warnings": [
                *warnings,
                {"type": "manager_not_found_in_v_people_with_boss"},
            ],
        }

    people = _query_people_with_manager(organisation_db)
    direct_reports = _dedupe_people_by_email_or_assignment([
        person
        for person in people
        if (
            normalize_email(person.get("boss_email")) == normalized_email
            and normalize_email(person.get("email")) != normalized_email
        )
    ])
    direct_report_emails = [
        email
        for email in (
            normalize_email(person.get("email"))
            for person in direct_reports
        )
        if email
    ]

    return {
        "source": TEAM_SCOPE_SOURCE,
        "company_members_used": False,
        "manager_email": normalized_email,
        "selected_person": selected_person,
        "direct_reports": direct_reports,
        "direct_report_emails": list(dict.fromkeys(direct_report_emails)),
        "warnings": warnings,
    }


def get_direct_report_emails_for_team_scope(organisation_db, manager_email: str | None) -> list[str]:
    return get_direct_reports_for_manager_email(
        organisation_db,
        manager_email,
    ).get("direct_report_emails", [])


def _recursive_report_emails_from_organisation(organisation_db, manager_email: str | None) -> set[str]:
    seen_managers = set()
    result = set()
    queue = deque([normalize_email(manager_email)])

    while queue:
        current_email = queue.popleft()

        if not current_email or current_email in seen_managers:
            continue

        seen_managers.add(current_email)
        direct_emails = get_direct_report_emails_for_team_scope(
            organisation_db,
            current_email,
        )

        for report_email in direct_emails:
            if report_email not in result:
                result.add(report_email)
                queue.append(report_email)

    result.discard(normalize_email(manager_email))
    return result


def _actions_for_responsible_emails(db, emails: list[str] | set[str]) -> list[int]:
    normalized_emails = [
        email
        for email in (normalize_email(email) for email in emails)
        if email
    ]

    if not normalized_emails:
        return []

    rows = (
        db.query(Action.id)
        .filter(get_action_active_predicate(Action))
        .filter(func.lower(func.coalesce(Action.email_responsable, "")).in_(normalized_emails))
        .order_by(Action.id.asc())
        .all()
    )
    return [row.id for row in rows]


def get_team_scope_debug_service(db, organisation_db, email: str) -> dict:
    direct_scope = get_direct_reports_for_manager_email(organisation_db, email)
    direct_emails = set(direct_scope.get("direct_report_emails") or [])
    recursive_emails = _recursive_report_emails_from_organisation(organisation_db, email)
    n2_plus_emails = sorted(recursive_emails - direct_emails)

    included_action_ids = _actions_for_responsible_emails(db, direct_emails)
    excluded_action_ids = _actions_for_responsible_emails(db, n2_plus_emails)

    return {
        "hierarchy_source": TEAM_SCOPE_SOURCE,
        "source": "public.v_people_with_boss",
        "company_members_used": False,
        "email": normalize_email(email),
        "selected_person_row": direct_scope.get("selected_person"),
        "direct_reports_count": len(direct_scope.get("direct_reports") or []),
        "direct_reports": [
            {
                "name": person.get("personne"),
                "email": normalize_email(person.get("email")),
                "role": person.get("role"),
                "role_cible": person.get("role_cible"),
                "assignment_id": person.get("assignment_id"),
                "is_primary": person.get("is_primary"),
                "boss_person": person.get("boss_person"),
                "boss_email": normalize_email(person.get("boss_email")),
                "boss_role": person.get("boss_role"),
                "hierarchy_path": person.get("hierarchy_path"),
                "hr_site": person.get("hr_site"),
            }
            for person in direct_scope.get("direct_reports") or []
        ],
        "direct_report_emails": sorted(direct_emails),
        "recursive_hierarchy_count": len(recursive_emails),
        "n2_plus_emails_excluded": n2_plus_emails,
        "action_ids_included_in_team_actions": included_action_ids,
        "action_ids_excluded_because_n2_plus": excluded_action_ids,
        "warnings": direct_scope.get("warnings") or [],
    }
