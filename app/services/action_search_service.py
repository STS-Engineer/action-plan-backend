from sqlalchemy import func, or_
from app.models.action import Action
from app.services.action_Service import action_to_dict, get_latest_action_history_map
from app.models.sujet import Sujet
from app.services.action_access_service import normalize_access_email
from app.services.action_requester_scope_service import (
    build_requester_scope_predicate,
    get_logged_user_requester_aliases,
)
from app.services.action_status_logic_service import get_action_active_predicate
from app.services.directory_service import get_underlings_until_depth


TEAM_ACTIONS_MAX_DEPTH = 2
SUPPORTED_SEARCH_SCOPES = {"my", "team", "requested_by_me"}


def get_team_scope_emails(directory_db, email: str | None) -> list[str]:
    normalized_email = normalize_access_email(email)

    if not normalized_email or directory_db is None:
        return []

    return list(dict.fromkeys([
        normalized_underling_email
        for normalized_underling_email in (
            normalize_access_email(member.email)
            for member in get_underlings_until_depth(
                directory_db,
                normalized_email,
                max_depth=TEAM_ACTIONS_MAX_DEPTH,
            )
        )
        if normalized_underling_email
    ]))


def get_requester_aliases(db, normalized_email: str | None, directory_db=None) -> list[str]:
    return get_logged_user_requester_aliases(
        db,
        normalized_email,
        directory_db=directory_db,
    )


async def search_actions_service(
    query: str,
    db,
    email: str | None = None,
    scope: str | None = None,
    directory_db=None,
):
    if not query or query.strip() == "":
        return []

    search = f"%{query.strip()}%"
    normalized_email = normalize_access_email(email)
    normalized_scope = scope.strip().lower() if scope else None
    email_responsable = func.lower(func.coalesce(Action.email_responsable, ""))

    if (email or scope) and normalized_scope not in SUPPORTED_SEARCH_SCOPES:
        return []

    filters = [
        get_action_active_predicate(Action),
        or_(
            Action.titre.ilike(search),
            Action.description.ilike(search),
            Action.responsable.ilike(search),
            Action.demandeur.ilike(search),
            Action.email_demandeur.ilike(search),
            Action.status.ilike(search),
            Action.importance.ilike(search),
            Action.urgency.ilike(search),
        )
    ]

    if normalized_scope == "my":
        if not normalized_email:
            return []

        filters.append(email_responsable == normalized_email)

    if normalized_scope == "team":
        underling_emails = get_team_scope_emails(directory_db, normalized_email)

        if not underling_emails:
            return []

        filters.append(email_responsable.in_(underling_emails))

    if normalized_scope == "requested_by_me":
        if not normalized_email:
            return []

        requester_values = get_requester_aliases(db, normalized_email, directory_db)

        requester_scope_filter = build_requester_scope_predicate(
            Action,
            normalized_email,
            requester_values=requester_values,
        )

        if requester_scope_filter is None:
            return []

        filters.append(requester_scope_filter)

    actions = (
        db.query(Action)
        .filter(*filters)
        .order_by(
            Action.priority_index.desc().nullslast(),
            Action.due_date.asc().nullslast(),
        )
        .limit(100)
        .all()
    )

    results = []
    latest_history_by_action_id = get_latest_action_history_map(
        db,
        [action.id for action in actions],
    )

    for action in actions:
        sujet = db.query(Sujet).filter(Sujet.id == action.sujet_id).first()
        root_sujet = sujet

        while root_sujet and root_sujet.parent_sujet_id is not None:
            root_sujet = db.query(Sujet).filter(Sujet.id == root_sujet.parent_sujet_id).first()

        results.append(
            action_to_dict(
                action,
                root_sujet=root_sujet,
                latest_history=latest_history_by_action_id.get(action.id),
            )
        )

    return results
