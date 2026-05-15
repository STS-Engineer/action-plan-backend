from sqlalchemy import func, or_
from app.models.action import Action
from app.services.action_Service import action_to_dict, get_latest_action_history_map
from app.models.sujet import Sujet
from app.services.action_access_service import normalize_access_email
from app.services.directory_service import get_all_underlings


def get_team_scope_emails(directory_db, email: str | None) -> list[str]:
    normalized_email = normalize_access_email(email)

    if not normalized_email or directory_db is None:
        return []

    return [
        normalized_underling_email
        for normalized_underling_email in (
            normalize_access_email(member.email)
            for member in get_all_underlings(directory_db, normalized_email)
        )
        if normalized_underling_email
    ]


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

    if (email or scope) and normalized_scope not in {"my", "team"}:
        return []

    filters = [
        or_(
            Action.titre.ilike(search),
            Action.description.ilike(search),
            Action.responsable.ilike(search),
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
