from sqlalchemy import or_
from app.models.action import Action
from app.services.action_Service import action_to_dict, get_latest_action_history_map
from app.models.sujet import Sujet

async def search_actions_service(query: str, db):
    if not query or query.strip() == "":
        return []

    search = f"%{query.lower()}%"

    actions = (
        db.query(Action)
        .filter(
            or_(
                Action.titre.ilike(search),
                Action.description.ilike(search),
                Action.responsable.ilike(search),
                Action.status.ilike(search),
                Action.importance.ilike(search),
                Action.urgency.ilike(search),
            )
        )
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
