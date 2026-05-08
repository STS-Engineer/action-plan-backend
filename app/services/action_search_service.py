from sqlalchemy import or_
from app.models.action import Action
from app.services.action_priority_service import (
    enrich_action_priority,
    get_reaction_time_days,
    calculate_reaction_deadline,
    is_escalation_ready
)
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

    for action in actions:
        enrich_action_priority(action)

    results = []

    for action in actions:
        enrich_action_priority(action)

        sujet = db.query(Sujet).filter(Sujet.id == action.sujet_id).first()
        root_sujet = sujet

        while root_sujet and root_sujet.parent_sujet_id is not None:
            root_sujet = db.query(Sujet).filter(Sujet.id == root_sujet.parent_sujet_id).first()

        results.append({
            **action.__dict__,
            "corrective_action_app": root_sujet.code.startswith("8D") if root_sujet else False,
            "rm_stock_app": "AP-RAW-MATERIAL" in root_sujet.code if root_sujet else False,
            "reaction_time_days": get_reaction_time_days(action.importance),
            "reaction_deadline": str(calculate_reaction_deadline(action.due_date, action.importance)) if action.due_date else None,
            "escalation_ready": is_escalation_ready(action),
        })

    return results