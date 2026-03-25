from sqlalchemy.orm import Session
from app.models.action import Action
from app.models.sujet import Sujet
from app.models.sujet import Sujet
from sqlalchemy import case, func

async def get_actions_by_sujet_id_service(sujet_id: int, db: Session):
    actions = (
        db.query(Action)
        .filter(Action.sujet_id == sujet_id)
        .order_by(Action.ordre.asc(), Action.created_at.desc())
        .all()
    )

    # Walk up to the root sujet (parent_sujet_id is null)
    sujet = db.query(Sujet).filter(Sujet.id == sujet_id).first()
    root_sujet = sujet

    while root_sujet and root_sujet.parent_sujet_id is not None:
        root_sujet = db.query(Sujet).filter(Sujet.id == root_sujet.parent_sujet_id).first()

    return [
        {
            **action.__dict__,
            "corrective_action_app": root_sujet.code.startswith("8D") if root_sujet else False,
            "rm_stock_app": "AP-RAW-MATERIAL" in root_sujet.code if root_sujet else False,
        }
        for action in actions
    ]

async def get_action_by_id_service(action_id: int, db: Session):
    action = db.query(Action).filter(Action.id == action_id).first()
    return action

async def get_sous_actions_by_action_id_service(action_id: int, db: Session):
    sous_actions = (
        db.query(Action)
        .filter(Action.parent_action_id == action_id)
        .order_by(Action.ordre.asc(), Action.created_at.desc())
        .all()
    )
    return sous_actions

async def get_statistiques_service(db: Session):
    stats = (
        db.query(
            func.count(func.distinct(Sujet.id)).label("total_sujets"),
            func.count(func.distinct(Action.id)).label("total_actions"),
            func.count(
                func.distinct(
                    case((Action.status == "closed", Action.id))
                )
            ).label("actions_completed"),
            func.count(
                func.distinct(
                    case((Action.status == "overdue", Action.id))
                )
            ).label("actions_overdue"),
            func.count(
                func.distinct(
                    case((Action.status.in_(["open", "in_progress"]), Action.id))
                )
            ).label("actions_in_progress"),
            func.count(
                func.distinct(
                    case((Action.status == "blocked", Action.id))
                )
            ).label("actions_blocked"),
        )
        .outerjoin(Action, Sujet.id == Action.sujet_id)
        .one()
    )

    return {
        "total_sujets": stats.total_sujets,
        "total_actions": stats.total_actions,
        "actions_completed": stats.actions_completed,
        "actions_overdue": stats.actions_overdue,
        "actions_in_progress": stats.actions_in_progress,
        "actions_blocked": stats.actions_blocked,
    }
    
async def get_emails_service(db: Session):
    emails = (
        db.query(Action.email_responsable)
        .filter(Action.email_responsable != None)
        .distinct()
        .all()
    )
    
    return [email[0] for email in emails if '' != email[0]]