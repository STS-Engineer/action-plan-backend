from app.models.sujet import Sujet
from app.models.action import Action
from sqlalchemy import case, func
from sqlalchemy.orm import aliased, Session
from sqlalchemy import or_, select


async def getSujetsService(db: Session):
    sujets = (
        db.query(
            Sujet,
            func.count(func.distinct(Action.id)).label("total_actions"),
            func.count(func.distinct(
                case((Action.status == "completed", Action.id))
            )).label("completed_actions"),
            func.count(func.distinct(
                case((Action.status == "overdue", Action.id))
            )).label("overdue_actions"),
        )
        .outerjoin(Action, Sujet.id == Action.sujet_id)
        .group_by(Sujet.id)
        .order_by(Sujet.created_at.desc())
        .all()
    )
    
    return [
        {
            **sujet.__dict__,
            "total_actions": total_actions,
            "completed_actions": completed_actions,
            "overdue_actions": overdue_actions
        } 
        for sujet, total_actions, completed_actions, overdue_actions in sujets
    ]
    
async def getSujetsRacineService(db: Session, email: str | None = None):
    SousSujet = aliased(Sujet)

    query = (
        db.query(
            Sujet,
            func.count(func.distinct(Action.id)).label("total_actions"),
            func.count(func.distinct(SousSujet.id)).label("total_sous_sujets"),
        )
        .outerjoin(Action, Sujet.id == Action.sujet_id)
        .outerjoin(SousSujet, Sujet.id == SousSujet.parent_sujet_id)
        .filter(Sujet.parent_sujet_id.is_(None))
    )

    if email:
        # Subquery: find root sujet IDs that have the email
        # either on a direct action OR on an action under a child sujet
        ChildSujet = aliased(Sujet)
        ChildAction = aliased(Action)

        email_subquery = (
            db.query(Sujet.id)
            .outerjoin(Action, Sujet.id == Action.sujet_id)
            .outerjoin(ChildSujet, Sujet.id == ChildSujet.parent_sujet_id)
            .outerjoin(ChildAction, ChildSujet.id == ChildAction.sujet_id)
            .filter(Sujet.parent_sujet_id.is_(None))
            .filter(
                or_(
                    Action.email_responsable == email,
                    ChildAction.email_responsable == email,
                )
            )
            .subquery()
        )

        query = query.filter(Sujet.id.in_(select(email_subquery)))

    sujets_racine = (
        query
        .group_by(Sujet.id)
        .order_by(Sujet.created_at.desc())
        .all()
    )

    return [
        {
            **sujet.__dict__,
            "total_actions": total,
            "total_sous_sujets": sous,
        }
        for sujet, total, sous in sujets_racine
    ]
    
async def get_sous_sujets_by_sujet_id_service(sujet_id: int, db: Session):
    sous_sujets = (
        db.query(Sujet)
        .filter(Sujet.parent_sujet_id == sujet_id)
        .order_by(Sujet.created_at.desc())
        .all()
    )
    return sous_sujets