from sqlalchemy.orm import Session
from app.models.action import Action
from app.models.sujet import Sujet
from sqlalchemy import case, func
import datetime

from app.services.action_event_log_service import log_action_event
from app.services.action_priority_service import (
    enrich_action_priority,
    get_reaction_time_days,
    calculate_reaction_deadline,
    is_escalation_ready
)

async def get_actions_by_sujet_id_service(sujet_id: int, db: Session):
    actions = (
        db.query(Action)
        .filter(Action.sujet_id == sujet_id)
        .order_by(Action.ordre.asc(), Action.created_at.desc())
        .all()
    )

    sujet = db.query(Sujet).filter(Sujet.id == sujet_id).first()
    root_sujet = sujet

    while root_sujet and root_sujet.parent_sujet_id is not None:
        root_sujet = db.query(Sujet).filter(Sujet.id == root_sujet.parent_sujet_id).first()

    result = []

    for action in actions:
        enrich_action_priority(action)

        result.append({
            **action.__dict__,
            "corrective_action_app": root_sujet.code.startswith("8D") if root_sujet else False,
            "rm_stock_app": "AP-RAW-MATERIAL" in root_sujet.code if root_sujet else False,
            "reaction_time_days": get_reaction_time_days(action.importance),
            "reaction_deadline": str(calculate_reaction_deadline(action.due_date, action.importance)) if action.due_date else None,
            "escalation_ready": is_escalation_ready(action),
        })

    return result


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
                func.distinct(case((Action.status == "closed", Action.id)))
            ).label("actions_completed"),
            func.count(
                func.distinct(case((Action.status == "overdue", Action.id)))
            ).label("actions_overdue"),
            func.count(
                func.distinct(case((Action.status.in_(["open", "in_progress"]), Action.id)))
            ).label("actions_in_progress"),
            func.count(
                func.distinct(case((Action.status == "blocked", Action.id)))
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

    return [email[0] for email in emails if email[0] != ""]


async def update_action_status_service(action_id: int, status: str, db: Session):
    action = db.query(Action).filter(Action.id == action_id).first()

    if not action:
        return {"error": "Action not found"}

    old_status = action.status
    action.status = status

    if status == "closed":
        action.closed_date = datetime.date.today()
    else:
        action.closed_date = None

    action.updated_at = datetime.datetime.now(datetime.timezone.utc)

    log_action_event(
        db=db,
        action_id=action.id,
        event_type="status_changed",
        old_value=old_status,
        new_value=status,
        details="Status changed from application",
        created_by="application",
    )

    db.commit()
    db.refresh(action)

    return action

def action_to_dict(action):
    enrich_action_priority(action)

    return {
        **action.__dict__,
        "reaction_time_days": get_reaction_time_days(action.importance),
        "reaction_deadline": str(calculate_reaction_deadline(action.due_date, action.importance)) if action.due_date else None,
        "escalation_ready": is_escalation_ready(action),
    }
async def get_my_actions_service(email: str, db: Session):
    actions = (
        db.query(Action)
        .filter(Action.email_responsable.ilike(email))
        .order_by(
            Action.priority_index.desc().nullslast(),
            Action.due_date.asc().nullslast(),
        )
        .all()
    )

    return [action_to_dict(action) for action in actions]
async def get_team_actions_service(email: str, db: Session, directory_db):
    from app.services.directory_service import get_all_underlings

    underlings = get_all_underlings(directory_db, email)
    underling_emails = [
        member.email.lower()
        for member in underlings
        if member.email
    ]

    if not underling_emails:
        return {
            "team_members": 0,
            "actions": [],
        }

    actions = (
        db.query(Action)
        .filter(Action.email_responsable.in_(underling_emails))
        .order_by(
            Action.priority_index.desc().nullslast(),
            Action.due_date.asc().nullslast(),
        )
        .all()
    )

    return {
        "team_members": len(underling_emails),
        "actions": [action_to_dict(action) for action in actions],
    }
async def mark_action_closed_from_email_service(action_id: int, db):
    action = db.query(Action).filter(Action.id == action_id).first()

    if not action:
        return """
        <html>
          <body style="font-family:Arial;">
            <h2>Action not found</h2>
          </body>
        </html>
        """

    old_status = action.status

    action.status = "closed"
    action.closed_date = datetime.date.today()
    action.updated_at = datetime.datetime.now(datetime.timezone.utc)

    log_action_event(
        db=db,
        action_id=action.id,
        event_type="status_changed_from_email",
        old_value=old_status,
        new_value="closed",
        details="Action marked as completed from email link",
        created_by="email_link",
    )

    db.commit()

    return f"""
    <html>
    <body style="margin:0;background:#f3f4f6;font-family:Arial,sans-serif;color:#111827;">
        <div style="min-height:100vh;display:flex;align-items:center;justify-content:center;padding:30px;">
        <div style="max-width:620px;width:100%;background:white;border-radius:24px;box-shadow:0 20px 50px rgba(15,23,42,0.16);overflow:hidden;">

            <div style="background:linear-gradient(135deg,#16a34a,#0f766e);padding:34px;color:white;text-align:center;">
            <div style="font-size:48px;margin-bottom:10px;">✓</div>
            <h1 style="margin:0;font-size:28px;">Action completed successfully</h1>
            <p style="margin:8px 0 0;color:#dcfce7;">The action status has been updated.</p>
            </div>

            <div style="padding:30px;">
            <div style="background:#f8fafc;border:1px solid #e5e7eb;border-radius:16px;padding:20px;margin-bottom:20px;">
                <div style="font-size:12px;color:#64748b;font-weight:700;text-transform:uppercase;margin-bottom:8px;">Action</div>
                <div style="font-size:18px;font-weight:800;color:#0f172a;">{action.titre}</div>
            </div>

            <div style="display:flex;gap:12px;margin-bottom:20px;">
                <div style="flex:1;background:#ecfdf5;border:1px solid #bbf7d0;border-radius:14px;padding:16px;">
                <div style="font-size:12px;color:#15803d;font-weight:700;">STATUS</div>
                <div style="font-size:20px;font-weight:800;color:#166534;">Closed</div>
                </div>

                <div style="flex:1;background:#eff6ff;border:1px solid #bfdbfe;border-radius:14px;padding:16px;">
                <div style="font-size:12px;color:#1d4ed8;font-weight:700;">CLOSED DATE</div>
                <div style="font-size:20px;font-weight:800;color:#1e3a8a;">{action.closed_date}</div>
                </div>
            </div>

            <p style="color:#475569;font-size:14px;text-align:center;margin-top:24px;">
                You can safely close this page.
            </p>
            </div>
        </div>
        </div>
    </body>
    </html>
    """