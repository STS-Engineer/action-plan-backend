from sqlalchemy.orm import Session
from app.models.action import Action
from app.models.sujet import Sujet
from sqlalchemy import case, func
import datetime
from app.models.action_attachment import ActionAttachment
from app.models.action_status_comment import ActionStatusComment
from app.services.action_event_log_service import log_action_event
from app.services.action_priority_service import (
    enrich_action_priority,
    get_reaction_time_days,
    calculate_reaction_deadline,
    is_escalation_ready
)

def get_latest_action_history_map(db: Session, action_ids):
    unique_action_ids = list({action_id for action_id in action_ids if action_id is not None})

    latest_history = {
        action_id: {
            "last_status_comment": None,
            "last_status_comment_by": None,
            "last_status_comment_at": None,
            "last_attachment_id": None,
            "last_attachment_name": None,
            "last_attachment_uploaded_by": None,
            "last_attachment_created_at": None,
        }
        for action_id in unique_action_ids
    }

    if not unique_action_ids:
        return latest_history

    comments = (
        db.query(ActionStatusComment)
        .filter(ActionStatusComment.action_id.in_(unique_action_ids))
        .order_by(
            ActionStatusComment.action_id.asc(),
            ActionStatusComment.created_at.desc(),
            ActionStatusComment.id.desc(),
        )
        .all()
    )

    for comment in comments:
        history = latest_history.get(comment.action_id)

        if history and history["last_status_comment_at"] is None:
            history["last_status_comment"] = comment.comment
            history["last_status_comment_by"] = comment.created_by
            history["last_status_comment_at"] = comment.created_at

    attachments = (
        db.query(ActionAttachment)
        .filter(ActionAttachment.action_id.in_(unique_action_ids))
        .order_by(
            ActionAttachment.action_id.asc(),
            ActionAttachment.created_at.desc(),
            ActionAttachment.id.desc(),
        )
        .all()
    )

    for attachment in attachments:
        history = latest_history.get(attachment.action_id)

        if history and history["last_attachment_id"] is None:
            history["last_attachment_id"] = attachment.id
            history["last_attachment_name"] = attachment.file_name
            history["last_attachment_uploaded_by"] = attachment.uploaded_by
            history["last_attachment_created_at"] = attachment.created_at

    return latest_history


def action_to_dict(action, root_sujet=None, latest_history=None):
    enrich_action_priority(action)

    payload = {
        **action.__dict__,
        "reaction_time_days": get_reaction_time_days(action.importance),
        "reaction_deadline": str(calculate_reaction_deadline(action.due_date, action.importance)) if action.due_date else None,
        "escalation_ready": is_escalation_ready(action),
    }

    root_code = root_sujet.code if root_sujet and root_sujet.code else ""
    payload["corrective_action_app"] = root_code.startswith("8D")
    payload["rm_stock_app"] = "AP-RAW-MATERIAL" in root_code

    payload.update(latest_history or {
        "last_status_comment": None,
        "last_status_comment_by": None,
        "last_status_comment_at": None,
        "last_attachment_id": None,
        "last_attachment_name": None,
        "last_attachment_uploaded_by": None,
        "last_attachment_created_at": None,
    })

    return payload


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
    latest_history_by_action_id = get_latest_action_history_map(
        db,
        [action.id for action in actions],
    )

    for action in actions:
        result.append(
            action_to_dict(
                action,
                root_sujet=root_sujet,
                latest_history=latest_history_by_action_id.get(action.id),
            )
        )

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

    latest_history_by_action_id = get_latest_action_history_map(
        db,
        [action.id for action in sous_actions],
    )

    return [
        action_to_dict(
            action,
            latest_history=latest_history_by_action_id.get(action.id),
        )
        for action in sous_actions
    ]


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


async def update_action_status_service(
    action_id: int,
    status: str,
    db: Session,
    comment: str | None = None,
    created_by: str | None = None,
):
    action = db.query(Action).filter(Action.id == action_id).first()

    if not action:
        return {"error": "Action not found"}

    old_status = action.status

    action.status = status

    if status == "closed":
        action.closed_date = datetime.date.today()
    else:
        action.closed_date = None

    status_comment = ActionStatusComment(
        action_id=action.id,
        old_status=old_status,
        new_status=status,
        comment=comment,
        created_by=created_by,
    )

    db.add(status_comment)
    db.commit()
    db.refresh(action)

    return action


async def get_action_status_comments_service(action_id: int, db: Session):
    comments = (
        db.query(ActionStatusComment)
        .filter(ActionStatusComment.action_id == action_id)
        .order_by(ActionStatusComment.created_at.desc())
        .all()
    )

    return [
        {
            "id": comment.id,
            "action_id": comment.action_id,
            "old_status": comment.old_status,
            "new_status": comment.new_status,
            "comment": comment.comment,
            "created_by": comment.created_by,
            "created_at": comment.created_at,
        }
        for comment in comments
    ]

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

    latest_history_by_action_id = get_latest_action_history_map(
        db,
        [action.id for action in actions],
    )

    return [
        action_to_dict(
            action,
            latest_history=latest_history_by_action_id.get(action.id),
        )
        for action in actions
    ]
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

    latest_history_by_action_id = get_latest_action_history_map(
        db,
        [action.id for action in actions],
    )

    return {
        "team_members": len(underling_emails),
        "actions": [
            action_to_dict(
                action,
                latest_history=latest_history_by_action_id.get(action.id),
            )
            for action in actions
        ],
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
