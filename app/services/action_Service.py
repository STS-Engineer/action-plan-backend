from fastapi import HTTPException
from sqlalchemy.orm import Session
from app.models.action import Action
from app.models.sujet import Sujet
from sqlalchemy import and_, case, func, or_, true
import datetime
from app.models.action_attachment import ActionAttachment
from app.models.action_status_comment import ActionStatusComment
from app.services.action_event_log_service import log_action_event
from app.services.action_priority_service import (
    enrich_action_priority,
    get_reaction_time_days,
    calculate_reaction_deadline,
    is_escalation_ready,
    recalculate_action_priority_for_status_change,
)
from app.services.action_access_service import can_access_action, normalize_access_email
from app.services.action_requester_scope_service import (
    build_requester_scope_predicate,
    get_logged_user_requester_aliases,
)
from app.services.auth_service import is_admin, is_admin_role
from app.services.action_status_logic_service import (
    get_action_active_predicate,
    get_action_home_bucket,
    get_action_home_bucket_predicate,
    get_action_overdue_predicate,
    get_action_visible_from_home_predicate,
    normalize_action_status,
    get_normalized_action_status_expression,
)
from app.services.sujet_service import get_sujet_logical_group_ids


ACTION_DELETE_FORBIDDEN_MESSAGE = "You can only delete actions you own or manage."
TEAM_ACTIONS_MAX_DEPTH = 2
SUPPORTED_ACTION_SCOPES = {"my", "team", "requested_by_me", "all"}

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


def action_detail_to_dict(action, root_sujet=None, latest_history=None):
    enrich_action_priority(action)

    root_code = root_sujet.code if root_sujet and root_sujet.code else ""

    payload = {
        "id": action.id,
        "titre": action.titre,
        "description": action.description,
        "status": action.status,
        "responsable": action.responsable,
        "email_responsable": action.email_responsable,
        "demandeur": action.demandeur,
        "email_demandeur": action.email_demandeur,
        "sujet_id": action.sujet_id,
        "parent_action_id": action.parent_action_id,
        "due_date": action.due_date,
        "estimated_duration_days": action.estimated_duration_days,
        "closed_date": action.closed_date,
        "priority_index": action.priority_index,
        "priorite": action.priorite,
        "importance": action.importance,
        "urgency": action.urgency,
        "escalation_level": action.escalation_level,
        "corrective_action_app": root_code.startswith("8D"),
        "rm_stock_app": "AP-RAW-MATERIAL" in root_code,
    }

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


def build_sujet_path_info_map(db: Session, sujet_ids):
    pending_ids = {
        sujet_id
        for sujet_id in sujet_ids
        if sujet_id is not None
    }
    sujets_by_id = {}

    while pending_ids:
        sujets = (
            db.query(Sujet)
            .filter(Sujet.id.in_(pending_ids))
            .all()
        )
        pending_ids = set()

        for sujet in sujets:
            if sujet.id in sujets_by_id:
                continue

            sujets_by_id[sujet.id] = sujet

            if sujet.parent_sujet_id and sujet.parent_sujet_id not in sujets_by_id:
                pending_ids.add(sujet.parent_sujet_id)

    path_info_by_sujet_id = {}

    for sujet_id in sujet_ids:
        path = []
        visited_ids = set()
        current_sujet = sujets_by_id.get(sujet_id)

        while current_sujet and current_sujet.id not in visited_ids:
            path.append(current_sujet)
            visited_ids.add(current_sujet.id)
            current_sujet = sujets_by_id.get(current_sujet.parent_sujet_id)

        path.reverse()

        root_sujet = path[0] if path else None
        nearest_sujet = path[-1] if path else None

        path_info_by_sujet_id[sujet_id] = {
            "root_sujet": root_sujet,
            "root_sujet_title": root_sujet.titre if root_sujet else None,
            "sujet_title": nearest_sujet.titre if nearest_sujet else None,
            "topic_path": " > ".join(
                sujet.titre
                for sujet in path
                if sujet.titre
            ) or None,
        }

    return path_info_by_sujet_id


def get_team_scope_action_emails(email: str | None, directory_db) -> list[str]:
    from app.services.directory_service import get_underlings_until_depth

    normalized_email = normalize_access_email(email)

    if not normalized_email:
        return []

    underlings = get_underlings_until_depth(
        directory_db,
        normalized_email,
        max_depth=TEAM_ACTIONS_MAX_DEPTH,
    )

    return list(dict.fromkeys([
        normalized_underling_email
        for normalized_underling_email in (
            normalize_access_email(member.email)
            for member in underlings
        )
        if normalized_underling_email
    ]))


def normalize_action_scope(scope: str | None) -> str:
    normalized_scope = (scope or "my").strip().lower()

    if normalized_scope not in SUPPORTED_ACTION_SCOPES:
        return "my"

    return normalized_scope


def get_requester_aliases(
    db: Session | None,
    normalized_email: str,
    directory_db=None,
) -> list[str]:
    return get_logged_user_requester_aliases(
        db,
        normalized_email,
        directory_db=directory_db,
    )


def get_requester_action_predicate(action_like, requester_values: list[str]):
    logged_user_email = requester_values[0] if requester_values else None
    return build_requester_scope_predicate(
        action_like,
        logged_user_email,
        requester_values=requester_values,
    )


def build_action_scope_filter(
    action_like,
    normalized_email: str,
    scope: str,
    directory_db,
    requester_values: list[str] | None = None,
    user_role: str | None = None,
):
    email_responsable = func.lower(func.coalesce(action_like.email_responsable, ""))
    normalized_scope = normalize_action_scope(scope)

    if normalized_scope == "all":
        if not is_admin_role(user_role):
            return None

        return true()

    if normalized_scope == "team":
        underling_emails = get_team_scope_action_emails(normalized_email, directory_db)

        if not underling_emails:
            return None

        return email_responsable.in_(underling_emails)

    if normalized_scope == "requested_by_me":
        return get_requester_action_predicate(
            action_like,
            requester_values or [normalized_email],
        )

    return email_responsable == normalized_email


def get_flat_action_canonical_status(action) -> str | None:
    status_bucket = get_action_home_bucket(action)

    if status_bucket in {"overdue", "closed"}:
        return status_bucket

    normalized_status = normalize_action_status(getattr(action, "status", None))

    if normalized_status in {"open", "blocked", "closed"}:
        return normalized_status

    return status_bucket


def get_admin_all_status_predicate(status: str, action_like=Action):
    normalized_status = (status or "all").strip().lower()
    normalized_action_status = get_normalized_action_status_expression(action_like)

    if normalized_status == "all":
        return get_action_active_predicate(action_like)

    if normalized_status == "closed":
        return and_(
            get_action_active_predicate(action_like),
            normalized_action_status == "closed",
        )

    if normalized_status in {"overdue", "late"}:
        return and_(
            get_action_active_predicate(action_like),
            or_(
                normalized_action_status.in_(["overdue", "late"]),
                and_(
                    action_like.due_date.isnot(None),
                    action_like.due_date < func.current_date(),
                    normalized_action_status != "closed",
                ),
            ),
        )

    if normalized_status == "in_progress":
        return and_(
            get_action_active_predicate(action_like),
            normalized_action_status.in_(["open", "blocked"]),
            ~get_admin_all_status_predicate("overdue", action_like),
        )

    if normalized_status == "blocked":
        return and_(
            get_action_active_predicate(action_like),
            normalized_action_status == "blocked",
        )

    return get_action_home_bucket_predicate(normalized_status, action_like)


async def get_filtered_actions_service(
    email: str,
    scope: str,
    status: str,
    db: Session,
    directory_db,
    user_role: str | None = None,
):
    normalized_email = normalize_access_email(email)
    normalized_scope = (scope or "my").strip().lower()
    normalized_status = (status or "all").strip().lower()

    if not normalized_email:
        return []

    if normalized_scope not in SUPPORTED_ACTION_SCOPES:
        raise HTTPException(status_code=400, detail="Invalid scope")

    if normalized_status not in {"all", "closed", "in_progress", "overdue", "blocked"}:
        raise HTTPException(status_code=400, detail="Invalid status")

    admin_all_scope = normalized_scope == "all" and is_admin_role(user_role)

    if admin_all_scope:
        status_predicate = get_admin_all_status_predicate(normalized_status, Action)
    elif normalized_status == "in_progress":
        normalized_action_status = get_normalized_action_status_expression(Action)
        status_predicate = and_(
            get_action_visible_from_home_predicate(Action),
            normalized_action_status.in_(["open", "blocked"]),
            ~get_action_overdue_predicate(Action),
        )
    else:
        status_predicate = get_action_home_bucket_predicate(
            None if normalized_status == "all" else normalized_status,
            Action,
        )

    filters = [get_action_active_predicate(Action), status_predicate]

    scope_filter = build_action_scope_filter(
        Action,
        normalized_email,
        normalized_scope,
        directory_db,
        requester_values=(
            get_requester_aliases(db, normalized_email, directory_db)
            if normalized_scope == "requested_by_me"
            else None
        ),
        user_role=user_role,
    )

    if scope_filter is None:
        return []

    filters.append(scope_filter)

    actions = (
        db.query(Action)
        .filter(*filters)
        .order_by(
            Action.priority_index.desc().nullslast(),
            Action.due_date.asc().nullslast(),
            Action.created_at.desc(),
        )
        .all()
    )

    latest_history_by_action_id = get_latest_action_history_map(
        db,
        [action.id for action in actions],
    )
    sujet_path_info_by_id = build_sujet_path_info_map(
        db,
        [action.sujet_id for action in actions],
    )

    result = []

    for action in actions:
        sujet_info = sujet_path_info_by_id.get(action.sujet_id, {})
        status_bucket = get_action_home_bucket(action)
        payload = action_to_dict(
            action,
            root_sujet=sujet_info.get("root_sujet"),
            latest_history=latest_history_by_action_id.get(action.id),
        )
        payload.pop("_sa_instance_state", None)
        payload.update({
            "topic_path": sujet_info.get("topic_path"),
            "sujet_title": sujet_info.get("sujet_title"),
            "root_sujet_title": sujet_info.get("root_sujet_title"),
            "status_bucket": status_bucket,
            "canonical_status": get_flat_action_canonical_status(action),
        })
        result.append(payload)

    return result


async def get_actions_by_sujet_id_service(
    sujet_id: int,
    db: Session,
    email: str | None = None,
    scope: str | None = None,
    directory_db=None,
    status: str | None = None,
    user_role: str | None = None,
):
    normalized_email = normalize_access_email(email)
    admin_all_scope = normalize_action_scope(scope) == "all" and is_admin_role(user_role)
    action_visibility_predicate = (
        get_action_active_predicate(Action)
        if admin_all_scope or not normalized_email
        else get_action_visible_from_home_predicate(Action)
    )
    sujet_ids = get_sujet_logical_group_ids(db, sujet_id)

    filters = [
        Action.sujet_id.in_(sujet_ids),
        action_visibility_predicate,
    ]

    if normalized_email:
        scope_filter = build_action_scope_filter(
            Action,
            normalized_email,
            scope or "my",
            directory_db,
            requester_values=(
                get_requester_aliases(db, normalized_email, directory_db)
                if normalize_action_scope(scope) == "requested_by_me"
                else None
            ),
            user_role=user_role,
        )

        if scope_filter is None:
            return []

        filters.append(scope_filter)

    if status:
        filters.append(
            get_admin_all_status_predicate(status, Action)
            if admin_all_scope
            else get_action_home_bucket_predicate(status, Action)
        )

    actions = (
        db.query(Action)
        .filter(*filters)
        .order_by(Action.ordre.asc(), Action.created_at.desc())
        .all()
    )

    result = []
    latest_history_by_action_id = get_latest_action_history_map(
        db,
        [action.id for action in actions],
    )
    sujet_path_info_by_id = build_sujet_path_info_map(
        db,
        [action.sujet_id for action in actions],
    )

    for action in actions:
        sujet_info = sujet_path_info_by_id.get(action.sujet_id, {})
        result.append(
            action_to_dict(
                action,
                root_sujet=sujet_info.get("root_sujet"),
                latest_history=latest_history_by_action_id.get(action.id),
            )
        )

    return result


async def get_action_by_id_service(
    action_id: int,
    db: Session,
    directory_db=None,
    current_user=None,
):
    action = (
        db.query(Action)
        .filter(Action.id == action_id)
        .filter(get_action_active_predicate(Action))
        .first()
    )

    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    if current_user is not None:
        access = can_access_action(
            normalize_access_email(getattr(current_user, "email", None)),
            action,
            directory_db,
            user_role=getattr(current_user, "role", None),
        )

        if not access["allowed"]:
            raise HTTPException(status_code=403, detail="Forbidden")

    sujet = db.query(Sujet).filter(Sujet.id == action.sujet_id).first()
    root_sujet = sujet

    while root_sujet and root_sujet.parent_sujet_id is not None:
        root_sujet = db.query(Sujet).filter(Sujet.id == root_sujet.parent_sujet_id).first()

    latest_history_by_action_id = get_latest_action_history_map(db, [action.id])

    return action_detail_to_dict(
        action,
        root_sujet=root_sujet,
        latest_history=latest_history_by_action_id.get(action.id),
    )


async def get_sous_actions_by_action_id_service(
    action_id: int,
    db: Session,
    directory_db=None,
    current_user=None,
):
    if current_user is not None:
        parent_action = (
            db.query(Action)
            .filter(Action.id == action_id)
            .filter(get_action_active_predicate(Action))
            .first()
        )

        if not parent_action:
            raise HTTPException(status_code=404, detail="Action not found")

        access = can_access_action(
            normalize_access_email(getattr(current_user, "email", None)),
            parent_action,
            directory_db,
            user_role=getattr(current_user, "role", None),
        )

        if not access["allowed"]:
            raise HTTPException(status_code=403, detail="Forbidden")

    sous_actions = (
        db.query(Action)
        .filter(Action.parent_action_id == action_id)
        .filter(get_action_active_predicate(Action))
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
        .outerjoin(Action, and_(Sujet.id == Action.sujet_id, get_action_active_predicate(Action)))
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
        .filter(get_action_active_predicate(Action))
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
    directory_db=None,
    current_user=None,
):
    action = (
        db.query(Action)
        .filter(Action.id == action_id)
        .filter(get_action_active_predicate(Action))
        .first()
    )

    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    if current_user is not None:
        access = can_access_action(
            normalize_access_email(getattr(current_user, "email", None)),
            action,
            directory_db,
            user_role=getattr(current_user, "role", None),
        )

        if not access["allowed"]:
            raise HTTPException(status_code=403, detail="Forbidden")

    old_status = action.status
    normalized_status = normalize_action_status(status) or "open"
    now = datetime.datetime.now(datetime.timezone.utc)

    action.status = normalized_status

    if normalized_status == "closed":
        action.closed_date = datetime.date.today()
    else:
        action.closed_date = None

    action.updated_at = now
    recalculate_action_priority_for_status_change(action)

    status_comment = ActionStatusComment(
        action_id=action.id,
        old_status=old_status,
        new_status=normalized_status,
        comment=comment,
        created_by=created_by,
    )

    db.add(status_comment)

    if current_user is not None and is_admin(current_user):
        log_action_event(
            db=db,
            action_id=action.id,
            event_type="admin_status_changed",
            old_value=old_status,
            new_value=normalized_status,
            details=f"Admin changed action status from {old_status} to {normalized_status}.",
            created_by=normalize_access_email(getattr(current_user, "email", None)),
        )

    db.commit()
    db.refresh(action)

    latest_history_by_action_id = get_latest_action_history_map(db, [action.id])
    sujet_path_info_by_id = build_sujet_path_info_map(db, [action.sujet_id])
    sujet_info = sujet_path_info_by_id.get(action.sujet_id, {})
    payload = action_to_dict(
        action,
        root_sujet=sujet_info.get("root_sujet"),
        latest_history=latest_history_by_action_id.get(action.id),
    )
    payload.pop("_sa_instance_state", None)
    payload.update({
        "topic_path": sujet_info.get("topic_path"),
        "sujet_title": sujet_info.get("sujet_title"),
        "root_sujet_title": sujet_info.get("root_sujet_title"),
        "status_bucket": get_action_home_bucket(action),
        "canonical_status": get_flat_action_canonical_status(action),
    })

    return payload


async def get_action_status_comments_service(
    action_id: int,
    db: Session,
    directory_db=None,
    current_user=None,
):
    if current_user is not None:
        action = (
            db.query(Action)
            .filter(Action.id == action_id)
            .filter(get_action_active_predicate(Action))
            .first()
        )

        if not action:
            raise HTTPException(status_code=404, detail="Action not found")

        access = can_access_action(
            normalize_access_email(getattr(current_user, "email", None)),
            action,
            directory_db,
            user_role=getattr(current_user, "role", None),
        )

        if not access["allowed"]:
            raise HTTPException(status_code=403, detail="Forbidden")

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
    normalized_email = normalize_access_email(email)

    if not normalized_email:
        return []

    actions = (
        db.query(Action)
        .filter(func.lower(func.coalesce(Action.email_responsable, "")) == normalized_email)
        .filter(get_action_active_predicate(Action))
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
    normalized_email = normalize_access_email(email)

    if not normalized_email:
        return {
            "team_members": 0,
            "actions": [],
        }

    underling_emails = get_team_scope_action_emails(normalized_email, directory_db)

    if not underling_emails:
        return {
            "team_members": 0,
            "actions": [],
        }

    actions = (
        db.query(Action)
        .filter(func.lower(func.coalesce(Action.email_responsable, "")).in_(underling_emails))
        .filter(get_action_active_predicate(Action))
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


def _collect_active_action_subtree_ids(action_id: int, db: Session) -> list[int]:
    collected_ids: list[int] = []
    seen_ids: set[int] = set()
    pending_ids = [action_id]

    while pending_ids:
        current_ids = [
            pending_id
            for pending_id in pending_ids
            if pending_id not in seen_ids
        ]

        if not current_ids:
            break

        actions = (
            db.query(Action.id)
            .filter(Action.id.in_(current_ids))
            .filter(get_action_active_predicate(Action))
            .all()
        )
        active_ids = [action.id for action in actions]

        for active_id in active_ids:
            seen_ids.add(active_id)
            collected_ids.append(active_id)

        child_rows = (
            db.query(Action.id)
            .filter(Action.parent_action_id.in_(active_ids))
            .filter(get_action_active_predicate(Action))
            .all()
            if active_ids
            else []
        )
        pending_ids = [
            child.id
            for child in child_rows
            if child.id not in seen_ids
        ]

    return collected_ids


def _collect_action_subtree_ids(action_id: int, db: Session) -> list[int]:
    collected_ids: list[int] = []
    seen_ids: set[int] = set()
    pending_ids = [action_id]

    while pending_ids:
        current_ids = [
            pending_id
            for pending_id in pending_ids
            if pending_id not in seen_ids
        ]

        if not current_ids:
            break

        actions = (
            db.query(Action.id)
            .filter(Action.id.in_(current_ids))
            .all()
        )
        action_ids = [action.id for action in actions]

        for current_action_id in action_ids:
            seen_ids.add(current_action_id)
            collected_ids.append(current_action_id)

        child_rows = (
            db.query(Action.id)
            .filter(Action.parent_action_id.in_(action_ids))
            .all()
            if action_ids
            else []
        )
        pending_ids = [
            child.id
            for child in child_rows
            if child.id not in seen_ids
        ]

    return collected_ids


async def delete_action_service(
    action_id: int,
    db: Session,
    directory_db,
    current_user,
):
    logged_email = normalize_access_email(getattr(current_user, "email", None))

    action = (
        db.query(Action)
        .filter(Action.id == action_id)
        .filter(get_action_active_predicate(Action))
        .first()
    )

    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    access = can_access_action(
        logged_email,
        action,
        directory_db,
        user_role=getattr(current_user, "role", None),
    )

    if not access["allowed"]:
        raise HTTPException(status_code=403, detail=ACTION_DELETE_FORBIDDEN_MESSAGE)

    subtree_ids = _collect_active_action_subtree_ids(action.id, db)
    now = datetime.datetime.now(datetime.timezone.utc)

    actions = (
        db.query(Action)
        .filter(Action.id.in_(subtree_ids))
        .filter(get_action_active_predicate(Action))
        .all()
    )

    for subtree_action in actions:
        subtree_action.is_deleted = True
        subtree_action.deleted_at = now
        subtree_action.deleted_by = logged_email
        subtree_action.updated_at = now

        log_action_event(
            db=db,
            action_id=subtree_action.id,
            event_type="action_archived",
            old_value="active",
            new_value="deleted",
            details=f"Action deleted/archived by {logged_email}",
            created_by=logged_email,
        )

    db.commit()

    return {
        "deleted": True,
        "action_id": action_id,
        "deleted_action_ids": subtree_ids,
        "count": len(subtree_ids),
        "message": "Action deleted.",
    }


async def restore_action_service(
    action_id: int,
    db: Session,
    current_user,
):
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Administrator access required.")

    action = db.query(Action).filter(Action.id == action_id).first()

    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    subtree_ids = _collect_action_subtree_ids(action.id, db)
    now = datetime.datetime.now(datetime.timezone.utc)
    logged_email = normalize_access_email(getattr(current_user, "email", None))
    actions = db.query(Action).filter(Action.id.in_(subtree_ids)).all()

    for subtree_action in actions:
        old_value = "deleted" if subtree_action.is_deleted else "active"
        subtree_action.is_deleted = False
        subtree_action.deleted_at = None
        subtree_action.deleted_by = None
        subtree_action.updated_at = now

        log_action_event(
            db=db,
            action_id=subtree_action.id,
            event_type="admin_action_restored",
            old_value=old_value,
            new_value="active",
            details=f"Action restored by admin {logged_email}",
            created_by=logged_email,
        )

    db.commit()

    return {
        "restored": True,
        "action_id": action_id,
        "restored_action_ids": subtree_ids,
        "count": len(subtree_ids),
        "message": "Action restored.",
    }


async def mark_action_closed_from_email_service(action_id: int, db):
    action = (
        db.query(Action)
        .filter(Action.id == action_id)
        .filter(get_action_active_predicate(Action))
        .first()
    )

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
    recalculate_action_priority_for_status_change(action)

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
