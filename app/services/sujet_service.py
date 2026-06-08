from app.models.sujet import Sujet
from app.models.action import Action
from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.orm import Session, aliased

from app.services.action_requester_scope_service import (
    build_requester_scope_predicate,
    get_logged_user_requester_aliases,
    normalize_requester_value,
    unique_requester_values,
)
from app.services.auth_service import is_admin_role
from app.services.action_status_logic_service import (
    get_action_active_predicate,
    get_action_blocked_predicate,
    get_action_closed_visible_predicate,
    get_action_home_bucket_predicate,
    get_action_in_progress_predicate,
    get_action_overdue_predicate,
    get_action_visible_from_home_predicate,
    get_normalized_action_status_expression,
)


TEAM_ACTIONS_MAX_DEPTH = 2
SUPPORTED_HOME_SCOPES = {"my", "team", "requested_by_me", "all"}

ZERO_HOME_SUMMARY = {
    "total_sujets": 0,
    "total_actions": 0,
    "actions_completed": 0,
    "actions_in_progress": 0,
    "actions_overdue": 0,
    "actions_open": 0,
    "actions_blocked": 0,
}


def normalize_scope_email(email: str | None) -> str | None:
    return normalize_requester_value(email)


def normalize_scope_emails(emails: list[str] | None) -> list[str]:
    return unique_requester_values(emails or [])


def get_requester_aliases(
    db: Session | None,
    email: str | None,
    directory_db=None,
) -> list[str]:
    return get_logged_user_requester_aliases(db, email, directory_db=directory_db)


def build_sujet_tree_cte():
    sujet_tree = (
        select(
            Sujet.id.label("root_id"),
            Sujet.id.label("sujet_id"),
        )
        .where(Sujet.parent_sujet_id.is_(None))
        .cte(name="sujet_tree", recursive=True)
    )

    child_sujet = aliased(Sujet)

    return sujet_tree.union_all(
        select(
            sujet_tree.c.root_id,
            child_sujet.id,
        )
        .where(child_sujet.parent_sujet_id == sujet_tree.c.sujet_id)
    )


def build_child_sujet_tree_cte(parent_sujet_id: int):
    sujet_tree = (
        select(
            Sujet.id.label("root_id"),
            Sujet.id.label("sujet_id"),
        )
        .where(Sujet.parent_sujet_id == parent_sujet_id)
        .cte(name="child_sujet_tree", recursive=True)
    )

    child_sujet = aliased(Sujet)

    return sujet_tree.union_all(
        select(
            sujet_tree.c.root_id,
            child_sujet.id,
        )
        .where(child_sujet.parent_sujet_id == sujet_tree.c.sujet_id)
    )


def build_scope_predicate(
    action_like=Action,
    email: str | None = None,
    emails: list[str] | None = None,
    requester_email: str | None = None,
    requester_values: list[str] | None = None,
):
    email_col = func.lower(func.coalesce(action_like.email_responsable, ""))

    if requester_email:
        return build_requester_scope_predicate(
            action_like,
            requester_email,
            requester_values=requester_values,
        )

    if email:
        return email_col == email

    if emails:
        return email_col.in_(emails)

    return None


def build_visible_scoped_actions_subquery(
    email: str | None = None,
    emails: list[str] | None = None,
    requester_email: str | None = None,
    requester_values: list[str] | None = None,
    sujet_tree=None,
    include_hidden_closed: bool = False,
):
    if sujet_tree is None:
        sujet_tree = build_sujet_tree_cte()

    visibility_predicate = (
        get_action_active_predicate(Action)
        if include_hidden_closed
        else get_action_visible_from_home_predicate(Action)
    )

    query = (
        select(
            sujet_tree.c.root_id.label("root_id"),
            Action.id.label("action_id"),
            Action.status.label("status"),
            Action.due_date.label("due_date"),
            Action.closed_date.label("closed_date"),
            Action.is_deleted.label("is_deleted"),
        )
        .select_from(sujet_tree)
        .join(Action, Action.sujet_id == sujet_tree.c.sujet_id)
        .where(visibility_predicate)
    )

    scope_predicate = build_scope_predicate(
        Action,
        email=email,
        emails=emails,
        requester_email=requester_email,
        requester_values=requester_values,
    )

    if scope_predicate is not None:
        query = query.where(scope_predicate)

    return query.subquery()


def get_scoped_closed_predicate(scoped_actions, include_hidden_closed: bool = False):
    if include_hidden_closed:
        return get_normalized_action_status_expression(scoped_actions) == "closed"

    return get_action_closed_visible_predicate(scoped_actions)


def get_scoped_overdue_predicate(scoped_actions, include_hidden_closed: bool = False):
    if include_hidden_closed:
        status_expr = get_normalized_action_status_expression(scoped_actions)

        return or_(
            status_expr.in_(["overdue", "late"]),
            (
                (scoped_actions.c.due_date.isnot(None))
                & (scoped_actions.c.due_date < func.current_date())
                & (status_expr != "closed")
            ),
        )

    return get_action_overdue_predicate(scoped_actions)


def get_scoped_in_progress_predicate(scoped_actions, include_hidden_closed: bool = False):
    if include_hidden_closed:
        status_expr = get_normalized_action_status_expression(scoped_actions)

        return and_(
            status_expr.in_(["open", "blocked"]),
            ~get_scoped_overdue_predicate(scoped_actions, include_hidden_closed=True),
        )

    return get_action_in_progress_predicate(scoped_actions)


def get_scoped_blocked_predicate(scoped_actions, include_hidden_closed: bool = False):
    if include_hidden_closed:
        return get_normalized_action_status_expression(scoped_actions) == "blocked"

    return get_action_blocked_predicate(scoped_actions)


def build_root_action_stats_subquery(scoped_actions, include_hidden_closed: bool = False):
    return (
        select(
            scoped_actions.c.root_id.label("root_id"),
            func.count(func.distinct(scoped_actions.c.action_id)).label("total_actions"),
            func.count(
                func.distinct(
                    case(
                        (get_scoped_closed_predicate(scoped_actions, include_hidden_closed), scoped_actions.c.action_id),
                    )
                )
            ).label("completed_actions"),
            func.count(
                func.distinct(
                    case(
                        (get_scoped_overdue_predicate(scoped_actions, include_hidden_closed), scoped_actions.c.action_id),
                    )
                )
            ).label("overdue_actions"),
            func.count(
                func.distinct(
                    case(
                        (get_scoped_in_progress_predicate(scoped_actions, include_hidden_closed), scoped_actions.c.action_id),
                    )
                )
            ).label("in_progress_actions"),
            func.count(
                func.distinct(
                    case(
                        (get_scoped_blocked_predicate(scoped_actions, include_hidden_closed), scoped_actions.c.action_id),
                    )
                )
            ).label("blocked_actions"),
        )
        .select_from(scoped_actions)
        .group_by(scoped_actions.c.root_id)
        .subquery()
    )


def build_matching_root_ids_query(
    scoped_actions,
    status: str | None = None,
    include_hidden_closed: bool = False,
):
    query = select(scoped_actions.c.root_id).select_from(scoped_actions)

    if status:
        normalized_status = str(status or "").strip().lower()

        if include_hidden_closed and normalized_status == "closed":
            query = query.where(get_scoped_closed_predicate(scoped_actions, include_hidden_closed=True))
        elif include_hidden_closed and normalized_status in {"overdue", "late"}:
            query = query.where(get_scoped_overdue_predicate(scoped_actions, include_hidden_closed=True))
        elif include_hidden_closed and normalized_status == "in_progress":
            query = query.where(get_scoped_in_progress_predicate(scoped_actions, include_hidden_closed=True))
        elif include_hidden_closed and normalized_status == "blocked":
            query = query.where(get_scoped_blocked_predicate(scoped_actions, include_hidden_closed=True))
        else:
            query = query.where(get_action_home_bucket_predicate(status, scoped_actions))

    return query.distinct()


def build_direct_child_count_subquery():
    child_sujet = aliased(Sujet)

    return (
        select(
            Sujet.id.label("root_id"),
            func.count(func.distinct(child_sujet.id)).label("total_sous_sujets"),
        )
        .select_from(Sujet)
        .outerjoin(child_sujet, Sujet.id == child_sujet.parent_sujet_id)
        .group_by(Sujet.id)
        .subquery()
    )


def serialize_root_sujet_row(
    sujet,
    total_actions,
    completed_actions,
    overdue_actions,
    in_progress_actions,
    blocked_actions,
    total_sous_sujets,
):
    return {
        **sujet.__dict__,
        "total_actions": total_actions or 0,
        "completed_actions": completed_actions or 0,
        "overdue_actions": overdue_actions or 0,
        "in_progress_actions": in_progress_actions or 0,
        "blocked_actions": blocked_actions or 0,
        "total_sous_sujets": total_sous_sujets or 0,
    }


def get_scope_emails_for_team(email: str | None, directory_db) -> list[str]:
    from app.services.directory_service import get_underlings_until_depth

    normalized_email = normalize_scope_email(email)

    if not normalized_email:
        return []

    underlings = get_underlings_until_depth(
        directory_db,
        normalized_email,
        max_depth=TEAM_ACTIONS_MAX_DEPTH,
    )

    return normalize_scope_emails([
        member.email
        for member in underlings
        if member.email
    ])


def normalize_home_scope(scope: str | None) -> str:
    normalized_scope = (scope or "my").strip().lower()

    if normalized_scope not in SUPPORTED_HOME_SCOPES:
        return "my"

    return normalized_scope


def build_scoped_actions_for_home_scope(
    email: str | None,
    scope: str | None,
    directory_db=None,
    sujet_tree=None,
    db: Session | None = None,
    user_role: str | None = None,
):
    normalized_email = normalize_scope_email(email)
    normalized_scope = normalize_home_scope(scope)

    if normalized_scope == "all":
        if not is_admin_role(user_role):
            return None

        return build_visible_scoped_actions_subquery(
            sujet_tree=sujet_tree,
            include_hidden_closed=True,
        )

    if not normalized_email:
        return None

    if normalized_scope == "team":
        scope_emails = get_scope_emails_for_team(normalized_email, directory_db)

        if not scope_emails:
            return None

        return build_visible_scoped_actions_subquery(
            emails=scope_emails,
            sujet_tree=sujet_tree,
        )

    if normalized_scope == "requested_by_me":
        return build_visible_scoped_actions_subquery(
            requester_email=normalized_email,
            requester_values=get_requester_aliases(db, normalized_email, directory_db),
            sujet_tree=sujet_tree,
        )

    return build_visible_scoped_actions_subquery(
        email=normalized_email,
        sujet_tree=sujet_tree,
    )


async def getSujetsService(db: Session):
    sujets = (
        db.query(
            Sujet,
            func.count(func.distinct(Action.id)).label("total_actions"),
            func.count(
                func.distinct(
                    case((Action.status == "completed", Action.id))
                )
            ).label("completed_actions"),
            func.count(
                func.distinct(
                    case((Action.status == "overdue", Action.id))
                )
            ).label("overdue_actions"),
        )
        .outerjoin(Action, (Sujet.id == Action.sujet_id) & get_action_active_predicate(Action))
        .group_by(Sujet.id)
        .order_by(Sujet.created_at.desc())
        .all()
    )

    return [
        {
            **sujet.__dict__,
            "total_actions": total_actions,
            "completed_actions": completed_actions,
            "overdue_actions": overdue_actions,
        }
        for sujet, total_actions, completed_actions, overdue_actions in sujets
    ]


async def get_home_summary_service(
    email: str,
    scope: str,
    db: Session,
    directory_db,
    user_role: str | None = None,
):
    include_hidden_closed = normalize_home_scope(scope) == "all" and is_admin_role(user_role)
    scoped_actions = build_scoped_actions_for_home_scope(
        email=email,
        scope=scope,
        directory_db=directory_db,
        db=db,
        user_role=user_role,
    )

    if scoped_actions is None:
        return dict(ZERO_HOME_SUMMARY)

    summary = (
        db.query(
            func.count(func.distinct(scoped_actions.c.root_id)).label("total_sujets"),
            func.count(func.distinct(scoped_actions.c.action_id)).label("total_actions"),
            func.count(
                func.distinct(
                    case(
                        (get_scoped_closed_predicate(scoped_actions, include_hidden_closed), scoped_actions.c.action_id),
                    )
                )
            ).label("actions_completed"),
            func.count(
                func.distinct(
                    case(
                        (get_scoped_overdue_predicate(scoped_actions, include_hidden_closed), scoped_actions.c.action_id),
                    )
                )
            ).label("actions_overdue"),
            func.count(
                func.distinct(
                    case(
                        (get_scoped_in_progress_predicate(scoped_actions, include_hidden_closed), scoped_actions.c.action_id),
                    )
                )
            ).label("actions_in_progress"),
            func.count(
                func.distinct(
                    case((get_normalized_action_status_expression(scoped_actions) == "open", scoped_actions.c.action_id))
                )
            ).label("actions_open"),
            func.count(
                func.distinct(
                    case((get_scoped_blocked_predicate(scoped_actions, include_hidden_closed), scoped_actions.c.action_id))
                )
            ).label("actions_blocked"),
        )
        .select_from(scoped_actions)
        .one()
    )

    return {
        "total_sujets": summary.total_sujets or 0,
        "total_actions": summary.total_actions or 0,
        "actions_completed": summary.actions_completed or 0,
        "actions_in_progress": summary.actions_in_progress or 0,
        "actions_overdue": summary.actions_overdue or 0,
        "actions_open": summary.actions_open or 0,
        "actions_blocked": summary.actions_blocked or 0,
    }


async def getSujetsRacineService(
    db: Session,
    email: str | None = None,
    status: str | None = None,
    scope: str = "my",
    directory_db=None,
    user_role: str | None = None,
):
    include_hidden_closed = normalize_home_scope(scope) == "all" and is_admin_role(user_role)
    scoped_actions = build_scoped_actions_for_home_scope(
        email=email,
        scope=scope,
        directory_db=directory_db,
        db=db,
        user_role=user_role,
    )

    if scoped_actions is None:
        return []

    root_stats = build_root_action_stats_subquery(
        scoped_actions,
        include_hidden_closed=include_hidden_closed,
    )
    direct_child_counts = build_direct_child_count_subquery()
    matching_root_ids_query = build_matching_root_ids_query(
        scoped_actions,
        status=status,
        include_hidden_closed=include_hidden_closed,
    )

    sujets_racine = (
        db.query(
            Sujet,
            func.coalesce(root_stats.c.total_actions, 0).label("total_actions"),
            func.coalesce(root_stats.c.completed_actions, 0).label("completed_actions"),
            func.coalesce(root_stats.c.overdue_actions, 0).label("overdue_actions"),
            func.coalesce(root_stats.c.in_progress_actions, 0).label("in_progress_actions"),
            func.coalesce(root_stats.c.blocked_actions, 0).label("blocked_actions"),
            func.coalesce(direct_child_counts.c.total_sous_sujets, 0).label("total_sous_sujets"),
        )
        .outerjoin(root_stats, root_stats.c.root_id == Sujet.id)
        .outerjoin(direct_child_counts, direct_child_counts.c.root_id == Sujet.id)
        .filter(Sujet.parent_sujet_id.is_(None))
        .filter(Sujet.id.in_(matching_root_ids_query))
        .order_by(Sujet.created_at.desc())
        .all()
    )

    return [
        serialize_root_sujet_row(
            sujet=sujet,
            total_actions=total_actions,
            completed_actions=completed_actions,
            overdue_actions=overdue_actions,
            in_progress_actions=in_progress_actions,
            blocked_actions=blocked_actions,
            total_sous_sujets=total_sous_sujets,
        )
        for (
            sujet,
            total_actions,
            completed_actions,
            overdue_actions,
            in_progress_actions,
            blocked_actions,
            total_sous_sujets,
        ) in sujets_racine
    ]


async def get_sous_sujets_by_sujet_id_service(
    sujet_id: int,
    db: Session,
    email: str | None = None,
    scope: str | None = None,
    directory_db=None,
    status: str | None = None,
    user_role: str | None = None,
):
    include_hidden_closed = normalize_home_scope(scope) == "all" and is_admin_role(user_role)
    if not email:
        sous_sujets = (
            db.query(Sujet)
            .filter(Sujet.parent_sujet_id == sujet_id)
            .order_by(Sujet.created_at.desc())
            .all()
        )
        return sous_sujets

    sujet_tree = build_child_sujet_tree_cte(sujet_id)
    scoped_actions = build_scoped_actions_for_home_scope(
        email=email,
        scope=scope,
        directory_db=directory_db,
        sujet_tree=sujet_tree,
        db=db,
        user_role=user_role,
    )

    if scoped_actions is None:
        return []

    root_stats = build_root_action_stats_subquery(
        scoped_actions,
        include_hidden_closed=include_hidden_closed,
    )
    direct_child_counts = build_direct_child_count_subquery()
    matching_root_ids_query = build_matching_root_ids_query(
        scoped_actions,
        status=status,
        include_hidden_closed=include_hidden_closed,
    )

    sous_sujets = (
        db.query(
            Sujet,
            func.coalesce(root_stats.c.total_actions, 0).label("total_actions"),
            func.coalesce(root_stats.c.completed_actions, 0).label("completed_actions"),
            func.coalesce(root_stats.c.overdue_actions, 0).label("overdue_actions"),
            func.coalesce(root_stats.c.in_progress_actions, 0).label("in_progress_actions"),
            func.coalesce(root_stats.c.blocked_actions, 0).label("blocked_actions"),
            func.coalesce(direct_child_counts.c.total_sous_sujets, 0).label("total_sous_sujets"),
        )
        .outerjoin(root_stats, root_stats.c.root_id == Sujet.id)
        .outerjoin(direct_child_counts, direct_child_counts.c.root_id == Sujet.id)
        .filter(Sujet.parent_sujet_id == sujet_id)
        .filter(Sujet.id.in_(matching_root_ids_query))
        .order_by(Sujet.created_at.desc())
        .all()
    )

    return [
        serialize_root_sujet_row(
            sujet=sujet,
            total_actions=total_actions,
            completed_actions=completed_actions,
            overdue_actions=overdue_actions,
            in_progress_actions=in_progress_actions,
            blocked_actions=blocked_actions,
            total_sous_sujets=total_sous_sujets,
        )
        for (
            sujet,
            total_actions,
            completed_actions,
            overdue_actions,
            in_progress_actions,
            blocked_actions,
            total_sous_sujets,
        ) in sous_sujets
    ]


async def get_team_sujets_racine_service(
    email: str,
    db: Session,
    directory_db,
    status: str | None = None,
    user_role: str | None = None,
):
    include_hidden_closed = False
    scoped_actions = build_scoped_actions_for_home_scope(
        email=email,
        scope="team",
        directory_db=directory_db,
        db=db,
        user_role=user_role,
    )

    if scoped_actions is None:
        return []

    root_stats = build_root_action_stats_subquery(
        scoped_actions,
        include_hidden_closed=include_hidden_closed,
    )
    direct_child_counts = build_direct_child_count_subquery()
    matching_root_ids_query = build_matching_root_ids_query(
        scoped_actions,
        status=status,
        include_hidden_closed=include_hidden_closed,
    )

    sujets_racine = (
        db.query(
            Sujet,
            func.coalesce(root_stats.c.total_actions, 0).label("total_actions"),
            func.coalesce(root_stats.c.completed_actions, 0).label("completed_actions"),
            func.coalesce(root_stats.c.overdue_actions, 0).label("overdue_actions"),
            func.coalesce(root_stats.c.in_progress_actions, 0).label("in_progress_actions"),
            func.coalesce(root_stats.c.blocked_actions, 0).label("blocked_actions"),
            func.coalesce(direct_child_counts.c.total_sous_sujets, 0).label("total_sous_sujets"),
        )
        .outerjoin(root_stats, root_stats.c.root_id == Sujet.id)
        .outerjoin(direct_child_counts, direct_child_counts.c.root_id == Sujet.id)
        .filter(Sujet.parent_sujet_id.is_(None))
        .filter(Sujet.id.in_(matching_root_ids_query))
        .order_by(Sujet.created_at.desc())
        .all()
    )

    return [
        serialize_root_sujet_row(
            sujet=sujet,
            total_actions=total_actions,
            completed_actions=completed_actions,
            overdue_actions=overdue_actions,
            in_progress_actions=in_progress_actions,
            blocked_actions=blocked_actions,
            total_sous_sujets=total_sous_sujets,
        )
        for (
            sujet,
            total_actions,
            completed_actions,
            overdue_actions,
            in_progress_actions,
            blocked_actions,
            total_sous_sujets,
        ) in sujets_racine
    ]
