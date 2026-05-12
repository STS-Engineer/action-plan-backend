from app.models.sujet import Sujet
from app.models.action import Action
from sqlalchemy import case, func, select
from sqlalchemy.orm import Session, aliased

from app.services.action_status_logic_service import (
    get_action_closed_visible_predicate,
    get_action_home_bucket_predicate,
    get_action_in_progress_predicate,
    get_action_overdue_predicate,
    get_action_visible_from_home_predicate,
    get_normalized_action_status_expression,
)


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
    if not email:
        return None

    normalized_email = email.strip().lower()
    return normalized_email or None


def normalize_scope_emails(emails: list[str] | None) -> list[str]:
    if not emails:
        return []

    normalized_emails = []

    for email in emails:
        normalized_email = normalize_scope_email(email)

        if normalized_email and normalized_email not in normalized_emails:
            normalized_emails.append(normalized_email)

    return normalized_emails


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


def build_scope_predicate(action_like=Action, email: str | None = None, emails: list[str] | None = None):
    email_col = func.lower(func.coalesce(action_like.email_responsable, ""))

    if email:
        return email_col == email

    if emails:
        return email_col.in_(emails)

    return None


def build_visible_scoped_actions_subquery(
    email: str | None = None,
    emails: list[str] | None = None,
):
    sujet_tree = build_sujet_tree_cte()

    query = (
        select(
            sujet_tree.c.root_id.label("root_id"),
            Action.id.label("action_id"),
            Action.status.label("status"),
            Action.due_date.label("due_date"),
            Action.closed_date.label("closed_date"),
        )
        .select_from(sujet_tree)
        .join(Action, Action.sujet_id == sujet_tree.c.sujet_id)
        .where(get_action_visible_from_home_predicate(Action))
    )

    scope_predicate = build_scope_predicate(Action, email=email, emails=emails)

    if scope_predicate is not None:
        query = query.where(scope_predicate)

    return query.subquery()


def build_root_action_stats_subquery(scoped_actions):
    return (
        select(
            scoped_actions.c.root_id.label("root_id"),
            func.count(func.distinct(scoped_actions.c.action_id)).label("total_actions"),
            func.count(
                func.distinct(
                    case(
                        (get_action_closed_visible_predicate(scoped_actions), scoped_actions.c.action_id),
                    )
                )
            ).label("completed_actions"),
            func.count(
                func.distinct(
                    case(
                        (get_action_overdue_predicate(scoped_actions), scoped_actions.c.action_id),
                    )
                )
            ).label("overdue_actions"),
            func.count(
                func.distinct(
                    case(
                        (get_action_in_progress_predicate(scoped_actions), scoped_actions.c.action_id),
                    )
                )
            ).label("in_progress_actions"),
        )
        .select_from(scoped_actions)
        .group_by(scoped_actions.c.root_id)
        .subquery()
    )


def build_matching_root_ids_query(scoped_actions, status: str | None = None):
    query = select(scoped_actions.c.root_id).select_from(scoped_actions)

    if status:
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
        .where(Sujet.parent_sujet_id.is_(None))
        .group_by(Sujet.id)
        .subquery()
    )


def serialize_root_sujet_row(
    sujet,
    total_actions,
    completed_actions,
    overdue_actions,
    in_progress_actions,
    total_sous_sujets,
):
    return {
        **sujet.__dict__,
        "total_actions": total_actions or 0,
        "completed_actions": completed_actions or 0,
        "overdue_actions": overdue_actions or 0,
        "in_progress_actions": in_progress_actions or 0,
        "total_sous_sujets": total_sous_sujets or 0,
    }


def get_scope_emails_for_team(email: str | None, directory_db) -> list[str]:
    from app.services.directory_service import get_all_underlings

    normalized_email = normalize_scope_email(email)

    if not normalized_email:
        return []

    underlings = get_all_underlings(directory_db, normalized_email)

    return normalize_scope_emails([
        member.email
        for member in underlings
        if member.email
    ])


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
            "overdue_actions": overdue_actions,
        }
        for sujet, total_actions, completed_actions, overdue_actions in sujets
    ]


async def get_home_summary_service(
    email: str,
    scope: str,
    db: Session,
    directory_db,
):
    normalized_email = normalize_scope_email(email)

    if not normalized_email:
        return dict(ZERO_HOME_SUMMARY)

    if scope == "team":
        scope_emails = get_scope_emails_for_team(normalized_email, directory_db)

        if not scope_emails:
            return dict(ZERO_HOME_SUMMARY)

        scoped_actions = build_visible_scoped_actions_subquery(emails=scope_emails)
    else:
        scoped_actions = build_visible_scoped_actions_subquery(email=normalized_email)

    normalized_status = get_normalized_action_status_expression(scoped_actions)

    summary = (
        db.query(
            func.count(func.distinct(scoped_actions.c.root_id)).label("total_sujets"),
            func.count(func.distinct(scoped_actions.c.action_id)).label("total_actions"),
            func.count(
                func.distinct(
                    case(
                        (get_action_closed_visible_predicate(scoped_actions), scoped_actions.c.action_id),
                    )
                )
            ).label("actions_completed"),
            func.count(
                func.distinct(
                    case(
                        (get_action_overdue_predicate(scoped_actions), scoped_actions.c.action_id),
                    )
                )
            ).label("actions_overdue"),
            func.count(
                func.distinct(
                    case(
                        (get_action_in_progress_predicate(scoped_actions), scoped_actions.c.action_id),
                    )
                )
            ).label("actions_in_progress"),
            func.count(
                func.distinct(
                    case((normalized_status == "open", scoped_actions.c.action_id))
                )
            ).label("actions_open"),
            func.count(
                func.distinct(
                    case((normalized_status == "blocked", scoped_actions.c.action_id))
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
):
    normalized_email = normalize_scope_email(email)
    scoped_actions = build_visible_scoped_actions_subquery(email=normalized_email)
    root_stats = build_root_action_stats_subquery(scoped_actions)
    direct_child_counts = build_direct_child_count_subquery()
    matching_root_ids_query = build_matching_root_ids_query(scoped_actions, status=status)

    sujets_racine = (
        db.query(
            Sujet,
            func.coalesce(root_stats.c.total_actions, 0).label("total_actions"),
            func.coalesce(root_stats.c.completed_actions, 0).label("completed_actions"),
            func.coalesce(root_stats.c.overdue_actions, 0).label("overdue_actions"),
            func.coalesce(root_stats.c.in_progress_actions, 0).label("in_progress_actions"),
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
            total_sous_sujets=total_sous_sujets,
        )
        for (
            sujet,
            total_actions,
            completed_actions,
            overdue_actions,
            in_progress_actions,
            total_sous_sujets,
        ) in sujets_racine
    ]


async def get_sous_sujets_by_sujet_id_service(sujet_id: int, db: Session):
    sous_sujets = (
        db.query(Sujet)
        .filter(Sujet.parent_sujet_id == sujet_id)
        .order_by(Sujet.created_at.desc())
        .all()
    )
    return sous_sujets


async def get_team_sujets_racine_service(
    email: str,
    db: Session,
    directory_db,
    status: str | None = None,
):
    scope_emails = get_scope_emails_for_team(email, directory_db)

    if not scope_emails:
        return []

    scoped_actions = build_visible_scoped_actions_subquery(emails=scope_emails)
    root_stats = build_root_action_stats_subquery(scoped_actions)
    direct_child_counts = build_direct_child_count_subquery()
    matching_root_ids_query = build_matching_root_ids_query(scoped_actions, status=status)

    sujets_racine = (
        db.query(
            Sujet,
            func.coalesce(root_stats.c.total_actions, 0).label("total_actions"),
            func.coalesce(root_stats.c.completed_actions, 0).label("completed_actions"),
            func.coalesce(root_stats.c.overdue_actions, 0).label("overdue_actions"),
            func.coalesce(root_stats.c.in_progress_actions, 0).label("in_progress_actions"),
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
            total_sous_sujets=total_sous_sujets,
        )
        for (
            sujet,
            total_actions,
            completed_actions,
            overdue_actions,
            in_progress_actions,
            total_sous_sujets,
        ) in sujets_racine
    ]
