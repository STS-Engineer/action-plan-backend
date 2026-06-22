import logging

from sqlalchemy import func

from app.models.action import Action
from app.services.action_status_logic_service import get_action_active_predicate


logger = logging.getLogger(__name__)


def normalize_action_duplicate_title(value: str | None) -> str:
    return str(value or "").strip().lower()


def find_duplicate_action(
    db,
    sujet_id: int,
    parent_action_id: int | None,
    titre: str | None,
    exclude_action_id: int | None = None,
):
    normalized_title = normalize_action_duplicate_title(titre)

    if not normalized_title:
        return None

    query = (
        db.query(Action)
        .filter(Action.sujet_id == sujet_id)
        .filter(func.lower(func.trim(Action.titre)) == normalized_title)
        .filter(get_action_active_predicate(Action))
    )

    if parent_action_id is None:
        query = query.filter(Action.parent_action_id.is_(None))
    else:
        query = query.filter(Action.parent_action_id == parent_action_id)

    if exclude_action_id is not None:
        query = query.filter(Action.id != exclude_action_id)

    return query.order_by(Action.id.asc()).first()


def update_missing_action_fields(action: Action, values: dict) -> list[str]:
    updated_fields: list[str] = []

    for field_name, new_value in values.items():
        if field_name in {"id", "sujet_id", "parent_action_id", "depth"}:
            continue

        if new_value is None:
            continue

        current_value = getattr(action, field_name, None)

        if current_value not in [None, ""]:
            continue

        setattr(action, field_name, new_value)
        updated_fields.append(field_name)

    return updated_fields


def find_or_update_duplicate_action(
    db,
    sujet_id: int,
    parent_action_id: int | None,
    titre: str,
    values: dict,
):
    existing_action = find_duplicate_action(
        db,
        sujet_id=sujet_id,
        parent_action_id=parent_action_id,
        titre=titre,
    )

    if not existing_action:
        return None, []

    updated_fields = update_missing_action_fields(existing_action, values)
    logger.info(
        (
            "Duplicate action prevented sujet_id=%s parent_action_id=%s "
            "normalized_titre=%s existing_action_id=%s updated_missing_fields=%s"
        ),
        sujet_id,
        parent_action_id,
        normalize_action_duplicate_title(titre),
        existing_action.id,
        updated_fields,
    )

    return existing_action, updated_fields


def get_duplicate_action_groups_service(db, limit: int = 100) -> dict:
    normalized_title = func.lower(func.trim(Action.titre)).label("normalized_titre")
    parent_key = func.coalesce(Action.parent_action_id, 0).label("parent_action_key")

    groups = (
        db.query(
            Action.sujet_id.label("sujet_id"),
            Action.parent_action_id.label("parent_action_id"),
            parent_key,
            normalized_title,
            func.count(Action.id).label("count"),
        )
        .filter(get_action_active_predicate(Action))
        .group_by(
            Action.sujet_id,
            Action.parent_action_id,
            parent_key,
            normalized_title,
        )
        .having(func.count(Action.id) > 1)
        .order_by(func.count(Action.id).desc(), Action.sujet_id.asc())
        .limit(limit)
        .all()
    )

    result_groups = []

    for group in groups:
        query = (
            db.query(Action)
            .filter(Action.sujet_id == group.sujet_id)
            .filter(func.lower(func.trim(Action.titre)) == group.normalized_titre)
            .filter(get_action_active_predicate(Action))
        )

        if group.parent_action_id is None:
            query = query.filter(Action.parent_action_id.is_(None))
        else:
            query = query.filter(Action.parent_action_id == group.parent_action_id)

        actions = query.order_by(Action.id.asc()).all()

        result_groups.append({
            "sujet_id": group.sujet_id,
            "parent_action_id": group.parent_action_id,
            "normalized_titre": group.normalized_titre,
            "count": group.count,
            "actions": [
                {
                    "id": action.id,
                    "titre": action.titre,
                    "status": action.status,
                    "responsable": action.responsable,
                    "email_responsable": action.email_responsable,
                    "created_at": action.created_at,
                }
                for action in actions
            ],
        })

    return {
        "duplicate_group_count": len(result_groups),
        "groups": result_groups,
    }
