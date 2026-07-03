import datetime
import json
from collections import defaultdict

from sqlalchemy import func

from app.models.action import Action
from app.models.sujet import Sujet
from app.services.action_event_log_service import log_action_event
from app.services.action_status_logic_service import get_action_active_predicate
from app.services.sujet_service import (
    get_sujet_logical_group_ids,
    normalize_sujet_display_key,
)


SUJET_DUPLICATE_MERGED_EVENT = "sujet_duplicate_merged"
ACTION_RELINKED_FROM_DUPLICATE_SUJET_EVENT = "action_relinked_from_duplicate_sujet"


def _json_safe(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return value


def _parent_logical_key(db, parent_sujet_id: int | None):
    if parent_sujet_id is None:
        return None

    group_ids = get_sujet_logical_group_ids(db, parent_sujet_id)
    return min(group_ids) if group_ids else parent_sujet_id


def _sujet_action_counts(db, sujet_id: int) -> dict:
    total_actions = (
        db.query(func.count(Action.id))
        .filter(Action.sujet_id == sujet_id)
        .scalar()
        or 0
    )
    active_actions = (
        db.query(func.count(Action.id))
        .filter(Action.sujet_id == sujet_id)
        .filter(get_action_active_predicate(Action))
        .scalar()
        or 0
    )
    child_sujets = (
        db.query(func.count(Sujet.id))
        .filter(Sujet.parent_sujet_id == sujet_id)
        .scalar()
        or 0
    )

    return {
        "total_action_count": int(total_actions),
        "active_action_count": int(active_actions),
        "child_sujet_count": int(child_sujets),
    }


def _sujet_to_duplicate_dict(db, sujet: Sujet):
    counts = _sujet_action_counts(db, sujet.id)
    return {
        "id": sujet.id,
        "titre": sujet.titre,
        "normalized_title": normalize_sujet_display_key(sujet.titre),
        "code": sujet.code,
        "parent_sujet_id": sujet.parent_sujet_id,
        "logical_parent_key": _parent_logical_key(db, sujet.parent_sujet_id),
        "inserted_by": sujet.inserted_by,
        "created_at": _json_safe(sujet.created_at),
        **counts,
    }


def get_duplicate_sujet_groups_service(db, limit: int = 100) -> dict:
    sujets = db.query(Sujet).order_by(Sujet.created_at.asc(), Sujet.id.asc()).all()
    buckets = defaultdict(list)

    for sujet in sujets:
        normalized_title = normalize_sujet_display_key(sujet.titre)
        if not normalized_title:
            continue

        key = (
            normalized_title,
            _parent_logical_key(db, sujet.parent_sujet_id),
        )
        buckets[key].append(sujet)

    groups = []
    for (normalized_title, logical_parent_key), grouped_sujets in buckets.items():
        if len(grouped_sujets) < 2:
            continue

        sorted_sujets = sorted(
            grouped_sujets,
            key=lambda item: (
                item.created_at is None,
                item.created_at or datetime.datetime.max.replace(tzinfo=datetime.timezone.utc),
                item.id or 0,
            ),
        )
        keep_sujet = sorted_sujets[0]
        groups.append({
            "group_key": {
                "normalized_title": normalized_title,
                "logical_parent_key": logical_parent_key,
            },
            "count": len(sorted_sujets),
            "sujet_ids": [sujet.id for sujet in sorted_sujets],
            "recommended_keep_id": keep_sujet.id,
            "safe_merge_possible": True,
            "sujets": [
                _sujet_to_duplicate_dict(db, sujet)
                for sujet in sorted_sujets
            ],
        })

    groups.sort(key=lambda group: (-group["count"], group["sujet_ids"][0]))
    return {
        "duplicate_group_count": len(groups[:limit]),
        "groups": groups[:limit],
    }


def _build_merge_details(
    keep_sujet_id: int,
    merge_sujet_ids: list[int],
    moved_action_ids: list[int],
    moved_child_sujet_ids: list[int],
):
    return json.dumps(
        {
            "keep_sujet_id": keep_sujet_id,
            "merge_sujet_ids": merge_sujet_ids,
            "moved_action_ids": moved_action_ids,
            "moved_child_sujet_ids": moved_child_sujet_ids,
        },
        default=_json_safe,
        ensure_ascii=False,
    )


def merge_duplicate_sujets_service(
    db,
    *,
    dry_run: bool,
    keep_sujet_id: int,
    merge_sujet_ids: list[int],
    current_user=None,
) -> dict:
    unique_merge_ids = [
        sujet_id
        for sujet_id in dict.fromkeys(merge_sujet_ids or [])
        if sujet_id != keep_sujet_id
    ]
    keep_sujet = db.query(Sujet).filter(Sujet.id == keep_sujet_id).first()

    if not keep_sujet:
        return {
            "dry_run": dry_run,
            "merged": False,
            "message": "Keep sujet not found.",
            "keep_sujet_id": keep_sujet_id,
            "merge_sujet_ids": unique_merge_ids,
            "actions_to_move": [],
            "child_sujets_to_move": [],
            "warnings": [{"type": "keep_sujet_not_found"}],
        }

    merge_sujets = (
        db.query(Sujet)
        .filter(Sujet.id.in_(unique_merge_ids))
        .order_by(Sujet.id.asc())
        .all()
    )
    found_merge_ids = {sujet.id for sujet in merge_sujets}
    missing_ids = [
        sujet_id
        for sujet_id in unique_merge_ids
        if sujet_id not in found_merge_ids
    ]
    actions_to_move = (
        db.query(Action)
        .filter(Action.sujet_id.in_(found_merge_ids))
        .order_by(Action.id.asc())
        .all()
        if found_merge_ids
        else []
    )
    child_sujets_to_move = (
        db.query(Sujet)
        .filter(Sujet.parent_sujet_id.in_(found_merge_ids))
        .filter(Sujet.id != keep_sujet_id)
        .order_by(Sujet.id.asc())
        .all()
        if found_merge_ids
        else []
    )
    moved_action_ids = [action.id for action in actions_to_move]
    moved_child_sujet_ids = [sujet.id for sujet in child_sujets_to_move]
    action_move_plan = [
        {
            "id": action.id,
            "titre": action.titre,
            "from_sujet_id": action.sujet_id,
            "to_sujet_id": keep_sujet_id,
        }
        for action in actions_to_move
    ]
    child_sujet_move_plan = [
        {
            "id": sujet.id,
            "titre": sujet.titre,
            "from_parent_sujet_id": sujet.parent_sujet_id,
            "to_parent_sujet_id": keep_sujet_id,
        }
        for sujet in child_sujets_to_move
    ]
    actor = getattr(current_user, "email", None) or "admin"

    if not dry_run:
        details = _build_merge_details(
            keep_sujet_id,
            unique_merge_ids,
            moved_action_ids,
            moved_child_sujet_ids,
        )

        for action in actions_to_move:
            old_sujet_id = action.sujet_id
            action.sujet_id = keep_sujet_id
            action.updated_at = datetime.datetime.now(datetime.timezone.utc)
            log_action_event(
                db=db,
                action_id=action.id,
                event_type=ACTION_RELINKED_FROM_DUPLICATE_SUJET_EVENT,
                old_value=str(old_sujet_id),
                new_value=str(keep_sujet_id),
                details=details,
                created_by=actor,
            )
            log_action_event(
                db=db,
                action_id=action.id,
                event_type=SUJET_DUPLICATE_MERGED_EVENT,
                old_value=str(old_sujet_id),
                new_value=str(keep_sujet_id),
                details=details,
                created_by=actor,
            )

        for child_sujet in child_sujets_to_move:
            child_sujet.parent_sujet_id = keep_sujet_id
            child_sujet.updated_at = datetime.datetime.now(datetime.timezone.utc)

        db.commit()

    return {
        "dry_run": dry_run,
        "merged": bool(actions_to_move or child_sujets_to_move),
        "keep_sujet": _sujet_to_duplicate_dict(db, keep_sujet),
        "merge_sujets": [
            _sujet_to_duplicate_dict(db, sujet)
            for sujet in merge_sujets
        ],
        "missing_sujet_ids": missing_ids,
        "actions_to_move": action_move_plan,
        "child_sujets_to_move": child_sujet_move_plan,
        "message": (
            "Dry run complete. No sujets or actions were changed."
            if dry_run
            else "Duplicate sujets merged by relinking actions and child sujets."
        ),
        "hard_deleted_sujets": [],
        "soft_deleted_sujets": [],
        "note": "Sujet has no soft-delete field in this schema; duplicate sujet rows are left empty and display-level merging hides logical duplicates.",
    }
