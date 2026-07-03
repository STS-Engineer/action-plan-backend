import datetime
import json
import logging
import re
import unicodedata
from collections import defaultdict

from sqlalchemy import func

from app.models.action import Action
from app.models.sujet import Sujet
from app.services.action_event_log_service import log_action_event
from app.services.action_status_logic_service import (
    get_action_active_predicate,
    get_normalized_action_status_expression,
    normalize_action_status,
)
from app.services.team_scope_service import get_direct_report_emails_for_team_scope


logger = logging.getLogger(__name__)

TITLE_COMPARE_LENGTH = 120
DUPLICATE_BLOCKED_EVENT = "action_duplicate_creation_blocked"
DUPLICATE_SOFT_DELETED_EVENT = "action_duplicate_soft_deleted"
DUPLICATE_KEPT_EVENT = "action_duplicate_kept"

_CONSOLIDATED_PREFIX_RE = re.compile(
    r"^(consolidated\s+duplicate\s+action[\s.:\-;]*)+",
    flags=re.IGNORECASE,
)
_PUNCTUATION_RE = re.compile(r"[^\w\s]", flags=re.UNICODE)
_SPACE_RE = re.compile(r"\s+")


def _normalize_email(value: str | None) -> str | None:
    normalized = (value or "").strip().lower()
    return normalized or None


def normalize_action_duplicate_title(value: str | None) -> str:
    text = str(value or "").strip().lower()
    text = "".join(
        character
        for character in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(character)
    )
    text = _CONSOLIDATED_PREFIX_RE.sub("", text).strip()
    text = _PUNCTUATION_RE.sub(" ", text)
    text = _SPACE_RE.sub(" ", text).strip()
    return text[:TITLE_COMPARE_LENGTH]


def _is_closed_status(status: str | None) -> bool:
    return normalize_action_status(status) == "closed"


def _is_closed_action(action: Action) -> bool:
    return _is_closed_status(action.status) or action.closed_date is not None


def _json_safe(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return value


def _action_sort_key(action: Action):
    return (
        action.created_at is None,
        action.created_at or datetime.datetime.max.replace(tzinfo=datetime.timezone.utc),
        action.id or 0,
    )


def _get_sujet_maps(db, sujet_ids: set[int | None]):
    pending_ids = {sujet_id for sujet_id in sujet_ids if sujet_id is not None}
    sujets_by_id = {}

    while pending_ids:
        sujets = db.query(Sujet).filter(Sujet.id.in_(pending_ids)).all()
        pending_ids = set()

        for sujet in sujets:
            if sujet.id in sujets_by_id:
                continue

            sujets_by_id[sujet.id] = sujet

            if sujet.parent_sujet_id and sujet.parent_sujet_id not in sujets_by_id:
                pending_ids.add(sujet.parent_sujet_id)

    info_by_sujet_id = {}

    for sujet_id in sujet_ids:
        path = []
        visited_ids = set()
        current = sujets_by_id.get(sujet_id)

        while current and current.id not in visited_ids:
            path.append(current)
            visited_ids.add(current.id)
            current = sujets_by_id.get(current.parent_sujet_id)

        path.reverse()
        root = path[0] if path else None
        nearest = path[-1] if path else None
        info_by_sujet_id[sujet_id] = {
            "root_sujet_id": root.id if root else sujet_id,
            "root_sujet_title": root.titre if root else None,
            "sujet_title": nearest.titre if nearest else None,
            "topic_path": " > ".join(
                sujet.titre for sujet in path if sujet.titre
            ) or None,
        }

    return info_by_sujet_id


def _action_to_duplicate_dict(action: Action, sujet_info: dict | None = None):
    sujet_info = sujet_info or {}
    return {
        "id": action.id,
        "titre": action.titre,
        "normalized_title": normalize_action_duplicate_title(action.titre),
        "description": action.description,
        "status": action.status,
        "canonical_status": normalize_action_status(action.status),
        "responsable": action.responsable,
        "email_responsable": _normalize_email(action.email_responsable),
        "demandeur": action.demandeur,
        "email_demandeur": _normalize_email(action.email_demandeur),
        "due_date": _json_safe(action.due_date),
        "sujet_id": action.sujet_id,
        "parent_action_id": action.parent_action_id,
        "root_sujet_id": sujet_info.get("root_sujet_id"),
        "root_sujet_title": sujet_info.get("root_sujet_title"),
        "sujet_title": sujet_info.get("sujet_title"),
        "topic_path": sujet_info.get("topic_path"),
        "created_at": _json_safe(action.created_at),
        "is_deleted": bool(action.is_deleted),
    }


def _candidate_query(
    db,
    include_deleted: bool = False,
    include_closed: bool = False,
):
    query = db.query(Action)

    if not include_closed:
        status_expr = get_normalized_action_status_expression(Action)
        query = query.filter(status_expr != "closed").filter(Action.closed_date.is_(None))

    if not include_deleted:
        query = query.filter(get_action_active_predicate(Action))

    return query


def find_duplicate_action(
    db,
    sujet_id: int,
    parent_action_id: int | None,
    titre: str | None,
    exclude_action_id: int | None = None,
    email_responsable: str | None = None,
    email_demandeur: str | None = None,
):
    normalized_title = normalize_action_duplicate_title(titre)

    if not normalized_title:
        return None

    query = _candidate_query(
        db,
        include_deleted=False,
        include_closed=False,
    ).filter(Action.sujet_id == sujet_id)

    if parent_action_id is None:
        query = query.filter(Action.parent_action_id.is_(None))
    else:
        query = query.filter(Action.parent_action_id == parent_action_id)

    normalized_responsible = _normalize_email(email_responsable)
    if normalized_responsible:
        query = query.filter(func.lower(func.coalesce(Action.email_responsable, "")) == normalized_responsible)

    normalized_requester = _normalize_email(email_demandeur)
    if normalized_requester:
        query = query.filter(func.lower(func.coalesce(Action.email_demandeur, "")) == normalized_requester)

    if exclude_action_id is not None:
        query = query.filter(Action.id != exclude_action_id)

    for action in query.order_by(Action.created_at.asc().nullsfirst(), Action.id.asc()).all():
        if normalize_action_duplicate_title(action.titre) == normalized_title:
            return action

    return None


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
        email_responsable=values.get("email_responsable"),
        email_demandeur=values.get("email_demandeur"),
    )

    if not existing_action:
        return None, []

    updated_fields = update_missing_action_fields(existing_action, values)
    details = json.dumps(
        {
            "sujet_id": sujet_id,
            "parent_action_id": parent_action_id,
            "normalized_titre": normalize_action_duplicate_title(titre),
            "existing_action_id": existing_action.id,
            "updated_missing_fields": updated_fields,
        },
        default=_json_safe,
        ensure_ascii=False,
    )
    log_action_event(
        db=db,
        action_id=existing_action.id,
        event_type=DUPLICATE_BLOCKED_EVENT,
        old_value=None,
        new_value=str(existing_action.id),
        details=details,
        created_by=values.get("email_demandeur") or values.get("email_responsable") or "system",
    )
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


def _scope_email_values(email: str | None, scope: str, organisation_db) -> set[str]:
    normalized_email = _normalize_email(email)

    if not normalized_email:
        return set()

    if scope != "team":
        return {normalized_email}

    if organisation_db is None:
        return set()

    return {
        direct_email
        for direct_email in get_direct_report_emails_for_team_scope(
            organisation_db,
            normalized_email,
        )
    }


def _apply_scope_filter(query, email: str | None, scope: str, organisation_db):
    normalized_scope = (scope or "all").strip().lower()
    normalized_email = _normalize_email(email)
    email_responsable = func.lower(func.coalesce(Action.email_responsable, ""))
    email_demandeur = func.lower(func.coalesce(Action.email_demandeur, ""))

    if normalized_scope == "responsible":
        if not normalized_email:
            return query
        return query.filter(email_responsable == normalized_email)

    if normalized_scope == "requester":
        if not normalized_email:
            return query
        return query.filter(email_demandeur == normalized_email)

    if normalized_scope == "team":
        team_emails = _scope_email_values(email, "team", organisation_db)
        if not team_emails:
            return query.filter(False)
        return query.filter(email_responsable.in_(team_emails))

    return query


def get_duplicate_action_groups_service(
    db,
    email: str | None = None,
    scope: str | None = None,
    include_deleted: bool = False,
    include_closed: bool = False,
    directory_db=None,
    organisation_db=None,
    limit: int = 100,
) -> dict:
    normalized_scope = (scope or "all").strip().lower()
    if normalized_scope not in {"responsible", "requester", "team", "all"}:
        normalized_scope = "all"

    actions = (
        _apply_scope_filter(
            _candidate_query(
                db,
                include_deleted=include_deleted,
                include_closed=include_closed,
            ),
            email=email,
            scope=normalized_scope,
            organisation_db=organisation_db,
        )
        .order_by(Action.sujet_id.asc(), Action.created_at.asc().nullsfirst(), Action.id.asc())
        .all()
    )
    sujet_info_by_id = _get_sujet_maps(db, {action.sujet_id for action in actions})
    buckets: dict[tuple, list[Action]] = defaultdict(list)

    for action in actions:
        normalized_title = normalize_action_duplicate_title(action.titre)
        if not normalized_title:
            continue

        sujet_info = sujet_info_by_id.get(action.sujet_id, {})
        root_sujet_key = sujet_info.get("root_sujet_id") or action.sujet_id
        due_date_key = action.due_date.isoformat() if action.due_date else None
        key = (
            normalized_title,
            root_sujet_key,
            action.parent_action_id or 0,
            due_date_key,
            _normalize_email(action.email_responsable) or "",
            _normalize_email(action.email_demandeur) or "",
        )
        buckets[key].append(action)

    result_groups = []
    seen_action_sets = set()

    for key, grouped_actions in buckets.items():
        if len(grouped_actions) < 2:
            continue

        sorted_actions = sorted(grouped_actions, key=_action_sort_key)
        action_id_tuple = tuple(action.id for action in sorted_actions)
        if action_id_tuple in seen_action_sets:
            continue
        seen_action_sets.add(action_id_tuple)

        (
            normalized_title,
            root_sujet_id,
            parent_action_id,
            due_date_key,
            responsible_email,
            requester_email,
        ) = key
        keep_oldest = sorted_actions[0]
        result_groups.append({
            "group_key": {
                "normalized_title": normalized_title,
                "root_sujet_id": root_sujet_id,
                "parent_action_id": None if parent_action_id == 0 else parent_action_id,
                "due_date": due_date_key,
                "email_responsable": responsible_email or None,
                "email_demandeur": requester_email or None,
            },
            "count": len(sorted_actions),
            "action_ids": [action.id for action in sorted_actions],
            "recommendation": "keep_oldest",
            "resolution_strategy": "soft_delete_duplicates_keep_oldest",
            "recommended_keep_action_id": keep_oldest.id,
            "actions": [
                _action_to_duplicate_dict(
                    action,
                    sujet_info_by_id.get(action.sujet_id),
                )
                for action in sorted_actions
            ],
        })

    result_groups.sort(key=lambda group: (-group["count"], group["action_ids"][0]))
    result_groups = result_groups[:limit]

    return {
        "scope": normalized_scope,
        "email": _normalize_email(email),
        "include_deleted": include_deleted,
        "include_closed": include_closed,
        "duplicate_group_count": len(result_groups),
        "groups": result_groups,
    }


def _build_duplicate_resolution_details(
    kept_action_id: int,
    group_action_ids: list[int],
    keep: str = "oldest",
):
    return json.dumps(
        {
            "kept_action_id": kept_action_id,
            "group_action_ids": group_action_ids,
            "strategy": f"soft_delete_duplicates_keep_{keep}",
        },
        default=_json_safe,
        ensure_ascii=False,
    )


def resolve_duplicate_actions_service(
    db,
    action_ids: list[int],
    dry_run: bool = True,
    strategy: str = "soft_delete_duplicates_keep_oldest",
    current_user=None,
    keep: str = "oldest",
    include_closed: bool = False,
) -> dict:
    normalized_keep = (keep or "oldest").strip().lower()
    if strategy == "soft_delete_duplicates_keep_newest" and keep == "oldest":
        normalized_keep = "newest"
    if normalized_keep not in {"oldest", "newest"}:
        raise ValueError("Unsupported keep value.")

    if strategy not in {"soft_delete_duplicates_keep_oldest", "soft_delete_duplicates_keep_newest"}:
        raise ValueError("Unsupported duplicate resolution strategy.")

    unique_action_ids = list(dict.fromkeys(action_ids or []))
    if len(unique_action_ids) < 2:
        return {
            "dry_run": dry_run,
            "strategy": strategy,
            "changed": False,
            "message": "At least two action_ids are required.",
            "kept_action": None,
            "actions_to_soft_delete": [],
            "skipped": [],
        }

    actions = (
        db.query(Action)
        .filter(Action.id.in_(unique_action_ids))
        .order_by(Action.created_at.asc().nullsfirst(), Action.id.asc())
        .all()
    )
    found_by_id = {action.id: action for action in actions}
    missing_ids = [action_id for action_id in unique_action_ids if action_id not in found_by_id]
    skipped = [
        {"id": action_id, "reason": "not_found"}
        for action_id in missing_ids
    ]

    candidate_actions = []
    for action in actions:
        if bool(action.is_deleted):
            skipped.append({"id": action.id, "reason": "already_deleted"})
            continue

        if not include_closed and _is_closed_action(action):
            skipped.append({"id": action.id, "reason": "closed_actions_are_not_deleted_by_default"})
            continue

        candidate_actions.append(action)

    if len(candidate_actions) < 2:
        return {
            "dry_run": dry_run,
            "strategy": strategy,
            "changed": False,
            "message": "Fewer than two active non-closed actions can be resolved.",
            "kept_action": _action_to_duplicate_dict(candidate_actions[0]) if candidate_actions else None,
            "actions_to_soft_delete": [],
            "skipped": skipped,
        }

    sorted_actions = sorted(
        candidate_actions,
        key=_action_sort_key,
        reverse=normalized_keep == "newest",
    )
    kept_action = sorted_actions[0]
    duplicate_actions = sorted_actions[1:]
    group_action_ids = [action.id for action in sorted_actions]
    actor_email = _normalize_email(getattr(current_user, "email", None)) or "admin"
    now = datetime.datetime.now(datetime.timezone.utc)

    if not dry_run:
        details = _build_duplicate_resolution_details(
            kept_action.id,
            group_action_ids,
            keep=normalized_keep,
        )
        log_action_event(
            db=db,
            action_id=kept_action.id,
            event_type=DUPLICATE_KEPT_EVENT,
            old_value="active",
            new_value="kept",
            details=details,
            created_by=actor_email,
        )

        for action in duplicate_actions:
            action.is_deleted = True
            action.deleted_at = now
            action.deleted_by = actor_email
            action.updated_at = now
            log_action_event(
                db=db,
                action_id=action.id,
                event_type=DUPLICATE_SOFT_DELETED_EVENT,
                old_value="active",
                new_value="deleted",
                details=details,
                created_by=actor_email,
            )

        db.commit()

    sujet_info_by_id = _get_sujet_maps(db, {action.sujet_id for action in sorted_actions})
    return {
        "dry_run": dry_run,
        "strategy": f"soft_delete_duplicates_keep_{normalized_keep}",
        "keep": normalized_keep,
        "include_closed": include_closed,
        "changed": bool(duplicate_actions),
        "kept_action": _action_to_duplicate_dict(
            kept_action,
            sujet_info_by_id.get(kept_action.sujet_id),
        ),
        "actions_to_soft_delete": [
            _action_to_duplicate_dict(action, sujet_info_by_id.get(action.sujet_id))
            for action in duplicate_actions
        ],
        "skipped": skipped,
        "message": (
            "Dry run complete. No actions were changed."
            if dry_run
            else "Duplicate actions soft-deleted."
        ),
    }
