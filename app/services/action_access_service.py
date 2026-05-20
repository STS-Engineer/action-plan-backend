import os

from app.services.directory_service import get_all_underlings


def normalize_access_email(email: str | None) -> str | None:
    if not email:
        return None

    normalized_email = email.strip().lower()
    return normalized_email or None


def is_enabled(value: str | None, default: bool = True) -> bool:
    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "on"}


def can_access_global_actions(user_role: str | None) -> bool:
    normalized_role = str(user_role or "").strip().lower()
    return normalized_role in {"admin", "global", "superadmin", "super_admin"}


def can_access_action(
    logged_email: str | None,
    action,
    directory_db=None,
    user_role: str | None = None,
    created_by_email: str | None = None,
):
    logged_email = normalize_access_email(logged_email)
    action_email = normalize_access_email(getattr(action, "email_responsable", None))
    requester_email = normalize_access_email(getattr(action, "email_demandeur", None))
    created_by_email = normalize_access_email(created_by_email)

    if bool(getattr(action, "is_deleted", False)):
        return {
            "allowed": False,
            "scope": None,
            "reason": "action_deleted",
        }

    if not logged_email:
        return {
            "allowed": False,
            "scope": None,
            "reason": "missing_logged_user",
        }

    if can_access_global_actions(user_role):
        return {
            "allowed": True,
            "scope": "global",
            "reason": "global_role",
        }

    if action_email and action_email == logged_email:
        return {
            "allowed": True,
            "scope": "my",
            "reason": "owner",
        }

    requester_visibility_enabled = is_enabled(
        os.getenv("ACTION_REQUESTER_VISIBILITY_ENABLED"),
        default=True,
    )

    if requester_visibility_enabled and requester_email and requester_email == logged_email:
        return {
            "allowed": True,
            "scope": "requester",
            "reason": "requester",
        }

    created_by_visibility_enabled = is_enabled(
        os.getenv("ACTION_CREATED_BY_VISIBILITY_ENABLED"),
        default=True,
    )

    if created_by_visibility_enabled and created_by_email and created_by_email == logged_email:
        return {
            "allowed": True,
            "scope": "created_by",
            "reason": "created_by",
        }

    if not action_email:
        return {
            "allowed": False,
            "scope": None,
            "reason": "missing_action_owner",
        }

    if directory_db is None:
        return {
            "allowed": False,
            "scope": None,
            "reason": "missing_directory_scope",
        }

    underlings = get_all_underlings(directory_db, logged_email)
    underling_emails = {
        normalized_email
        for normalized_email in (
            normalize_access_email(member.email)
            for member in underlings
        )
        if normalized_email
    }

    if action_email in underling_emails:
        return {
            "allowed": True,
            "scope": "team",
            "reason": "team_underling",
        }

    return {
        "allowed": False,
        "scope": None,
        "reason": "not_visible",
    }


def action_access_summary(action):
    return {
        "id": action.id,
        "titre": action.titre,
        "status": action.status,
        "email_responsable": action.email_responsable,
    }
