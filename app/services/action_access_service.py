from app.services.directory_service import get_all_underlings


def normalize_access_email(email: str | None) -> str | None:
    if not email:
        return None

    normalized_email = email.strip().lower()
    return normalized_email or None


def can_access_action(logged_email: str | None, action, directory_db=None):
    logged_email = normalize_access_email(logged_email)
    action_email = normalize_access_email(getattr(action, "email_responsable", None))

    if not logged_email or not action_email:
        return {
            "allowed": False,
            "scope": None,
        }

    if action_email == logged_email:
        return {
            "allowed": True,
            "scope": "my",
        }

    if directory_db is None:
        return {
            "allowed": False,
            "scope": None,
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
        }

    return {
        "allowed": False,
        "scope": None,
    }


def action_access_summary(action):
    return {
        "id": action.id,
        "titre": action.titre,
        "status": action.status,
        "email_responsable": action.email_responsable,
    }
