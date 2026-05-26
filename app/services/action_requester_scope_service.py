from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session

from app.models.user import User


def normalize_requester_value(value: str | None) -> str | None:
    if value is None:
        return None

    normalized = " ".join(str(value).strip().lower().split())
    return normalized or None


def unique_requester_values(values: list[str | None] | tuple[str | None, ...]) -> list[str]:
    normalized_values: list[str] = []

    for value in values:
        normalized = normalize_requester_value(value)

        if normalized and normalized not in normalized_values:
            normalized_values.append(normalized)

    return normalized_values


def get_logged_user_requester_aliases(
    db: Session | None,
    email: str | None,
    directory_db=None,
) -> list[str]:
    normalized_email = normalize_requester_value(email)

    if not normalized_email:
        return []

    aliases = [normalized_email]

    if db is not None:
        user = (
            db.query(User.full_name)
            .filter(func.lower(User.email) == normalized_email)
            .first()
        )
        full_name = normalize_requester_value(user.full_name if user else None)

        if full_name and full_name not in aliases:
            aliases.append(full_name)

    if directory_db is not None:
        try:
            from app.services.directory_service import get_member_by_email

            member = get_member_by_email(directory_db, normalized_email)
        except Exception:
            member = None

        if member:
            for value in [
                getattr(member, "display_name", None),
                " ".join(
                    part
                    for part in [
                        getattr(member, "first_name", None),
                        getattr(member, "last_name", None),
                    ]
                    if part
                ),
            ]:
                normalized_value = normalize_requester_value(value)

                if normalized_value and normalized_value not in aliases:
                    aliases.append(normalized_value)

    return aliases


def build_requester_scope_predicate(
    action_like,
    logged_user_email: str | None,
    requester_values: list[str] | None = None,
):
    normalized_email = normalize_requester_value(logged_user_email)

    if not normalized_email:
        return None

    values = unique_requester_values([
        normalized_email,
        *(requester_values or []),
    ])

    requester_email_column = func.lower(
        func.trim(func.coalesce(action_like.email_demandeur, ""))
    )
    requester_name_column = func.lower(
        func.trim(func.coalesce(action_like.demandeur, ""))
    )
    requester_email_missing = or_(
        action_like.email_demandeur.is_(None),
        func.trim(func.coalesce(action_like.email_demandeur, "")) == "",
    )

    name_matches = [requester_name_column == value for value in values]
    name_matches.append(requester_name_column.like(f"%{normalized_email}%"))

    for value in values:
        if value != normalized_email and "@" not in value:
            name_matches.append(requester_name_column.like(f"%{value}%"))

    return or_(
        requester_email_column == normalized_email,
        and_(requester_email_missing, or_(*name_matches)),
    )
