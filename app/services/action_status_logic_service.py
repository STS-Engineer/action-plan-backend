import datetime
import unicodedata

from sqlalchemy import and_, case, func, or_, true

from app.models.action import Action


HIDDEN_CLOSED_DAYS = 7
CLOSED_HOME_BUCKET = "closed"
OVERDUE_HOME_BUCKET = "overdue"
IN_PROGRESS_HOME_BUCKET = "in_progress"
BLOCKED_HOME_BUCKET = "blocked"
OVERDUE_STATUSES = {"overdue", "late"}
STATUS_ALIASES = {
    "open": "open",
    "pending": "open",
    "in progress": "open",
    "in_progress": "open",
    "blocked": "blocked",
    "closed": "closed",
    "completed": "closed",
    "complete": "closed",
    "done": "closed",
    "termine": "closed",
    "terminee": "closed",
    "finished": "closed",
    "overdue": "overdue",
    "late": "overdue",
}


def _remove_accents(value: str) -> str:
    return "".join(
        character
        for character in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(character)
    )


def normalize_action_status(status: str | None) -> str:
    normalized = _remove_accents((status or "").strip().lower())
    return STATUS_ALIASES.get(normalized, normalized)


def is_action_hidden_from_home(action, today: datetime.date | None = None) -> bool:
    if bool(getattr(action, "is_deleted", False)):
        return True

    today = today or datetime.date.today()
    status = normalize_action_status(getattr(action, "status", None))
    closed_date = getattr(action, "closed_date", None)

    if status != CLOSED_HOME_BUCKET and not closed_date:
        return False

    if not closed_date:
        return False

    return closed_date < (today - datetime.timedelta(days=HIDDEN_CLOSED_DAYS))


def get_action_home_bucket(action, today: datetime.date | None = None) -> str | None:
    today = today or datetime.date.today()
    status = normalize_action_status(getattr(action, "status", None))
    due_date = getattr(action, "due_date", None)
    closed_date = getattr(action, "closed_date", None)

    if is_action_hidden_from_home(action, today):
        return None

    if status == CLOSED_HOME_BUCKET or closed_date:
        return CLOSED_HOME_BUCKET

    if status in OVERDUE_STATUSES:
        return OVERDUE_HOME_BUCKET

    if due_date and due_date < today:
        return OVERDUE_HOME_BUCKET

    return IN_PROGRESS_HOME_BUCKET


def _get_action_column(action_like, column_name: str):
    if hasattr(action_like, column_name):
        return getattr(action_like, column_name)

    if hasattr(action_like, "c"):
        return action_like.c[column_name]

    raise AttributeError(f"Column '{column_name}' not found on action_like")


def get_action_active_predicate(action_like=Action):
    try:
        is_deleted_col = _get_action_column(action_like, "is_deleted")
    except AttributeError:
        return true()

    return or_(is_deleted_col.is_(False), is_deleted_col.is_(None))


def get_normalized_action_status_expression(action_like=Action):
    status_col = _get_action_column(action_like, "status")
    raw_status = func.lower(func.trim(func.coalesce(status_col, "")))

    return case(
        (raw_status.in_(["pending", "in progress", "in_progress"]), "open"),
        (
            raw_status.in_([
                "completed",
                "complete",
                "done",
                "termine",
                "terminee",
                "termin\u00e9",
                "termin\u00e9e",
                "finished",
            ]),
            CLOSED_HOME_BUCKET,
        ),
        (raw_status == "late", OVERDUE_HOME_BUCKET),
        else_=raw_status,
    )


def get_action_hidden_from_home_predicate(action_like=Action):
    closed_date_col = _get_action_column(action_like, "closed_date")

    return and_(
        get_action_closed_state_predicate(action_like),
        closed_date_col.isnot(None),
        closed_date_col < (func.current_date() - HIDDEN_CLOSED_DAYS),
    )


def get_action_closed_state_predicate(action_like=Action):
    status_expr = get_normalized_action_status_expression(action_like)
    closed_date_col = _get_action_column(action_like, "closed_date")

    return or_(
        status_expr == CLOSED_HOME_BUCKET,
        closed_date_col.isnot(None),
    )


def get_action_visible_from_home_predicate(action_like=Action):
    return and_(
        get_action_active_predicate(action_like),
        ~get_action_hidden_from_home_predicate(action_like),
    )


def get_action_closed_visible_predicate(action_like=Action):
    return and_(
        get_action_visible_from_home_predicate(action_like),
        get_action_closed_state_predicate(action_like),
    )


def get_action_overdue_predicate(action_like=Action):
    status_expr = get_normalized_action_status_expression(action_like)
    due_date_col = _get_action_column(action_like, "due_date")

    return and_(
        get_action_visible_from_home_predicate(action_like),
        ~get_action_closed_state_predicate(action_like),
        or_(
            status_expr.in_(OVERDUE_STATUSES),
            and_(
                due_date_col.isnot(None),
                due_date_col < func.current_date(),
            ),
        ),
    )


def get_action_in_progress_predicate(action_like=Action):
    status_expr = get_normalized_action_status_expression(action_like)

    return and_(
        get_action_visible_from_home_predicate(action_like),
        ~get_action_closed_state_predicate(action_like),
        status_expr.in_(["open", BLOCKED_HOME_BUCKET]),
        ~get_action_overdue_predicate(action_like),
    )


def get_action_blocked_predicate(action_like=Action):
    status_expr = get_normalized_action_status_expression(action_like)

    return and_(
        get_action_visible_from_home_predicate(action_like),
        status_expr == BLOCKED_HOME_BUCKET,
    )


def get_action_home_bucket_predicate(bucket: str | None, action_like=Action):
    normalized_bucket = normalize_action_status(bucket)

    if not normalized_bucket:
        return get_action_visible_from_home_predicate(action_like)

    if normalized_bucket == CLOSED_HOME_BUCKET:
        return get_action_closed_visible_predicate(action_like)

    if normalized_bucket in {OVERDUE_HOME_BUCKET, "late"}:
        return get_action_overdue_predicate(action_like)

    if normalized_bucket == IN_PROGRESS_HOME_BUCKET:
        return get_action_in_progress_predicate(action_like)

    status_expr = get_normalized_action_status_expression(action_like)

    return and_(
        get_action_visible_from_home_predicate(action_like),
        status_expr == normalized_bucket,
    )
