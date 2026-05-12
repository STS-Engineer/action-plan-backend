import datetime

from sqlalchemy import and_, func, or_

from app.models.action import Action


HIDDEN_CLOSED_DAYS = 7
CLOSED_HOME_BUCKET = "closed"
OVERDUE_HOME_BUCKET = "overdue"
IN_PROGRESS_HOME_BUCKET = "in_progress"
OVERDUE_STATUSES = {"overdue", "late"}


def normalize_action_status(status: str | None) -> str:
    return (status or "").strip().lower()


def is_action_hidden_from_home(action, today: datetime.date | None = None) -> bool:
    today = today or datetime.date.today()
    status = normalize_action_status(getattr(action, "status", None))
    closed_date = getattr(action, "closed_date", None)

    if status != CLOSED_HOME_BUCKET or not closed_date:
        return False

    return closed_date < (today - datetime.timedelta(days=HIDDEN_CLOSED_DAYS))


def get_action_home_bucket(action, today: datetime.date | None = None) -> str | None:
    today = today or datetime.date.today()
    status = normalize_action_status(getattr(action, "status", None))
    due_date = getattr(action, "due_date", None)

    if is_action_hidden_from_home(action, today):
        return None

    if status == CLOSED_HOME_BUCKET:
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


def get_normalized_action_status_expression(action_like=Action):
    status_col = _get_action_column(action_like, "status")
    return func.lower(func.coalesce(status_col, ""))


def get_action_hidden_from_home_predicate(action_like=Action):
    status_expr = get_normalized_action_status_expression(action_like)
    closed_date_col = _get_action_column(action_like, "closed_date")

    return and_(
        status_expr == CLOSED_HOME_BUCKET,
        closed_date_col.isnot(None),
        closed_date_col < (func.current_date() - HIDDEN_CLOSED_DAYS),
    )


def get_action_visible_from_home_predicate(action_like=Action):
    return ~get_action_hidden_from_home_predicate(action_like)


def get_action_closed_visible_predicate(action_like=Action):
    status_expr = get_normalized_action_status_expression(action_like)

    return and_(
        get_action_visible_from_home_predicate(action_like),
        status_expr == CLOSED_HOME_BUCKET,
    )


def get_action_overdue_predicate(action_like=Action):
    status_expr = get_normalized_action_status_expression(action_like)
    due_date_col = _get_action_column(action_like, "due_date")

    return and_(
        get_action_visible_from_home_predicate(action_like),
        or_(
            status_expr.in_(OVERDUE_STATUSES),
            and_(
                due_date_col.isnot(None),
                due_date_col < func.current_date(),
                status_expr != CLOSED_HOME_BUCKET,
            ),
        ),
    )


def get_action_in_progress_predicate(action_like=Action):
    status_expr = get_normalized_action_status_expression(action_like)

    return and_(
        get_action_visible_from_home_predicate(action_like),
        status_expr != CLOSED_HOME_BUCKET,
        ~get_action_overdue_predicate(action_like),
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
