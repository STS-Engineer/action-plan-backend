import re

from sqlalchemy import case, func, literal, or_
from app.models.action import Action
from app.services.action_Service import action_to_dict, get_latest_action_history_map
from app.models.sujet import Sujet
from app.services.action_access_service import normalize_access_email
from app.services.action_requester_scope_service import (
    build_requester_scope_predicate,
    get_logged_user_requester_aliases,
)
from app.services.action_status_logic_service import get_action_active_predicate
from app.services.auth_service import is_admin_role
from app.services.team_scope_service import get_direct_report_emails_for_team_scope


TEAM_ACTIONS_MAX_DEPTH = 2
SUPPORTED_SEARCH_SCOPES = {"my", "team", "requested_by_me", "all"}
SEARCH_CONFIG = "simple"
SEARCH_TOKEN_RE = re.compile(r"[\w@.+-]+", re.UNICODE)
MAX_SEARCH_TOKENS = 12


def tokenize_search_query(query: str) -> list[str]:
    tokens = []

    for raw_token in SEARCH_TOKEN_RE.findall(query.lower()):
        token = raw_token.strip("._+-@")

        if token:
            tokens.append(token)

    return list(dict.fromkeys(tokens))[:MAX_SEARCH_TOKENS]


def build_prefix_tsquery(tokens: list[str]) -> str | None:
    if not tokens:
        return None

    return " | ".join(f"'{token}':*" for token in tokens)


def coalesced_text(column):
    return func.coalesce(column, "")


def weighted_tsvector(column, weight: str):
    return func.setweight(
        func.to_tsvector(SEARCH_CONFIG, coalesced_text(column)),
        weight,
    )


def ilike_any_token(column, tokens: list[str]):
    return or_(*[coalesced_text(column).ilike(f"%{token}%") for token in tokens])


def token_match_score(column, tokens: list[str], points: float):
    return sum(
        case(
            (coalesced_text(column).ilike(f"%{token}%"), points),
            else_=0,
        )
        for token in tokens
    )


def get_team_scope_emails(organisation_db, email: str | None) -> list[str]:
    normalized_email = normalize_access_email(email)

    if not normalized_email or organisation_db is None:
        return []

    return get_direct_report_emails_for_team_scope(organisation_db, normalized_email)


def get_requester_aliases(db, normalized_email: str | None, directory_db=None) -> list[str]:
    return get_logged_user_requester_aliases(
        db,
        normalized_email,
        directory_db=directory_db,
    )


async def search_actions_service(
    query: str,
    db,
    email: str | None = None,
    scope: str | None = None,
    directory_db=None,
    organisation_db=None,
    user_role: str | None = None,
):
    if not query or query.strip() == "":
        return []

    tokens = tokenize_search_query(query)
    tsquery_text = build_prefix_tsquery(tokens)

    if not tsquery_text:
        return []

    exact_search = f"%{query.strip()}%"
    normalized_email = normalize_access_email(email)
    normalized_scope = scope.strip().lower() if scope else None
    email_responsable = func.lower(func.coalesce(Action.email_responsable, ""))

    if (email or scope) and normalized_scope not in SUPPORTED_SEARCH_SCOPES:
        return []

    if normalized_scope is None:
        if not normalized_email:
            return []

        normalized_scope = "my"

    metadata_text = func.concat_ws(
        " ",
        coalesced_text(Action.responsable),
        coalesced_text(Action.email_responsable),
        coalesced_text(Action.demandeur),
        coalesced_text(Action.email_demandeur),
        coalesced_text(Action.status),
        coalesced_text(Action.importance),
        coalesced_text(Action.urgency),
    )
    search_vector = (
        weighted_tsvector(Action.titre, "A")
        .op("||")(weighted_tsvector(Sujet.titre, "A"))
        .op("||")(weighted_tsvector(Action.description, "B"))
        .op("||")(weighted_tsvector(Sujet.description, "B"))
        .op("||")(
            func.setweight(
                func.to_tsvector(SEARCH_CONFIG, metadata_text),
                "D",
            )
        )
    )
    ts_query = func.to_tsquery(SEARCH_CONFIG, tsquery_text)
    ts_match = search_vector.op("@@")(ts_query)
    substring_match = or_(
        ilike_any_token(Action.titre, tokens),
        ilike_any_token(Action.description, tokens),
        ilike_any_token(Action.responsable, tokens),
        ilike_any_token(Action.email_responsable, tokens),
        ilike_any_token(Action.demandeur, tokens),
        ilike_any_token(Action.email_demandeur, tokens),
        ilike_any_token(Action.status, tokens),
        ilike_any_token(Action.importance, tokens),
        ilike_any_token(Action.urgency, tokens),
        ilike_any_token(Sujet.titre, tokens),
        ilike_any_token(Sujet.description, tokens),
    )
    rank_score = func.ts_rank_cd(search_vector, ts_query) * 100
    exact_phrase_score = (
        case((coalesced_text(Action.titre).ilike(exact_search), 80), else_=0)
        + case((coalesced_text(Sujet.titre).ilike(exact_search), 50), else_=0)
        + case((coalesced_text(Action.description).ilike(exact_search), 20), else_=0)
        + case((coalesced_text(Sujet.description).ilike(exact_search), 15), else_=0)
    )
    token_coverage_score = (
        token_match_score(Action.titre, tokens, 6)
        + token_match_score(Sujet.titre, tokens, 4)
        + token_match_score(Action.description, tokens, 2)
        + token_match_score(Sujet.description, tokens, 1.5)
        + token_match_score(Action.responsable, tokens, 0.75)
        + token_match_score(Action.email_responsable, tokens, 0.75)
        + token_match_score(Action.demandeur, tokens, 0.75)
        + token_match_score(Action.email_demandeur, tokens, 0.75)
        + token_match_score(Action.status, tokens, 0.5)
        + token_match_score(Action.importance, tokens, 0.5)
        + token_match_score(Action.urgency, tokens, 0.5)
    )
    search_score = (
        rank_score
        + exact_phrase_score
        + token_coverage_score
        + literal(0)
    ).label("search_score")

    filters = [
        get_action_active_predicate(Action),
        or_(ts_match, substring_match),
    ]

    if normalized_scope == "all":
        if not is_admin_role(user_role):
            return []

        normalized_scope = None

    if normalized_scope == "my":
        if not normalized_email:
            return []

        filters.append(email_responsable == normalized_email)

    if normalized_scope == "team":
        underling_emails = get_team_scope_emails(organisation_db, normalized_email)

        if not underling_emails:
            return []

        filters.append(email_responsable.in_(underling_emails))

    if normalized_scope == "requested_by_me":
        if not normalized_email:
            return []

        requester_values = get_requester_aliases(db, normalized_email, directory_db)

        requester_scope_filter = build_requester_scope_predicate(
            Action,
            normalized_email,
            requester_values=requester_values,
        )

        if requester_scope_filter is None:
            return []

        filters.append(requester_scope_filter)

    rows = (
        db.query(Action, search_score)
        .outerjoin(Sujet, Sujet.id == Action.sujet_id)
        .filter(*filters)
        .order_by(
            search_score.desc(),
            Action.priority_index.desc().nullslast(),
            Action.due_date.asc().nullslast(),
            Action.id.asc(),
        )
        .limit(100)
        .all()
    )
    actions = [row[0] for row in rows]

    results = []
    latest_history_by_action_id = get_latest_action_history_map(
        db,
        [action.id for action in actions],
    )

    for action in actions:
        sujet = db.query(Sujet).filter(Sujet.id == action.sujet_id).first()
        root_sujet = sujet

        while root_sujet and root_sujet.parent_sujet_id is not None:
            root_sujet = db.query(Sujet).filter(Sujet.id == root_sujet.parent_sujet_id).first()

        results.append(
            action_to_dict(
                action,
                root_sujet=root_sujet,
                source_sujet=sujet,
                latest_history=latest_history_by_action_id.get(action.id),
            )
        )

    return results
