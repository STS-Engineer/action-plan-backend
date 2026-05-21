import difflib
import re
import unicodedata

from sqlalchemy import func

from app.models.company_member import CompanyMember
from app.services.directory_service import get_all_underlings, normalize_email


HIGH_CONFIDENCE = 0.88
AMBIGUOUS_MARGIN = 0.05


def normalize_lookup_text(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(character for character in text if not unicodedata.combining(character))
    text = text.lower()
    text = re.sub(r"[^a-z0-9@.+_-]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def tokenize(value: str | None) -> list[str]:
    return [
        token
        for token in normalize_lookup_text(value).split()
        if token
    ]


def ratio(left: str, right: str) -> float:
    if not left or not right:
        return 0.0

    return difflib.SequenceMatcher(None, left, right).ratio()


def best_token_alignment_score(query_tokens: list[str], candidate_tokens: list[str]) -> float:
    if not query_tokens or not candidate_tokens:
        return 0.0

    scores = []

    for query_token in query_tokens:
        scores.append(max(ratio(query_token, candidate_token) for candidate_token in candidate_tokens))

    return sum(scores) / len(scores)


def member_full_name(member: CompanyMember) -> str:
    first_name = str(member.first_name or "").strip()
    last_name = str(member.last_name or "").strip()
    full_name = re.sub(r"\s+", " ", f"{first_name} {last_name}").strip()
    return member.display_name or full_name or member.email or ""


def member_to_candidate(member: CompanyMember, confidence: float, reason: str | None = None) -> dict:
    return {
        "type": "person",
        "display_name": member_full_name(member),
        "email": normalize_email(member.email),
        "department": member.department,
        "job_title": member.job_title,
        "site": member.site,
        "confidence": round(max(0.0, min(confidence, 1.0)), 3),
        "reason": reason,
    }


def empty_resolution(query: str | None, candidates: list[dict] | None = None) -> dict:
    return {
        "type": "unknown",
        "display_name": query,
        "email": None,
        "department": None,
        "confidence": 0.0,
        "candidates": candidates or [],
        "needs_confirmation": bool(candidates),
    }


def score_member(query: str, member: CompanyMember) -> tuple[float, str]:
    normalized_query = normalize_lookup_text(query)
    query_tokens = tokenize(query)
    email = normalize_email(member.email) or ""
    display_name = member.display_name or ""
    full_name = member_full_name(member)
    name_tokens = tokenize(f"{display_name} {member.first_name or ''} {member.last_name or ''}")
    normalized_name = normalize_lookup_text(full_name)
    normalized_display = normalize_lookup_text(display_name)

    if not normalized_query:
        return 0.0, "empty"

    if "@" in normalized_query:
        if normalized_query == email:
            return 1.0, "exact_email"

        if normalized_query in email:
            return 0.95, "partial_email"

    if normalized_query in {normalized_name, normalized_display}:
        return 0.98, "exact_name"

    if query_tokens and all(token in name_tokens for token in query_tokens):
        return 0.94 if len(query_tokens) > 1 else 0.84, "token_subset"

    aligned = best_token_alignment_score(query_tokens, name_tokens)
    sequence = max(ratio(normalized_query, normalized_name), ratio(normalized_query, normalized_display))
    score = max(aligned * 0.96, sequence * 0.9)

    if len(query_tokens) == 1 and score >= 0.82:
        score = min(score, 0.84)
        return score, "single_name"

    return score, "fuzzy_name"


def get_directory_members(directory_db) -> list[CompanyMember]:
    if directory_db is None:
        return []

    return (
        directory_db.query(CompanyMember)
        .filter(CompanyMember.email.isnot(None))
        .order_by(CompanyMember.display_name.asc().nullslast(), CompanyMember.email.asc())
        .all()
    )


def search_person_candidates(query: str, directory_db, limit: int = 6) -> list[dict]:
    scored_candidates = []

    for member in get_directory_members(directory_db):
        score, reason = score_member(query, member)

        if score >= 0.55:
            scored_candidates.append(member_to_candidate(member, score, reason))

    scored_candidates.sort(
        key=lambda candidate: (
            -candidate["confidence"],
            candidate["display_name"] or "",
            candidate["email"] or "",
        )
    )

    return scored_candidates[:limit]


def build_team_resolution(
    display_name: str,
    department: str | None,
    members: list[CompanyMember],
    confidence: float,
    reason: str,
) -> dict:
    candidates = [
        member_to_candidate(member, confidence=0.78, reason="team_member")
        for member in members[:8]
    ]

    return {
        "type": "team",
        "display_name": display_name,
        "email": None,
        "department": department,
        "confidence": round(confidence, 3),
        "candidates": candidates,
        "needs_confirmation": True,
        "reason": reason,
    }


def resolve_my_team(directory_db, logged_user_email: str | None) -> dict:
    members = get_all_underlings(directory_db, logged_user_email) if logged_user_email else []

    return build_team_resolution(
        display_name="My team",
        department=None,
        members=members,
        confidence=0.86 if members else 0.65,
        reason="my_team",
    )


def resolve_department_or_team(query: str, directory_db) -> dict | None:
    normalized_query = normalize_lookup_text(
        re.sub(r"\b(team|department|dept)\b", " ", query, flags=re.I)
    )

    if not normalized_query or directory_db is None:
        return None

    members = get_directory_members(directory_db)
    groups: dict[str, list[CompanyMember]] = {}

    for member in members:
        department = str(member.department or "").strip()

        if department:
            groups.setdefault(department, []).append(member)

    scored_groups = []

    for department, department_members in groups.items():
        normalized_department = normalize_lookup_text(department)
        department_tokens = tokenize(department)
        query_tokens = tokenize(normalized_query)

        score = max(
            ratio(normalized_query, normalized_department),
            best_token_alignment_score(query_tokens, department_tokens),
        )

        if normalized_query in normalized_department:
            score = max(score, 0.88)

        if score >= 0.68:
            scored_groups.append((score, department, department_members))

    scored_groups.sort(key=lambda item: (-item[0], item[1]))

    if scored_groups:
        score, department, department_members = scored_groups[0]
        display_name = f"{department} team" if "team" not in department.lower() else department

        return build_team_resolution(
            display_name=display_name,
            department=department,
            members=department_members,
            confidence=min(score, 0.9),
            reason="department_match",
        )

    if len(normalized_query) >= 2:
        title_matches = [
            member
            for member in members
            if normalized_query in normalize_lookup_text(member.job_title)
            or normalized_query in normalize_lookup_text(member.site)
        ][:8]

        if title_matches:
            return build_team_resolution(
                display_name=f"{query.strip()} team",
                department=title_matches[0].department,
                members=title_matches,
                confidence=0.74,
                reason="job_or_site_match",
            )

    return None


def resolve_responsible_query(
    query: str | None,
    directory_db,
    logged_user_email: str | None = None,
    limit: int = 6,
) -> dict:
    cleaned_query = re.sub(r"\s+", " ", str(query or "")).strip(" .,;")
    normalized_query = normalize_lookup_text(cleaned_query)

    if not normalized_query:
        return empty_resolution(cleaned_query)

    if normalized_query in {"none of these", "none", "not listed"}:
        return empty_resolution(cleaned_query)

    if normalized_query in {"my team", "our team", "team"}:
        return resolve_my_team(directory_db, normalize_email(logged_user_email))

    exact_email = normalize_email(cleaned_query)

    if exact_email and "@" in exact_email:
        member = (
            directory_db.query(CompanyMember)
            .filter(func.lower(CompanyMember.email) == exact_email)
            .first()
            if directory_db is not None
            else None
        )

        if member:
            candidate = member_to_candidate(member, 1.0, "exact_email")
            return {
                **candidate,
                "candidates": [candidate],
                "needs_confirmation": False,
            }

    person_candidates = search_person_candidates(cleaned_query, directory_db, limit=limit)
    team_resolution = resolve_department_or_team(cleaned_query, directory_db)

    if person_candidates:
        top_candidate = person_candidates[0]
        second_candidate = person_candidates[1] if len(person_candidates) > 1 else None
        top_confidence = top_candidate["confidence"]
        second_confidence = second_candidate["confidence"] if second_candidate else 0
        ambiguous = (
            second_candidate is not None
            and top_confidence < 0.95
            and (top_confidence - second_confidence) <= AMBIGUOUS_MARGIN
        )

        if top_confidence >= HIGH_CONFIDENCE and not ambiguous:
            return {
                **top_candidate,
                "candidates": person_candidates,
                "needs_confirmation": False,
            }

        if team_resolution and (
            re.search(r"\b(team|department|dept)\b", cleaned_query, flags=re.I)
            or top_confidence < HIGH_CONFIDENCE
            or team_resolution.get("confidence", 0) >= top_confidence
        ):
            return team_resolution

        if top_confidence >= 0.72:
            return {
                "type": "unknown",
                "display_name": cleaned_query,
                "email": None,
                "department": top_candidate.get("department"),
                "confidence": top_confidence,
                "candidates": person_candidates,
                "needs_confirmation": True,
                "reason": "ambiguous_person",
            }

    if team_resolution:
        return team_resolution

    return empty_resolution(cleaned_query, person_candidates)


def search_responsibles_service(query: str, directory_db, logged_user_email: str | None = None) -> dict:
    return resolve_responsible_query(
        query=query,
        directory_db=directory_db,
        logged_user_email=logged_user_email,
    )
