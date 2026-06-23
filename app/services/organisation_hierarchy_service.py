import re
import unicodedata
from typing import Any

from sqlalchemy import text

from app.services.directory_service import get_manager_chain


OLIVIER_EMAIL = "olivier.spicker@avocarbon.com"
ORGANISATION_SOURCE = "v_personne_complete"
COMPANY_MEMBERS_SOURCE = "company_members"
INVALID_EMAIL_VALUES = {
    "",
    "-",
    "no email id",
    "\"\"",
    "none",
    "null",
    "n/a",
    "na",
}

PERSON_SELECT = """
SELECT people_id, personne, email, role, role_cible, role_parent, is_primary,
       manager_hierarchique, manager_role, manager_fonctionnel,
       assignment_id, travaille_a, paye_par, hr_site
FROM public.v_personne_complete
"""


def normalize_email(value: Any) -> str | None:
    if value is None:
        return None

    cleaned = str(value).strip().strip("'").strip('"').strip()
    if cleaned.lower().startswith("mailto:"):
        cleaned = cleaned[7:].strip()

    cleaned = cleaned.lower()
    if cleaned in INVALID_EMAIL_VALUES:
        return None

    return cleaned or None


def is_valid_email(value: Any) -> bool:
    email = normalize_email(value)
    return bool(email and "@" in email and "." in email.split("@", 1)[-1])


def normalize_name(value: Any) -> str | None:
    if value is None:
        return None

    cleaned = str(value).strip()
    if not cleaned or cleaned.lower() in INVALID_EMAIL_VALUES:
        return None

    cleaned = unicodedata.normalize("NFKD", cleaned)
    cleaned = "".join(char for char in cleaned if not unicodedata.combining(char))
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.upper().strip() or None


def _person_to_dict(row) -> dict[str, Any]:
    if not row:
        return {}

    data = dict(row)
    data["email"] = normalize_email(data.get("email"))
    data["normalized_name"] = normalize_name(data.get("personne"))
    return data


def _query_people_by_email(organisation_db, email: str | None) -> list[dict[str, Any]]:
    normalized_email = normalize_email(email)
    if not organisation_db or not normalized_email:
        return []

    result = organisation_db.execute(
        text(
            PERSON_SELECT
            + """
WHERE lower(trim(coalesce(email, ''))) = lower(trim(:email))
ORDER BY is_primary DESC NULLS LAST, assignment_id ASC NULLS LAST
"""
        ),
        {"email": normalized_email},
    )
    return [_person_to_dict(row) for row in result.mappings().all()]


def _query_people_by_name(organisation_db, name: str | None) -> list[dict[str, Any]]:
    normalized_name = normalize_name(name)
    if not organisation_db or not normalized_name:
        return []

    result = organisation_db.execute(
        text(
            PERSON_SELECT
            + """
WHERE upper(regexp_replace(trim(coalesce(personne, '')), '\\s+', ' ', 'g')) = :name
ORDER BY is_primary DESC NULLS LAST, assignment_id ASC NULLS LAST
"""
        ),
        {"name": normalized_name},
    )
    return [_person_to_dict(row) for row in result.mappings().all()]


def _assignment_sort_key(person: dict[str, Any]):
    assignment_id = person.get("assignment_id")
    if assignment_id is None:
        assignment_id = 999999999999

    return (
        0 if person.get("is_primary") else 1,
        0 if is_valid_email(person.get("email")) else 1,
        assignment_id,
    )


def _select_person(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None

    return sorted(rows, key=_assignment_sort_key)[0]


def _lookup_response(kind: str, input_value: str | None, rows: list[dict[str, Any]]):
    selected = _select_person(rows)
    warnings = []

    if len(rows) > 1:
        warnings.append({
            "type": "duplicate_person_matches",
            "lookup_kind": kind,
            "input": input_value,
            "count": len(rows),
            "candidates": [
                {
                    "people_id": row.get("people_id"),
                    "personne": row.get("personne"),
                    "email": row.get("email"),
                    "assignment_id": row.get("assignment_id"),
                    "is_primary": row.get("is_primary"),
                }
                for row in rows[:10]
            ],
        })

    if selected and not is_valid_email(selected.get("email")):
        warnings.append({
            "type": "invalid_selected_email",
            "lookup_kind": kind,
            "input": input_value,
            "people_id": selected.get("people_id"),
            "personne": selected.get("personne"),
            "email": selected.get("email"),
        })

    return {
        "source": ORGANISATION_SOURCE,
        "lookup_kind": kind,
        "input": input_value,
        "found": selected is not None,
        "selected": selected,
        "candidates": rows,
        "warnings": warnings,
    }


def find_person_by_email(organisation_db, email: str | None):
    normalized_email = normalize_email(email)
    rows = _query_people_by_email(organisation_db, normalized_email)
    return _lookup_response("email", normalized_email, rows)


def find_person_by_name(organisation_db, personne: str | None):
    rows = _query_people_by_name(organisation_db, personne)
    return _lookup_response("name", personne, rows)


def _person_key(person: dict[str, Any] | None) -> str | None:
    if not person:
        return None

    people_id = person.get("people_id")
    email = normalize_email(person.get("email"))
    name = normalize_name(person.get("personne"))

    if people_id is not None:
        return f"id:{people_id}"
    if email:
        return f"email:{email}"
    if name:
        return f"name:{name}"
    return None


def _is_ceo_or_olivier(person: dict[str, Any] | None) -> bool:
    if not person:
        return False

    email = normalize_email(person.get("email"))
    role = normalize_name(person.get("role"))
    role_cible = normalize_name(person.get("role_cible"))

    return email == OLIVIER_EMAIL or role == "CEO" or role_cible == "CEO"


def build_organisation_manager_chain(organisation_db, email: str | None):
    lookup = find_person_by_email(organisation_db, email)
    selected = lookup.get("selected")
    warnings = [*lookup.get("warnings", [])]
    chain = []
    visited = set()
    stop_reason = None

    if not organisation_db:
        return {
            "source": ORGANISATION_SOURCE,
            "input_email": normalize_email(email),
            "lookup": lookup,
            "chain": chain,
            "chain_count": 0,
            "warnings": [{"type": "organisation_db_unavailable"}],
            "stop_reason": "organisation_db_unavailable",
            "reaches_olivier": False,
        }

    if not selected:
        return {
            "source": ORGANISATION_SOURCE,
            "input_email": normalize_email(email),
            "lookup": lookup,
            "chain": chain,
            "chain_count": 0,
            "warnings": warnings,
            "stop_reason": "person_not_found",
            "reaches_olivier": False,
        }

    current = selected
    current_key = _person_key(current)
    if current_key:
        visited.add(current_key)

    while current and len(chain) < 30:
        manager_name = current.get("manager_hierarchique")
        if not manager_name:
            stop_reason = "missing_manager_hierarchique"
            break

        manager_lookup = find_person_by_name(organisation_db, manager_name)
        warnings.extend(manager_lookup.get("warnings", []))
        manager = manager_lookup.get("selected")

        if not manager:
            warnings.append({
                "type": "manager_hierarchique_not_found",
                "manager_hierarchique": manager_name,
                "personne": current.get("personne"),
                "email": current.get("email"),
            })
            stop_reason = "manager_hierarchique_not_found"
            break

        manager_key = _person_key(manager)
        if manager_key and manager_key in visited:
            warnings.append({
                "type": "loop_detected",
                "manager_hierarchique": manager_name,
                "personne": manager.get("personne"),
                "email": manager.get("email"),
            })
            stop_reason = "loop_detected"
            break

        if manager_key:
            visited.add(manager_key)

        entry = {
            "level": len(chain) + 1,
            "manager_lookup_name": manager_name,
            "lookup": manager_lookup,
            "person": manager,
            "email": normalize_email(manager.get("email")),
            "valid_email": is_valid_email(manager.get("email")),
            "is_ceo_or_olivier": _is_ceo_or_olivier(manager),
        }
        chain.append(entry)

        if not entry["valid_email"]:
            warnings.append({
                "type": "manager_has_no_valid_email",
                "manager_hierarchique": manager_name,
                "personne": manager.get("personne"),
                "email": manager.get("email"),
            })
            stop_reason = "manager_has_no_valid_email"
            break

        if entry["is_ceo_or_olivier"]:
            stop_reason = (
                "olivier_reached"
                if normalize_email(manager.get("email")) == OLIVIER_EMAIL
                else "ceo_reached"
            )
            break

        current = manager

    if len(chain) >= 30 and not stop_reason:
        stop_reason = "max_depth_reached"
        warnings.append({"type": "max_depth_reached"})

    return {
        "source": ORGANISATION_SOURCE,
        "input_email": normalize_email(email),
        "lookup": lookup,
        "chain": chain,
        "chain_count": len(chain),
        "warnings": warnings,
        "stop_reason": stop_reason,
        "reaches_olivier": any(
            normalize_email(entry.get("email")) == OLIVIER_EMAIL
            for entry in chain
        ),
    }


def _company_member_to_dict(member):
    if not member:
        return None

    return {
        "id": getattr(member, "id", None),
        "display_name": getattr(member, "display_name", None),
        "first_name": getattr(member, "first_name", None),
        "last_name": getattr(member, "last_name", None),
        "email": normalize_email(getattr(member, "email", None)),
        "job_title": getattr(member, "job_title", None),
        "department": getattr(member, "department", None),
        "site": getattr(member, "site", None),
        "country": getattr(member, "country", None),
        "manager_id": getattr(member, "manager_id", None),
        "manager_email": normalize_email(getattr(member, "manager_email", None)),
        "depth": getattr(member, "depth", None),
    }


def build_company_members_manager_chain(directory_db, email: str | None):
    if not directory_db or not normalize_email(email):
        return {
            "source": COMPANY_MEMBERS_SOURCE,
            "input_email": normalize_email(email),
            "chain": [],
            "chain_count": 0,
            "warnings": [{"type": "directory_db_or_email_unavailable"}],
            "stop_reason": "directory_db_or_email_unavailable",
            "reaches_olivier": False,
        }

    try:
        chain = get_manager_chain(directory_db, normalize_email(email))
    except Exception as exc:
        return {
            "source": COMPANY_MEMBERS_SOURCE,
            "input_email": normalize_email(email),
            "chain": [],
            "chain_count": 0,
            "warnings": [{"type": "company_members_lookup_failed", "detail": str(exc)}],
            "stop_reason": "company_members_lookup_failed",
            "reaches_olivier": False,
        }

    chain_items = [_company_member_to_dict(member) for member in chain]
    return {
        "source": COMPANY_MEMBERS_SOURCE,
        "input_email": normalize_email(email),
        "chain": chain_items,
        "chain_count": len(chain_items),
        "warnings": [],
        "stop_reason": "chain_exhausted",
        "reaches_olivier": any(
            normalize_email(member.get("email")) == OLIVIER_EMAIL
            for member in chain_items
        ),
    }


def _dedupe_cc(to_email: str | None, cc_emails: list[str | None]) -> list[str]:
    seen = {normalize_email(to_email)} if normalize_email(to_email) else set()
    result = []

    for email in cc_emails:
        normalized = normalize_email(email)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)

    return result


def _chain_email_at(chain_result: dict[str, Any], index: int):
    chain = chain_result.get("chain") or []
    if index < len(chain):
        entry = chain[index]
        email = normalize_email(entry.get("email"))
        return email if is_valid_email(email) else None

    if chain and chain_result.get("stop_reason") in {"ceo_reached", "olivier_reached"}:
        entry = chain[-1]
        email = normalize_email(entry.get("email"))
        return email if is_valid_email(email) else None

    return None


def _legacy_chain_email_at(chain_result: dict[str, Any], index: int):
    chain = chain_result.get("chain") or []
    if index < len(chain):
        email = normalize_email((chain[index] or {}).get("email"))
        return email if is_valid_email(email) else None
    return None


def resolve_with_organisation(action, organisation_db):
    level = int(getattr(action, "escalation_level", None) or 0)
    responsible_email = normalize_email(getattr(action, "email_responsable", None))
    requester_email = normalize_email(getattr(action, "email_demandeur", None))
    responsible_chain = build_organisation_manager_chain(organisation_db, responsible_email)
    requester_chain = build_organisation_manager_chain(organisation_db, requester_email)
    warnings = [*responsible_chain.get("warnings", []), *requester_chain.get("warnings", [])]
    missing_reason = None
    to_email = None
    cc_emails = []

    if level <= 0:
        missing_reason = "escalation_level_zero"
    elif level == 1:
        to_email = responsible_email if is_valid_email(responsible_email) else None
        if not to_email:
            missing_reason = "missing_responsible_email"
    elif level == 2:
        to_email = requester_email if is_valid_email(requester_email) else None
        cc_emails = [_chain_email_at(responsible_chain, 0)]
        if not to_email:
            missing_reason = "missing_requester_email"
        if not cc_emails[0]:
            warnings.append({"type": "missing_responsible_manager"})
    else:
        to_email = _chain_email_at(requester_chain, level - 3)
        cc_emails = [_chain_email_at(responsible_chain, level - 2)]
        if not to_email:
            missing_reason = "missing_requester_manager_chain"
        if not cc_emails[0]:
            warnings.append({"type": "missing_responsible_manager_chain"})

    cc_emails = _dedupe_cc(to_email, cc_emails)

    return {
        "source": ORGANISATION_SOURCE,
        "level": level,
        "to_email": normalize_email(to_email),
        "cc_emails": cc_emails,
        "resolved": bool(normalize_email(to_email)),
        "missing_reason": missing_reason,
        "warnings": warnings,
        "responsible_lookup": responsible_chain.get("lookup"),
        "requester_lookup": requester_chain.get("lookup"),
        "responsible_chain": responsible_chain,
        "requester_chain": requester_chain,
        "target_reached": bool(
            normalize_email(to_email) == OLIVIER_EMAIL
            or OLIVIER_EMAIL in cc_emails
            or responsible_chain.get("reaches_olivier")
            or requester_chain.get("reaches_olivier")
        ),
    }


def resolve_with_company_members(action, directory_db):
    level = int(getattr(action, "escalation_level", None) or 0)
    responsible_email = normalize_email(getattr(action, "email_responsable", None))
    requester_email = normalize_email(getattr(action, "email_demandeur", None))
    responsible_chain = build_company_members_manager_chain(directory_db, responsible_email)
    requester_chain = build_company_members_manager_chain(directory_db, requester_email)
    warnings = [*responsible_chain.get("warnings", []), *requester_chain.get("warnings", [])]
    missing_reason = None
    to_email = None
    cc_emails = []

    if level <= 0:
        missing_reason = "escalation_level_zero"
    elif level == 1:
        to_email = responsible_email if is_valid_email(responsible_email) else None
        if not to_email:
            missing_reason = "missing_responsible_email"
    elif level == 2:
        to_email = requester_email if is_valid_email(requester_email) else None
        cc_emails = [_legacy_chain_email_at(responsible_chain, 0)]
        if not to_email:
            missing_reason = "missing_requester_email"
        if not cc_emails[0]:
            warnings.append({"type": "missing_responsible_manager"})
    else:
        to_email = _legacy_chain_email_at(requester_chain, level - 3)
        cc_emails = [_legacy_chain_email_at(responsible_chain, level - 2)]
        if not to_email:
            missing_reason = "missing_requester_manager_chain"
        if not cc_emails[0]:
            warnings.append({"type": "missing_responsible_manager_chain"})

    cc_emails = _dedupe_cc(to_email, cc_emails)

    return {
        "source": COMPANY_MEMBERS_SOURCE,
        "level": level,
        "to_email": normalize_email(to_email),
        "cc_emails": cc_emails,
        "resolved": bool(normalize_email(to_email)),
        "missing_reason": missing_reason,
        "warnings": warnings,
        "responsible_chain": responsible_chain,
        "requester_chain": requester_chain,
        "target_reached": bool(
            normalize_email(to_email) == OLIVIER_EMAIL
            or OLIVIER_EMAIL in cc_emails
            or responsible_chain.get("reaches_olivier")
            or requester_chain.get("reaches_olivier")
        ),
    }


def resolve_escalation_recipients(action, organisation_db=None, directory_db=None):
    organisation_result = resolve_with_organisation(action, organisation_db)
    fallback_result = None
    fallback_used = False
    to_email = organisation_result.get("to_email")
    cc_emails = list(organisation_result.get("cc_emails") or [])
    hierarchy_source_used = ORGANISATION_SOURCE
    warning_types = {
        warning.get("type")
        for warning in organisation_result.get("warnings", [])
        if isinstance(warning, dict)
    }
    level = int(organisation_result.get("level") or 0)

    if level == 1:
        needs_fallback = not organisation_result.get("resolved")
    else:
        needs_fallback = (
            not organisation_result.get("resolved")
            or any(
                warning_type
                for warning_type in warning_types
                if warning_type
                and (
                    warning_type.startswith("missing_")
                    or warning_type
                    in {
                        "organisation_db_unavailable",
                        "person_not_found",
                        "manager_hierarchique_not_found",
                        "manager_has_no_valid_email",
                    }
                )
            )
        )

    if needs_fallback:
        fallback_result = resolve_with_company_members(action, directory_db)
        fallback_used = bool(
            fallback_result.get("resolved")
            or fallback_result.get("cc_emails")
        )

        if not to_email and fallback_result.get("to_email"):
            to_email = fallback_result["to_email"]
            hierarchy_source_used = COMPANY_MEMBERS_SOURCE

        if not cc_emails and fallback_result.get("cc_emails"):
            cc_emails = list(fallback_result["cc_emails"])
            hierarchy_source_used = (
                "mixed"
                if hierarchy_source_used == ORGANISATION_SOURCE and to_email
                else COMPANY_MEMBERS_SOURCE
            )

    cc_emails = _dedupe_cc(to_email, cc_emails)
    warnings = [
        *organisation_result.get("warnings", []),
        *((fallback_result or {}).get("warnings", [])),
    ]
    missing_reason = None if to_email else (
        (fallback_result or {}).get("missing_reason")
        or organisation_result.get("missing_reason")
        or "recipient_unresolved"
    )

    return {
        "hierarchy_source_used": hierarchy_source_used if to_email else "none",
        "fallback_used": fallback_used,
        "level": int(getattr(action, "escalation_level", None) or 0),
        "to_email": normalize_email(to_email),
        "cc_emails": cc_emails,
        "resolved": bool(normalize_email(to_email)),
        "missing_reason": missing_reason,
        "warnings": warnings,
        "organisation": organisation_result,
        "fallback": fallback_result,
        "responsible_chain": organisation_result.get("responsible_chain"),
        "requester_chain": organisation_result.get("requester_chain"),
    }
