import re
import unicodedata
from typing import Any

from sqlalchemy import text


OLIVIER_EMAIL = "olivier.spicker@avocarbon.com"
ORGANISATION_SOURCE = "v_people_with_boss"
UNAVAILABLE_SOURCE = "unavailable"
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
SELECT people_id,
       person AS personne,
       email,
       role_name AS role,
       role_name AS role_cible,
       hierarchy_path AS role_parent,
       (lower(coalesce(role_name, '')) = 'ceo' OR lower(coalesce(role_level, '')) = 'executive') AS is_primary,
       boss_person AS manager_hierarchique,
       boss_role AS manager_role,
       hierarchy_path AS manager_fonctionnel,
       people_id AS assignment_id,
       factory AS travaille_a,
       country AS paye_par,
       factory AS hr_site,
       boss_email,
       boss_person,
       boss_role,
       hierarchy_path,
       role_level
FROM public.v_people_with_boss
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
        0 if is_valid_email(person.get("email")) else 1,
        _role_sort_rank(person),
        0 if person.get("is_primary") else 1,
        assignment_id,
    )


def _role_sort_rank(person: dict[str, Any]) -> int:
    role = normalize_name(person.get("role"))
    role_level = normalize_name(person.get("role_level"))

    if normalize_email(person.get("email")) == OLIVIER_EMAIL and role == "CEO":
        return 0
    if role == "CEO" or role_level == "EXECUTIVE":
        return 1
    if role_level == "VP" or (role or "").startswith("VP "):
        return 2
    if role_level == "MANAGER":
        return 3
    if role_level == "PROFESSIONAL":
        return 4
    return 5


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
    role_level = normalize_name(person.get("role_level"))

    return email == OLIVIER_EMAIL or role == "CEO" or role_cible == "CEO" or role_level == "EXECUTIVE"


def _unavailable_chain(email: str | None, reason: str):
    return {
        "source": UNAVAILABLE_SOURCE,
        "input_email": normalize_email(email),
        "lookup": {
            "source": UNAVAILABLE_SOURCE,
            "input": normalize_email(email),
            "found": False,
            "selected": None,
            "candidates": [],
            "warnings": [{"type": reason}],
        },
        "chain": [],
        "chain_count": 0,
        "warnings": [{"type": reason}],
        "stop_reason": reason,
        "reaches_olivier": False,
    }


def build_organisation_manager_chain(organisation_db, email: str | None):
    if not organisation_db:
        return _unavailable_chain(email, "organisation_db_unavailable")

    try:
        lookup = find_person_by_email(organisation_db, email)
        selected = lookup.get("selected")
        warnings = [*lookup.get("warnings", [])]
        chain = []
        visited = set()
        stop_reason = None

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
        if _is_ceo_or_olivier(current):
            stop_reason = (
                "olivier_reached"
                if normalize_email(current.get("email")) == OLIVIER_EMAIL
                else "ceo_reached"
            )
            return {
                "source": ORGANISATION_SOURCE,
                "input_email": normalize_email(email),
                "lookup": lookup,
                "chain": chain,
                "chain_count": 0,
                "warnings": warnings,
                "stop_reason": stop_reason,
                "reaches_olivier": normalize_email(current.get("email")) == OLIVIER_EMAIL,
            }

        current_key = _person_key(current)
        if current_key:
            visited.add(current_key)

        while current and len(chain) < 30:
            boss_email = normalize_email(current.get("boss_email"))
            boss_person = current.get("boss_person") or current.get("manager_hierarchique")
            if boss_email:
                manager_lookup = find_person_by_email(organisation_db, boss_email)
                lookup_value = boss_email
                lookup_field = "boss_email"
            elif boss_person:
                manager_lookup = find_person_by_name(organisation_db, boss_person)
                lookup_value = boss_person
                lookup_field = "boss_person"
                warnings.append({
                    "type": "missing_boss_email_fallback_to_boss_person",
                    "boss_person": boss_person,
                    "personne": current.get("personne"),
                    "email": current.get("email"),
                })
            else:
                stop_reason = "missing_boss_email"
                break

            warnings.extend(manager_lookup.get("warnings", []))
            manager = manager_lookup.get("selected")

            if not manager:
                warnings.append({
                    "type": "boss_not_found",
                    lookup_field: lookup_value,
                    "boss_email": boss_email,
                    "boss_person": boss_person,
                    "personne": current.get("personne"),
                    "email": current.get("email"),
                })
                stop_reason = "boss_not_found"
                break

            is_top_manager = _is_ceo_or_olivier(manager)
            manager_key = _person_key(manager)
            if not is_top_manager and manager_key and manager_key in visited:
                warnings.append({
                    "type": "loop_detected",
                    lookup_field: lookup_value,
                    "boss_email": boss_email,
                    "boss_person": boss_person,
                    "personne": manager.get("personne"),
                    "email": manager.get("email"),
                })
                stop_reason = "loop_detected"
                break

            if manager_key:
                visited.add(manager_key)

            entry = {
                "level": len(chain) + 1,
                "manager_lookup_name": boss_person,
                "boss_lookup_email": boss_email,
                "boss_lookup_person": boss_person,
                "boss_lookup_used": lookup_field,
                "lookup": manager_lookup,
                "person": manager,
                "email": normalize_email(manager.get("email")),
                "valid_email": is_valid_email(manager.get("email")),
                "is_ceo_or_olivier": is_top_manager,
            }
            chain.append(entry)

            if not entry["valid_email"]:
                warnings.append({
                    "type": "boss_has_no_valid_email",
                    lookup_field: lookup_value,
                    "boss_email": boss_email,
                    "boss_person": boss_person,
                    "personne": manager.get("personne"),
                    "email": manager.get("email"),
                })
                stop_reason = "boss_has_no_valid_email"
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
    except Exception as exc:
        return {
            **_unavailable_chain(email, "organisation_db_query_failed"),
            "error_type": type(exc).__name__,
            "error_detail": str(exc),
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


def resolve_escalation_recipients(action, organisation_db=None):
    level = int(getattr(action, "escalation_level", None) or 0)
    responsible_email = normalize_email(getattr(action, "email_responsable", None))
    requester_email = normalize_email(getattr(action, "email_demandeur", None))
    responsible_chain = build_organisation_manager_chain(organisation_db, responsible_email)
    requester_chain = build_organisation_manager_chain(organisation_db, requester_email)
    warnings = [*responsible_chain.get("warnings", []), *requester_chain.get("warnings", [])]
    missing_reason = None
    to_email = None
    cc_emails = []

    organisation_unavailable = any(
        warning.get("type") in {"organisation_db_unavailable", "organisation_db_query_failed"}
        for warning in warnings
        if isinstance(warning, dict)
    )

    if organisation_unavailable:
        missing_reason = "organisation_db_unavailable"
    elif level <= 0:
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
    hierarchy_source_used = ORGANISATION_SOURCE if to_email and not organisation_unavailable else UNAVAILABLE_SOURCE

    return {
        "hierarchy_source_used": hierarchy_source_used,
        "fallback_used": False,
        "fallback_source_removed": True,
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
