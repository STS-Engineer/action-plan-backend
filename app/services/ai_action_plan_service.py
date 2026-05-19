import datetime
import json
import os
import re

import httpx
from fastapi import HTTPException
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from app.models.action import Action
from app.models.company_member import CompanyMember
from app.models.sujet import Sujet
from app.schemas.ai_action_plan_schema import (
    AIActionPlanDraftRequest,
    ActionNode,
    PlanV1,
    SujetNode,
)
from app.services.action_priority_service import calculate_priority_index
from app.services.directory_service import get_member_by_email, normalize_email


MAX_SUJET_DEPTH = 3
MAX_ACTION_DEPTH = 3
MAX_TOTAL_ACTIONS = 50
ALLOWED_STATUSES = {"open", "blocked", "closed"}
ACTION_TYPES = ["action", "sub_action", "sub_sub_action"]

IMPORTANCE_NORMALIZATION = {
    "high": "haute",
    "haute": "haute",
    "important": "haute",
    "critique": "haute",
    "critical": "haute",
    "medium": "moyenne",
    "moyenne": "moyenne",
    "moyen": "moyenne",
    "average": "moyenne",
    "low": "faible",
    "faible": "faible",
    "basse": "faible",
}

URGENCY_NORMALIZATION = {
    "urgent": "Urgent",
    "urgente": "Urgent",
    "flexible": "Flexible",
    "secondary": "Secondaire",
    "secondaire": "Secondaire",
    "low": "Secondaire",
}


def slugify_code(value: str, prefix: str = "AI") -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", value.upper()).strip("-")
    slug = re.sub(r"-+", "-", slug)

    if not slug:
        slug = "ACTION-PLAN"

    return f"{prefix}-{slug[:36]}"


def humanize_prompt(prompt: str) -> str:
    problem_match = re.search(r"^Problem:\s*(.+)$", prompt, flags=re.I | re.M)

    if problem_match:
        problem = re.sub(r"\s+", " ", problem_match.group(1)).strip(" .")

        if problem:
            return problem[0].upper() + problem[1:90]

    normalized = re.sub(r"\s+", " ", prompt).strip(" .")
    normalized = re.sub(r"^(create|build|make|generate)\s+(an?\s+)?", "", normalized, flags=re.I)
    normalized = normalized[:90].strip(" .")

    if not normalized:
        return "AI Action Plan"

    return normalized[0].upper() + normalized[1:]


def normalize_status(value: str | None) -> str:
    normalized = str(value or "open").strip().lower()

    if normalized not in ALLOWED_STATUSES:
        raise HTTPException(
            status_code=400,
            detail="Allowed action status values are open, blocked, closed.",
        )

    return normalized


def normalize_importance_value(value: str | None) -> str:
    normalized = str(value or "moyenne").strip().lower()
    return IMPORTANCE_NORMALIZATION.get(normalized, "moyenne")


def normalize_urgency_value(value: str | None) -> str:
    normalized = str(value or "Flexible").strip().lower()
    return URGENCY_NORMALIZATION.get(normalized, "Flexible")


def normalize_optional_email(value: str | None) -> str | None:
    normalized = normalize_email(value)
    return normalized or None


def priority_index_to_priorite(priority_index: int | None) -> int:
    value = priority_index or 0

    if value >= 16:
        return 1

    if value >= 12:
        return 2

    if value >= 8:
        return 3

    if value >= 4:
        return 4

    return 5


def find_member_by_name(directory_db, name: str | None):
    normalized_name = re.sub(r"\s+", " ", str(name or "").strip().lower())

    if not normalized_name:
        return None

    if "@" in normalized_name:
        return get_member_by_email(directory_db, normalized_name)

    candidates = (
        directory_db.query(CompanyMember)
        .filter(
            or_(
                func.lower(CompanyMember.display_name) == normalized_name,
                func.lower(
                    func.concat(
                        func.coalesce(CompanyMember.first_name, ""),
                        " ",
                        func.coalesce(CompanyMember.last_name, ""),
                    )
                )
                == normalized_name,
            )
        )
        .limit(2)
        .all()
    )

    if len(candidates) == 1:
        return candidates[0]

    return None


def resolve_responsable(
    action: ActionNode,
    inserted_by: str,
    directory_db,
    warnings: list[str],
):
    explicit_email = normalize_optional_email(action.email_responsable)
    member = None

    if explicit_email:
        member = get_member_by_email(directory_db, explicit_email)

    if not member and action.responsable:
        member = find_member_by_name(directory_db, action.responsable)

    if not member:
        inserted_by_email = normalize_optional_email(inserted_by)
        member = get_member_by_email(directory_db, inserted_by_email) if inserted_by_email else None

    if member and member.email:
        action.email_responsable = normalize_optional_email(member.email)
        action.responsable = member.display_name or action.responsable or action.email_responsable
        return

    if not action.email_responsable:
        warning = (
            f"Could not confidently match responsable for action "
            f"'{action.titre}'. email_responsable left empty."
        )
        action.warnings.append(warning)
        warnings.append(warning)


def normalize_action_node(
    action: ActionNode,
    action_depth: int,
    order: int,
    inserted_by: str,
    directory_db,
    warnings: list[str],
) -> int:
    if action_depth >= MAX_ACTION_DEPTH:
        raise HTTPException(status_code=400, detail="Maximum action depth is 3.")

    action.status = normalize_status(action.status)
    action.importance = normalize_importance_value(action.importance)
    action.urgency = normalize_urgency_value(action.urgency)
    action.escalation_level = max(int(action.escalation_level or 0), 0)
    action.type = ACTION_TYPES[action_depth]
    action.ordre = action.ordre if action.ordre is not None else order

    resolve_responsable(action, inserted_by, directory_db, warnings)

    action.priority_index = calculate_priority_index(
        action.importance,
        action.urgency,
        action.escalation_level,
    )

    if action.priorite is None:
        action.priorite = priority_index_to_priorite(action.priority_index)

    count = 1

    for index, child in enumerate(action.sub_actions, start=1):
        count += normalize_action_node(
            child,
            action_depth + 1,
            index,
            inserted_by,
            directory_db,
            warnings,
        )

    return count


def normalize_sujet_node(
    sujet: SujetNode,
    sujet_depth: int,
    order: int,
    plan_code: str | None,
    inserted_by: str,
    directory_db,
    warnings: list[str],
) -> int:
    if sujet_depth >= MAX_SUJET_DEPTH:
        raise HTTPException(status_code=400, detail="Maximum sujet depth is 3.")

    if not sujet.code:
        suffix = f"{order:02d}" if sujet_depth else ""
        sujet.code = "-".join(part for part in [plan_code, suffix] if part) or slugify_code(sujet.titre)

    action_count = 0

    for index, action in enumerate(sujet.actions, start=1):
        action_count += normalize_action_node(
            action,
            0,
            index,
            inserted_by,
            directory_db,
            warnings,
        )

    for index, child in enumerate(sujet.sujets, start=1):
        action_count += normalize_sujet_node(
            child,
            sujet_depth + 1,
            index,
            f"{sujet.code}-{index:02d}",
            inserted_by,
            directory_db,
            warnings,
        )

    return action_count


def normalize_and_validate_plan(plan: PlanV1, directory_db) -> PlanV1:
    warnings = list(plan.warnings or [])

    if not plan.sujets:
        raise HTTPException(status_code=400, detail="At least one sujet is required.")

    plan.plan_code = plan.plan_code or slugify_code(plan.plan_title)

    total_actions = 0

    for index, sujet in enumerate(plan.sujets, start=1):
        total_actions += normalize_sujet_node(
            sujet,
            0,
            index,
            plan.plan_code,
            plan.inserted_by,
            directory_db,
            warnings,
        )

    if total_actions > MAX_TOTAL_ACTIONS:
        raise HTTPException(status_code=400, detail="Maximum total actions is 50.")

    plan.warnings = list(dict.fromkeys(warnings))

    return plan


def build_default_draft_actions(prompt: str, inserted_by: str):
    today = datetime.date.today()
    topic = humanize_prompt(prompt)

    return [
        SujetNode(
            titre="Diagnosis and baseline",
            description=f"Clarify the current situation for: {topic}.",
            actions=[
                ActionNode(
                    titre="Collect current performance data",
                    description="Gather overdue items, impacted suppliers, root causes, and current recovery dates.",
                    responsable=inserted_by,
                    due_date=today + datetime.timedelta(days=3),
                    importance="haute",
                    urgency="Urgent",
                    sub_actions=[
                        ActionNode(
                            titre="Build overdue supplier list",
                            description="Create a single list with supplier, part, delay reason, quantity, and promised delivery date.",
                            responsable=inserted_by,
                            due_date=today + datetime.timedelta(days=2),
                            importance="moyenne",
                            urgency="Urgent",
                        )
                    ],
                ),
                ActionNode(
                    titre="Identify top recurring delay causes",
                    description="Group delays by capacity, transport, quality, forecast, and ordering issues.",
                    responsable=inserted_by,
                    due_date=today + datetime.timedelta(days=5),
                    importance="moyenne",
                    urgency="Flexible",
                ),
            ],
        ),
        SujetNode(
            titre="Supplier recovery actions",
            description="Execute short-term actions with the highest-risk suppliers.",
            actions=[
                ActionNode(
                    titre="Agree recovery plan with critical suppliers",
                    description="Confirm shipment dates, escalation contacts, and daily follow-up rhythm.",
                    responsable=inserted_by,
                    due_date=today + datetime.timedelta(days=7),
                    importance="haute",
                    urgency="Flexible",
                    sub_actions=[
                        ActionNode(
                            titre="Schedule supplier review meetings",
                            description="Book reviews for suppliers with the largest overdue value or production risk.",
                            responsable=inserted_by,
                            due_date=today + datetime.timedelta(days=4),
                        ),
                        ActionNode(
                            titre="Track commitments daily",
                            description="Update committed dates and flag missed commitments for escalation.",
                            responsable=inserted_by,
                            due_date=today + datetime.timedelta(days=10),
                        ),
                    ],
                ),
                ActionNode(
                    titre="Escalate blocked deliveries",
                    description="Escalate actions that need management, logistics, or quality support.",
                    responsable=inserted_by,
                    due_date=today + datetime.timedelta(days=8),
                    importance="haute",
                    urgency="Urgent",
                ),
            ],
        ),
        SujetNode(
            titre="Prevention and monitoring",
            description="Install routines that prevent overdue deliveries from returning.",
            actions=[
                ActionNode(
                    titre="Create weekly supplier delivery KPI review",
                    description="Review overdue count, overdue value, supplier commitments, and prevention actions every week.",
                    responsable=inserted_by,
                    due_date=today + datetime.timedelta(days=14),
                    importance="moyenne",
                    urgency="Secondaire",
                ),
                ActionNode(
                    titre="Define early warning triggers",
                    description="Set triggers for late confirmations, capacity alerts, transport delays, and missing ASN updates.",
                    responsable=inserted_by,
                    due_date=today + datetime.timedelta(days=21),
                    importance="moyenne",
                    urgency="Secondaire",
                ),
            ],
        ),
    ]


async def generate_llm_draft_payload(payload: AIActionPlanDraftRequest):
    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        return None

    model = os.getenv("AI_ACTION_PLAN_MODEL", "gpt-4.1-mini")
    today = datetime.date.today().isoformat()

    system_prompt = (
        "You generate JSON drafts for recursive action plans. "
        "Return only valid JSON. The JSON must match this shape: "
        "{version:'1.0', plan_title:string, plan_code:string|null, inserted_by:string, "
        "sujets:[{titre, code, description, sujets, actions}], warnings:[string]}. "
        "Each action must have titre, description, status, priorite, responsable, "
        "email_responsable, due_date, ordre, importance, urgency, escalation_level, "
        "priority_index, type, sub_actions, warnings. "
        "Use max 3 sujet levels, max 3 action levels, max 50 actions. "
        "Allowed statuses: open, blocked, closed. "
        "Use French importance labels: haute, moyenne, faible. "
        "Use urgency labels: Urgent, Flexible, Secondaire. "
        "If a responsable is unclear, leave email_responsable null and add a warning."
    )

    user_prompt = {
        "prompt": payload.prompt,
        "inserted_by": payload.inserted_by,
        "scope": payload.scope,
        "today": today,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(user_prompt)},
                ],
            },
        )

    response.raise_for_status()
    content = response.json()["choices"][0]["message"]["content"]

    return json.loads(content)


async def generate_action_plan_draft_service(
    payload: AIActionPlanDraftRequest,
    directory_db,
) -> PlanV1:
    fallback_warnings = []

    try:
        llm_payload = await generate_llm_draft_payload(payload)

        if llm_payload:
            llm_payload["version"] = "1.0"
            llm_payload["inserted_by"] = payload.inserted_by

            return normalize_and_validate_plan(PlanV1.model_validate(llm_payload), directory_db)
    except Exception:
        fallback_warnings.append(
            "AI model draft was unavailable; generated a structured fallback draft."
        )

    plan_title = humanize_prompt(payload.prompt)
    plan_code = slugify_code(plan_title)

    plan = PlanV1(
        version="1.0",
        plan_title=plan_title,
        plan_code=plan_code,
        inserted_by=payload.inserted_by,
        sujets=[
            SujetNode(
                titre=plan_title,
                code=plan_code,
                description=payload.prompt,
                sujets=build_default_draft_actions(payload.prompt, payload.inserted_by),
            )
        ],
        warnings=fallback_warnings,
    )

    return normalize_and_validate_plan(plan, directory_db)


def ingest_action_recursive(
    action_node: ActionNode,
    db: Session,
    sujet_id: int,
    parent_action_id: int | None,
    action_depth: int,
    order: int,
    created_action_ids: list[int],
) -> Action:
    action = Action(
        sujet_id=sujet_id,
        parent_action_id=parent_action_id,
        type=ACTION_TYPES[action_depth],
        titre=action_node.titre,
        description=action_node.description,
        status=action_node.status,
        priorite=action_node.priorite,
        responsable=action_node.responsable,
        email_responsable=action_node.email_responsable,
        due_date=action_node.due_date,
        importance=action_node.importance,
        urgency=action_node.urgency,
        escalation_level=action_node.escalation_level,
        priority_index=action_node.priority_index,
        ordre=action_node.ordre if action_node.ordre is not None else order,
        closed_date=datetime.date.today() if action_node.status == "closed" else None,
    )

    db.add(action)
    db.flush()
    created_action_ids.append(action.id)

    for index, child in enumerate(action_node.sub_actions, start=1):
        ingest_action_recursive(
            child,
            db,
            sujet_id=sujet_id,
            parent_action_id=action.id,
            action_depth=action_depth + 1,
            order=index,
            created_action_ids=created_action_ids,
        )

    return action


def ingest_sujet_tree(
    sujet_node: SujetNode,
    db: Session,
    inserted_by: str,
    parent_sujet_id: int | None,
    created_sujet_ids: list[int],
    created_action_ids: list[int],
) -> Sujet:
    sujet = Sujet(
        code=sujet_node.code or slugify_code(sujet_node.titre),
        titre=sujet_node.titre,
        description=sujet_node.description,
        parent_sujet_id=parent_sujet_id,
        inserted_by=inserted_by,
    )

    db.add(sujet)
    db.flush()
    created_sujet_ids.append(sujet.id)

    for index, action_node in enumerate(sujet_node.actions, start=1):
        action = ingest_action_recursive(
            action_node,
            db,
            sujet_id=sujet.id,
            parent_action_id=None,
            action_depth=0,
            order=index,
            created_action_ids=created_action_ids,
        )

    for child in sujet_node.sujets:
        ingest_sujet_tree(
            child,
            db,
            inserted_by=inserted_by,
            parent_sujet_id=sujet.id,
            created_sujet_ids=created_sujet_ids,
            created_action_ids=created_action_ids,
        )

    return sujet


async def create_action_plan_service(
    plan: PlanV1,
    db: Session,
    directory_db,
):
    plan = normalize_and_validate_plan(plan, directory_db)
    created_sujet_ids: list[int] = []
    created_action_ids: list[int] = []
    root_sujet_ids: list[int] = []

    try:
        for sujet_node in plan.sujets:
            root_sujet = ingest_sujet_tree(
                sujet_node,
                db,
                inserted_by=plan.inserted_by,
                parent_sujet_id=None,
                created_sujet_ids=created_sujet_ids,
                created_action_ids=created_action_ids,
            )
            root_sujet_ids.append(root_sujet.id)

        db.commit()
    except Exception:
        db.rollback()
        raise

    return {
        "created": True,
        "plan_title": plan.plan_title,
        "plan_code": plan.plan_code,
        "root_sujet_id": root_sujet_ids[0] if root_sujet_ids else None,
        "root_sujet_ids": root_sujet_ids,
        "created_sujet_ids": created_sujet_ids,
        "created_action_ids": created_action_ids,
        "total_sujets": len(created_sujet_ids),
        "total_actions": len(created_action_ids),
        "warnings": plan.warnings,
    }
