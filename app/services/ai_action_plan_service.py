import datetime
import json
import logging
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
    AssistantChatRequest,
    AssistantConversationState,
    AssistantChatResponse,
    AssistantCreateRequest,
    AssistantSummary,
    PlanV1,
    SujetNode,
)
from app.services.action_priority_service import calculate_priority_index
from app.services.action_status_logic_service import get_action_active_predicate
from app.services.directory_service import get_member_by_email, normalize_email
from app.services.ia_assistant_knowledge_service import (
    get_ia_assistant_knowledge,
    get_ia_assistant_prompt_context,
)


MAX_SUJET_DEPTH = 3
MAX_ACTION_DEPTH = 3
MAX_TOTAL_ACTIONS = 50
ALLOWED_STATUSES = {"open", "blocked", "closed"}
ACTION_TYPES = ["action", "sub_action", "sub_sub_action"]
PROMPT_POLLUTION_MARKERS = [
    "Knowledge:",
    "Current conversation state",
    "Conversation history",
    "Relevant existing action plan patterns",
    "Create an enterprise action plan",
    "application_context",
    "recursive_model",
]

logger = logging.getLogger(__name__)

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
            titre="Diagnosis and scope",
            description=f"Clarify the current situation, impact, and expected result for: {topic}.",
            actions=[
                ActionNode(
                    titre="Confirm issue scope and current impact",
                    description="List affected areas, owners, current status, impact, and immediate risks.",
                    responsable=inserted_by,
                    due_date=today + datetime.timedelta(days=3),
                    importance="haute",
                    urgency="Urgent",
                    sub_actions=[
                        ActionNode(
                            titre="Build the working issue list",
                            description="Create one shared list with item, owner, due date, blocker, and latest update.",
                            responsable=inserted_by,
                            due_date=today + datetime.timedelta(days=2),
                            importance="moyenne",
                            urgency="Urgent",
                        )
                    ],
                ),
                ActionNode(
                    titre="Identify main root causes",
                    description="Group causes by process, people, material, method, system, and escalation needs.",
                    responsable=inserted_by,
                    due_date=today + datetime.timedelta(days=5),
                    importance="moyenne",
                    urgency="Flexible",
                ),
            ],
        ),
        SujetNode(
            titre="Corrective execution",
            description="Execute the actions needed to recover the situation and prevent further impact.",
            actions=[
                ActionNode(
                    titre="Define corrective action owners and due dates",
                    description="Assign clear owners, dates, and expected deliverables for each major corrective action.",
                    responsable=inserted_by,
                    due_date=today + datetime.timedelta(days=7),
                    importance="haute",
                    urgency="Flexible",
                    sub_actions=[
                        ActionNode(
                            titre="Confirm owner commitment",
                            description="Review ownership, blockers, and realistic completion date with each owner.",
                            responsable=inserted_by,
                            due_date=today + datetime.timedelta(days=4),
                        ),
                        ActionNode(
                            titre="Track corrective action progress",
                            description="Update progress, blockers, and next decisions before each review.",
                            responsable=inserted_by,
                            due_date=today + datetime.timedelta(days=10),
                        ),
                    ],
                ),
                ActionNode(
                    titre="Escalate blocked actions",
                    description="Escalate items that need management support, cross-functional decisions, or priority arbitration.",
                    responsable=inserted_by,
                    due_date=today + datetime.timedelta(days=8),
                    importance="haute",
                    urgency="Urgent",
                ),
            ],
        ),
        SujetNode(
            titre="Monitoring and prevention",
            description="Install follow-up routines and preventive actions so the problem does not return.",
            actions=[
                ActionNode(
                    titre="Create weekly progress review",
                    description="Review action progress, overdue items, blockers, and expected results every week.",
                    responsable=inserted_by,
                    due_date=today + datetime.timedelta(days=14),
                    importance="moyenne",
                    urgency="Secondaire",
                ),
                ActionNode(
                    titre="Define prevention and early warning triggers",
                    description="Set triggers for repeated delays, missed commitments, blocked owners, and overdue actions.",
                    responsable=inserted_by,
                    due_date=today + datetime.timedelta(days=21),
                    importance="moyenne",
                    urgency="Secondaire",
                ),
            ],
        ),
    ]


def build_clean_fallback_plan(
    objective: str,
    inserted_by: str,
    warnings: list[str] | None = None,
) -> PlanV1:
    clean_objective = extract_business_objective_from_text(objective)
    plan_code = slugify_code(clean_objective)

    return PlanV1(
        version="1.0",
        plan_title=clean_objective,
        plan_code=plan_code,
        inserted_by=inserted_by,
        sujets=[
            SujetNode(
                titre=clean_objective,
                code=plan_code,
                description=f"Action plan for: {clean_objective}.",
                sujets=build_default_draft_actions(clean_objective, inserted_by),
            )
        ],
        warnings=warnings or [],
    )


def plan_contains_prompt_pollution(plan: PlanV1) -> bool:
    values: list[str | None] = [
        plan.plan_title,
        plan.plan_code,
        *list(plan.warnings or []),
    ]

    def visit_action(action: ActionNode):
        values.extend([action.titre, action.description, *list(action.warnings or [])])

        for child_action in action.sub_actions:
            visit_action(child_action)

    def visit_sujet(sujet: SujetNode):
        values.extend([sujet.titre, sujet.code, sujet.description, *list(sujet.warnings or [])])

        for action in sujet.actions:
            visit_action(action)

        for child_sujet in sujet.sujets:
            visit_sujet(child_sujet)

    for sujet in plan.sujets:
        visit_sujet(sujet)

    return any(is_prompt_polluted(value) for value in values if value)


def sanitize_plan_or_fallback(plan: PlanV1, objective: str, inserted_by: str) -> PlanV1:
    clean_objective = extract_business_objective_from_text(objective or plan.plan_title)

    if not plan_contains_prompt_pollution(plan):
        plan.plan_title = clean_human_text(plan.plan_title, clean_objective) or clean_objective
        plan.plan_code = plan.plan_code or slugify_code(plan.plan_title)
        return plan

    logger.warning(
        "IA Assistant blocked prompt-polluted draft; using clean fallback for objective=%s",
        clean_objective,
    )

    warnings = list(plan.warnings or [])
    warnings.append("Internal prompt text was removed; generated a clean fallback plan.")
    return build_clean_fallback_plan(clean_objective, inserted_by, warnings=list(dict.fromkeys(warnings)))


async def generate_llm_draft_payload(payload: AIActionPlanDraftRequest):
    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        return None

    model = os.getenv("AI_ACTION_PLAN_MODEL", "gpt-4.1-mini")
    today = datetime.date.today().isoformat()
    knowledge_context = get_ia_assistant_prompt_context()

    system_prompt = (
        "You generate JSON drafts for AVOCarbon recursive action plans. "
        "Use this IA Assistant knowledge as authoritative application context: "
        f"{knowledge_context}\n"
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
        "business_objective": get_payload_business_objective(payload),
        "generation_context": payload.generation_context,
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
    allow_fallback: bool = True,
) -> PlanV1:
    fallback_warnings = []
    llm_payload = None
    clean_objective = get_payload_business_objective(payload)

    try:
        llm_payload = await generate_llm_draft_payload(payload)

        if llm_payload:
            llm_payload["version"] = "1.0"
            llm_payload["inserted_by"] = payload.inserted_by

            plan = sanitize_plan_or_fallback(
                PlanV1.model_validate(llm_payload),
                clean_objective,
                payload.inserted_by,
            )
            return normalize_and_validate_plan(plan, directory_db)
    except Exception as exc:
        if not allow_fallback:
            raise HTTPException(
                status_code=503,
                detail="IA Assistant is temporarily unavailable.",
            ) from exc

        fallback_warnings.append(
            "AI model draft was unavailable; generated a structured fallback draft."
        )

    if not llm_payload and not allow_fallback:
        raise HTTPException(
            status_code=503,
            detail="IA Assistant is temporarily unavailable.",
        )

    plan = build_clean_fallback_plan(
        clean_objective,
        payload.inserted_by,
        warnings=fallback_warnings,
    )

    return normalize_and_validate_plan(plan, directory_db)


def collect_action_nodes_from_sujets(sujets: list[SujetNode]) -> list[ActionNode]:
    actions: list[ActionNode] = []

    def visit_action(action: ActionNode):
        actions.append(action)

        for child_action in action.sub_actions:
            visit_action(child_action)

    def visit_sujet(sujet: SujetNode):
        for action in sujet.actions:
            visit_action(action)

        for child_sujet in sujet.sujets:
            visit_sujet(child_sujet)

    for sujet in sujets:
        visit_sujet(sujet)

    return actions


def collect_sujet_nodes(sujets: list[SujetNode]) -> list[SujetNode]:
    collected: list[SujetNode] = []

    def visit_sujet(sujet: SujetNode):
        collected.append(sujet)

        for child_sujet in sujet.sujets:
            visit_sujet(child_sujet)

    for sujet in sujets:
        visit_sujet(sujet)

    return collected


def get_user_message_texts(messages) -> list[str]:
    return [message.content.strip() for message in messages if message.role == "user" and message.content.strip()]


def clean_problem_statement(message: str | None) -> str | None:
    normalized = re.sub(r"\s+", " ", str(message or "")).strip(" .")

    if not normalized:
        return None

    normalized = re.sub(r"^(please\s+)?(create|build|make|generate|prepare|draft)\s+", "", normalized, flags=re.I)
    normalized = re.sub(
        r"^(an?\s+)?(urgent\s+|critical\s+|strategic\s+|high\s+priority\s+)?"
        r"(corrective\s+)?(enterprise\s+)?(action\s+plan|plan)\s*(to|for)?\s*",
        "",
        normalized,
        flags=re.I,
    ).strip(" .")
    normalized = re.split(
        r"\s*,\s*(?:assign|owner|responsible|deadline|due|include|with weekly|urgent|high priority)\b",
        normalized,
        maxsplit=1,
        flags=re.I,
    )[0].strip(" .")

    if normalized.lower() in {"", "action plan", "plan", "create action plan"}:
        return None

    return normalized[0].upper() + normalized[1:]


def extract_business_objective_from_text(value: str | None) -> str:
    text = str(value or "")
    problem_match = re.search(r"\bProblem:\s*([^.;\n]+)", text, flags=re.I)

    if problem_match:
        objective = re.sub(r"\s+", " ", problem_match.group(1)).strip(" .")

        if objective and not is_prompt_polluted(objective):
            return objective[0].upper() + objective[1:120]

    for marker in PROMPT_POLLUTION_MARKERS:
        marker_index = text.find(marker)

        if marker_index > 0:
            text = text[:marker_index]
            break

    objective = clean_problem_statement(text) if "clean_problem_statement" in globals() else None
    objective = objective or humanize_prompt(text)

    if not objective or is_prompt_polluted(objective):
        return "Action plan"

    return objective[:120].strip(" .")


def get_payload_business_objective(payload: AIActionPlanDraftRequest) -> str:
    return extract_business_objective_from_text(
        payload.business_objective or payload.prompt
    )


def is_prompt_polluted(value: str | None) -> bool:
    text = str(value or "")

    if not text:
        return False

    if any(marker.lower() in text.lower() for marker in PROMPT_POLLUTION_MARKERS):
        return True

    return bool(
        re.search(r"\{[^{}]*(application_context|recursive_model)[^{}]*\}", text, flags=re.I | re.S)
    )


def clean_human_text(value: str | None, fallback: str | None = None) -> str | None:
    if value is None:
        return fallback

    text = re.sub(r"\s+", " ", str(value)).strip()

    if not text:
        return fallback

    if not is_prompt_polluted(text):
        return text

    problem_match = re.search(r"\bProblem:\s*([^.;\n]+)", text, flags=re.I)

    if problem_match:
        cleaned = re.sub(r"\s+", " ", problem_match.group(1)).strip(" .")

        if cleaned and not is_prompt_polluted(cleaned):
            return cleaned

    return fallback


DEPARTMENT_ALIASES = {
    "quality": "Quality team",
    "it": "IT team",
    "maintenance": "Maintenance team",
    "production": "Production team",
    "purchasing": "Purchasing team",
    "purchase": "Purchasing team",
    "supply chain": "Supply chain team",
    "logistics": "Logistics team",
    "hr": "HR team",
    "finance": "Finance team",
    "engineering": "Engineering team",
}


def normalize_responsible_label(value: str | None) -> str | None:
    normalized = re.sub(r"\s+", " ", str(value or "")).strip(" .,;").lower()

    if not normalized:
        return None

    if normalized in DEPARTMENT_ALIASES:
        return DEPARTMENT_ALIASES[normalized]

    if normalized.endswith(" team"):
        return normalized[0].upper() + normalized[1:]

    return re.sub(r"\s+", " ", str(value or "")).strip(" .,;")


def extract_responsible_from_text(text: str) -> str | None:
    email_match = re.search(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+", text)

    if email_match:
        return email_match.group(0)

    owner_match = re.search(
        r"\b(?:responsible|owner|owned by|assign(?:ed)?(?: it)? to|belongs to)\s+"
        r"([A-Za-z][A-Za-z0-9@._' -]{1,60})",
        text,
        flags=re.I,
    )

    if owner_match:
        return normalize_responsible_label(owner_match.group(1))

    team_match = re.search(r"\b([A-Za-z][A-Za-z0-9&' -]{1,42}\s+team)\b", text, flags=re.I)

    if team_match:
        return normalize_responsible_label(team_match.group(1))

    if re.search(r"\bmy team\b", text, flags=re.I):
        return "My team"

    short_department = re.fullmatch(
        r"\s*(quality|it|maintenance|production|purchasing|purchase|supply chain|logistics|hr|finance|engineering)\s*",
        text,
        flags=re.I,
    )

    if short_department:
        return normalize_responsible_label(short_department.group(1))

    return None


def extract_deadline_from_text(text: str) -> str | None:
    deadline_match = re.search(
        r"\b(?:deadline|due date|target date|target|due|by|before)\s+"
        r"(?:is\s+|should be\s+|for\s+)?([^.,;\n]+)",
        text,
        flags=re.I,
    )

    if deadline_match:
        return re.sub(r"\s+", " ", deadline_match.group(1)).strip(" .")

    pattern_match = re.search(
        r"\b(end of [A-Za-z]+|this week|next week|end of month|end of quarter|"
        r"next month|next quarter|30 days|60 days|90 days|tomorrow|today|as soon as possible|asap)\b",
        text,
        flags=re.I,
    )

    if pattern_match:
        return pattern_match.group(1)

    return None


def extract_urgency_from_text(text: str) -> str | None:
    if re.search(r"\b(urgent|high priority|critical|asap|as soon as possible)\b", text, flags=re.I):
        return "Urgent"

    if re.search(r"\b(strategic)\b", text, flags=re.I):
        return "Strategic"

    if re.search(r"\b(medium priority|normal priority|normal|medium)\b", text, flags=re.I):
        return "Flexible"

    if re.search(r"\b(low priority|secondary|flexible)\b", text, flags=re.I):
        return "Flexible"

    return None


def extract_sub_action_preference(text: str) -> bool | None:
    if re.search(r"\b(no sub-actions|no sub actions|keep it simple|simple plan|no recurring)\b", text, flags=re.I):
        return False

    if re.search(
        r"\b(sub-actions|sub actions|nested|recursive|detailed|weekly|daily|follow-up|follow up|monitoring)\b",
        text,
        flags=re.I,
    ):
        return True

    return None


def extract_kpi_from_text(text: str, problem: str | None) -> str | None:
    kpi_match = re.search(
        r"\b(?:kpi|target|expected result|goal)\s*(?:is|:)?\s*([^.\n;]+)",
        text,
        flags=re.I,
    )

    if kpi_match:
        return re.sub(r"\s+", " ", kpi_match.group(1)).strip(" .")

    if problem:
        return f"Progress on: {problem}"

    return None


def get_latest_user_message(payload: AssistantChatRequest) -> str | None:
    user_messages = get_user_message_texts(payload.messages)
    return user_messages[-1] if user_messages else None


def is_affirmative_answer(text: str) -> bool:
    return bool(re.search(r"\b(yes|yeah|yep|sure|ok|okay|include|add it|do it|please do)\b", text, flags=re.I))


def is_negative_answer(text: str) -> bool:
    return bool(re.search(r"\b(no|nope|not needed|do not|don't|without|skip)\b", text, flags=re.I))


def is_generic_answer(text: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()
    return normalized in {
        "yes",
        "yeah",
        "yep",
        "sure",
        "ok",
        "okay",
        "no",
        "nope",
        "my team",
        "team",
    }


def extract_responsible_answer(text: str, allow_plain_text: bool = False) -> str | None:
    responsible = extract_responsible_from_text(text)

    if responsible:
        return responsible

    if not allow_plain_text:
        return None

    normalized = re.sub(r"\s+", " ", text).strip(" .,;")

    if not normalized or is_affirmative_answer(normalized) or is_negative_answer(normalized):
        return None

    if extract_deadline_from_text(normalized) or extract_urgency_from_text(normalized):
        return None

    if extract_sub_action_preference(normalized) is not None:
        return None

    department = normalize_responsible_label(normalized)

    if department:
        return department

    if len(normalized) <= 80:
        return normalized

    return None


def extract_deadline_answer(text: str, allow_plain_text: bool = False) -> str | None:
    explicit_deadline = extract_deadline_from_text(text)

    if explicit_deadline:
        days_match = re.fullmatch(r"(\d+)\s+days?", explicit_deadline.strip(), flags=re.I)

        if days_match:
            days = int(days_match.group(1))
            due_date = datetime.date.today() + datetime.timedelta(days=days)
            return f"{days} days (by {due_date.isoformat()})"

        normalized_deadline = explicit_deadline.strip().lower()

        if normalized_deadline in {"next week"}:
            due_date = datetime.date.today() + datetime.timedelta(days=7)
            return f"next week (by {due_date.isoformat()})"

        if normalized_deadline in {"as soon as possible", "asap"}:
            due_date = datetime.date.today() + datetime.timedelta(days=3)
            return f"as soon as possible (by {due_date.isoformat()})"

        if normalized_deadline == "next month":
            today = datetime.date.today()
            year = today.year + (1 if today.month == 12 else 0)
            month = 1 if today.month == 12 else today.month + 1
            return f"next month ({year:04d}-{month:02d})"

        return explicit_deadline

    days_match = re.search(r"\b(\d{1,3})\s+days?\b", text, flags=re.I)

    if days_match:
        days = int(days_match.group(1))
        due_date = datetime.date.today() + datetime.timedelta(days=days)
        return f"{days} days (by {due_date.isoformat()})"

    date_match = re.search(
        r"\b(\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?|"
        r"\d{4}-\d{2}-\d{2}|"
        r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2}|"
        r"\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*)\b",
        text,
        flags=re.I,
    )

    if date_match:
        return date_match.group(1)

    next_month_match = re.search(r"\bnext month\b", text, flags=re.I)

    if next_month_match:
        today = datetime.date.today()
        year = today.year + (1 if today.month == 12 else 0)
        month = 1 if today.month == 12 else today.month + 1
        return f"next month ({year:04d}-{month:02d})"

    if not allow_plain_text:
        return None

    normalized = re.sub(r"\s+", " ", text).strip(" .,;")

    if not normalized or is_affirmative_answer(normalized) or is_negative_answer(normalized):
        return None

    if len(normalized) <= 80:
        return normalized

    return None


def extract_subaction_answer(
    text: str,
    allow_generic_yes_no: bool = False,
) -> tuple[bool | None, bool | None, bool | None]:
    normalized = text.strip()
    preference = extract_sub_action_preference(normalized)
    include_monitoring = True if re.search(r"\b(weekly|daily|monitoring|follow-up|follow up|review)\b", normalized, flags=re.I) else None
    include_escalation = True if re.search(r"\b(escalat\w*|overdue|blocked)\b", normalized, flags=re.I) else None

    if re.search(r"\b(remove|without|no|disable|skip)\b.*\b(monitoring|follow-up|follow up|review)\b", normalized, flags=re.I):
        include_monitoring = False

    if re.search(r"\b(remove|without|no|disable|skip)\b.*\b(escalat\w*|overdue|blocked)\b", normalized, flags=re.I):
        include_escalation = False

    if preference is not None:
        if preference:
            return True, include_monitoring if include_monitoring is not None else True, include_escalation

        return False, False, False

    if allow_generic_yes_no:
        if is_affirmative_answer(normalized):
            return True, True, True

        if is_negative_answer(normalized):
            return False, False, False

    return None, include_monitoring, include_escalation


def normalize_assistant_state(
    state: AssistantConversationState | None,
    scope: str,
) -> AssistantConversationState:
    if state:
        data = state.model_dump()
        data["scope"] = scope
        return AssistantConversationState.model_validate(data)

    return AssistantConversationState(scope=scope)


def advance_assistant_state(state: AssistantConversationState) -> AssistantConversationState:
    if not state.objective:
        state.current_step = "objective"
    elif not state.responsible_team:
        state.current_step = "responsible_team"
    elif not state.deadline:
        state.current_step = "deadline"
    elif state.include_subactions is None:
        state.current_step = "subactions"
    elif not state.urgency:
        state.current_step = "urgency"
    else:
        state.current_step = "ready_to_create"

    return state


def update_state_from_message(
    state: AssistantConversationState,
    message: str,
) -> tuple[AssistantConversationState, dict[str, object]]:
    current_step = state.current_step
    extracted: dict[str, object] = {}
    normalized_message = re.sub(r"\s+", " ", message).strip()

    if not normalized_message:
        return advance_assistant_state(state), extracted

    if current_step == "objective" or current_step == "ready_to_create":
        objective = clean_problem_statement(normalized_message)

        if objective and (current_step == "objective" or re.search(r"\b(objective|problem|plan|action plan)\b", normalized_message, flags=re.I)):
            state.objective = objective
            extracted["objective"] = objective

    responsible = extract_responsible_answer(
        normalized_message,
        allow_plain_text=current_step == "responsible_team",
    )

    if responsible and (current_step in {"responsible_team", "ready_to_create"} or not state.responsible_team):
        state.responsible_team = responsible
        extracted["responsible_team"] = responsible

    deadline = extract_deadline_answer(
        normalized_message,
        allow_plain_text=current_step == "deadline",
    )

    if deadline and (current_step in {"deadline", "ready_to_create"} or not state.deadline):
        state.deadline = deadline
        extracted["deadline"] = deadline

    include_subactions, include_monitoring, include_escalation = extract_subaction_answer(
        normalized_message,
        allow_generic_yes_no=current_step == "subactions",
    )

    if include_subactions is not None and (
        current_step in {"subactions", "ready_to_create"} or state.include_subactions is None
    ):
        state.include_subactions = include_subactions
        extracted["include_subactions"] = include_subactions

    if include_monitoring is not None:
        state.include_monitoring = include_monitoring
        extracted["include_monitoring"] = include_monitoring

    if include_escalation is not None:
        state.include_escalation = include_escalation
        extracted["include_escalation"] = include_escalation

    urgency = extract_urgency_from_text(normalized_message)

    if current_step == "urgency" and not urgency:
        if re.search(r"\b(high|priority 1|p1)\b", normalized_message, flags=re.I):
            urgency = "Urgent"
        elif re.search(r"\b(low|secondary)\b", normalized_message, flags=re.I):
            urgency = "Flexible"
        elif re.search(r"\b(normal|standard)\b", normalized_message, flags=re.I):
            urgency = "Normal"

    if urgency and (current_step in {"urgency", "ready_to_create"} or not state.urgency):
        state.urgency = urgency
        extracted["urgency"] = urgency

    return advance_assistant_state(state), extracted


def rebuild_state_from_history(payload: AssistantChatRequest) -> AssistantConversationState:
    state = AssistantConversationState(scope=payload.scope)

    for message in get_user_message_texts(payload.messages):
        state, _ = update_state_from_message(state, message)

    return state


def get_question_for_step(step: str) -> str | None:
    questions = {
        "objective": "What problem or objective should this action plan solve?",
        "responsible_team": "Got it. Which team or responsible person should own this plan?",
        "deadline": "What deadline should I use for the main actions?",
        "subactions": "Should I include sub-actions, weekly monitoring, and escalation follow-up?",
        "urgency": "Is this urgent, high priority, strategic, or a normal priority?",
    }

    return questions.get(step)


def assistant_state_to_slots(state: AssistantConversationState) -> dict[str, object]:
    return {
        "problem": state.objective,
        "responsible": state.responsible_team,
        "deadline": state.deadline,
        "urgency": state.urgency,
        "sub_actions": state.include_subactions,
        "monitoring": state.include_monitoring,
        "escalation": state.include_escalation,
        "kpi": extract_kpi_from_text(state.objective or "", state.objective),
        "scope": state.scope,
    }


def log_assistant_transition(
    current_step: str,
    extracted: dict[str, object],
    next_state: AssistantConversationState,
):
    logger.info(
        "IA Assistant state transition current_step=%s extracted_fields=%s next_step=%s conversation_state=%s",
        current_step,
        extracted,
        next_state.current_step,
        next_state.model_dump(),
    )



def build_assistant_prompt(
    slots: dict[str, object],
    inserted_by: str,
    conversation_messages=None,
    conversation_state: AssistantConversationState | None = None,
    relevant_examples: list[dict] | None = None,
) -> str:
    sub_actions = "Yes" if slots.get("sub_actions") else "No"
    monitoring = "Yes" if slots.get("monitoring") else "No"
    escalation = "Yes" if slots.get("escalation") else "No"
    knowledge_context = get_ia_assistant_prompt_context(max_chars=4500)
    conversation_lines = []

    for message in conversation_messages or []:
        if message.role in {"user", "assistant"}:
            conversation_lines.append(f"{message.role}: {message.content}")

    lines = [
        "Create an enterprise action plan from this IA Assistant conversation.",
        "Use the AVOCarbon Action Plan knowledge and examples below. Do not expose JSON to the user.",
        f"Knowledge: {knowledge_context}",
        f"Current conversation state: {json.dumps(conversation_state.model_dump() if conversation_state else {}, ensure_ascii=True, default=str)}.",
        f"Conversation history: {json.dumps(conversation_lines[-12:], ensure_ascii=True, default=str)}.",
        f"Relevant existing action plan patterns: {json.dumps(relevant_examples or [], ensure_ascii=True, default=str)}.",
        f"Scope: {slots.get('scope') or 'my'}.",
        f"Problem: {slots.get('problem') or 'Not specified'}.",
        f"Responsible department/team/person: {slots.get('responsible') or inserted_by}.",
        f"Deadline: {slots.get('deadline') or 'Not specified'}.",
        f"Priority/Urgency: {slots.get('urgency') or 'Normal business priority'}.",
        f"Sub-actions requested: {sub_actions}.",
        f"Weekly monitoring requested: {monitoring}.",
        f"Escalation follow-up requested: {escalation}.",
        f"KPI or expected result: {slots.get('kpi') or 'Define a measurable follow-up KPI'}.",
        "Create clear topics, nested topics when useful, actions, sub-actions, responsables, due dates, urgency, importance, and escalation fields.",
    ]

    return "\n".join(lines)


def build_assistant_summary(
    plan: PlanV1,
    slots: dict[str, object] | None = None,
) -> AssistantSummary:
    slots = slots or {}
    actions = collect_action_nodes_from_sujets(plan.sujets)
    sujets = collect_sujet_nodes(plan.sujets)
    due_dates = sorted(str(action.due_date) for action in actions if action.due_date)
    first_responsible = next(
        (
            action.responsable or action.email_responsable
            for action in actions
            if action.responsable or action.email_responsable
        ),
        None,
    )
    first_urgency = next((action.urgency for action in actions if action.urgency), None)

    return AssistantSummary(
        plan_title=plan.plan_title,
        topics=[sujet.titre for sujet in sujets[:10]],
        actions=[action.titre for action in actions[:12]],
        features=[
            feature
            for feature in [
                "Sub-actions included" if slots.get("sub_actions") else None,
                "Weekly monitoring" if slots.get("monitoring") else None,
                "Escalation follow-up" if slots.get("escalation") else None,
            ]
            if feature
        ],
        actions_count=len(actions),
        main_responsible=str(slots.get("responsible") or first_responsible or ""),
        deadline=str(slots.get("deadline") or (due_dates[-1] if due_dates else "")),
        urgency=str(slots.get("urgency") or first_urgency or "Normal"),
        sub_actions_included=bool(
            slots.get("sub_actions") or any(action.sub_actions for action in actions)
        ),
    )


def get_relevant_action_plan_examples(prompt: str | None, db: Session | None, limit: int = 5) -> list[dict]:
    if db is None:
        return []

    try:
        terms = [
            term.lower()
            for term in re.findall(r"[A-Za-z0-9]+", str(prompt or ""))
            if len(term) >= 4
        ][:6]
        query = db.query(Sujet).filter(Sujet.parent_sujet_id.is_(None))

        if terms:
            query = query.filter(or_(*[Sujet.titre.ilike(f"%{term}%") for term in terms]))

        sujets = query.order_by(Sujet.created_at.desc()).limit(limit).all()

        if not sujets:
            sujets = (
                db.query(Sujet)
                .filter(Sujet.parent_sujet_id.is_(None))
                .order_by(Sujet.created_at.desc())
                .limit(limit)
                .all()
            )

        examples = []

        for sujet in sujets:
            actions = (
                db.query(Action)
                .filter(Action.sujet_id == sujet.id)
                .filter(get_action_active_predicate(Action))
                .order_by(Action.created_at.desc())
                .limit(8)
                .all()
            )
            examples.append(
                {
                    "root_sujet_title": sujet.titre,
                    "action_titles": [action.titre for action in actions],
                    "action_types": sorted({action.type for action in actions if action.type}),
                    "responsible_examples": sorted(
                        {
                            action.responsable or action.email_responsable
                            for action in actions
                            if action.responsable or action.email_responsable
                        }
                    )[:5],
                    "due_date_patterns": [str(action.due_date) for action in actions if action.due_date][:5],
                    "priority_patterns": [
                        {
                            "importance": action.importance,
                            "urgency": action.urgency,
                            "priority_index": action.priority_index,
                        }
                        for action in actions[:5]
                    ],
                }
            )

        return examples
    except Exception as exc:
        logger.info("IA Assistant relevant examples unavailable: %s", exc)
        return []


async def assistant_chat_service(
    payload: AssistantChatRequest,
    db: Session | None,
    directory_db,
) -> AssistantChatResponse:
    latest_message = get_latest_user_message(payload)

    if not latest_message:
        state = advance_assistant_state(
            normalize_assistant_state(payload.conversation_state, payload.scope)
        )

        return AssistantChatResponse(
            reply=get_question_for_step(state.current_step)
            or "Tell me what you want to change in this action plan.",
            state="collecting_info",
            conversation_state=state,
            summary=None,
            draft_id=None,
            draft=None,
        )

    if payload.conversation_state:
        previous_state = normalize_assistant_state(payload.conversation_state, payload.scope)
        current_step = previous_state.current_step
        conversation_state, extracted = update_state_from_message(previous_state, latest_message)
    else:
        current_step = "objective"
        conversation_state = rebuild_state_from_history(payload)
        extracted = conversation_state.model_dump()

    log_assistant_transition(current_step, extracted, conversation_state)
    question = get_question_for_step(conversation_state.current_step)

    if question:
        return AssistantChatResponse(
            reply=question,
            state="collecting_info",
            conversation_state=conversation_state,
            summary=None,
            draft_id=None,
            draft=None,
        )

    slots = assistant_state_to_slots(conversation_state)
    relevant_examples = get_relevant_action_plan_examples(conversation_state.objective, db)

    try:
        draft = await generate_action_plan_draft_service(
            AIActionPlanDraftRequest(
                prompt=conversation_state.objective or "Action plan",
                business_objective=conversation_state.objective,
                generation_context=build_assistant_prompt(
                    slots,
                    payload.inserted_by,
                    conversation_messages=payload.messages,
                    conversation_state=conversation_state,
                    relevant_examples=relevant_examples,
                ),
                inserted_by=payload.inserted_by,
                scope=payload.scope,
            ),
            directory_db,
            allow_fallback=True,
        )
    except HTTPException as exc:
        if exc.status_code == 503:
            return AssistantChatResponse(
                reply="AI generation failed and fallback generation was not available.",
                state="error",
                conversation_state=conversation_state,
                summary=None,
                draft_id=None,
                draft=None,
            )

        return AssistantChatResponse(
            reply=f"Draft validation failed: {exc.detail}",
            state="error",
            conversation_state=conversation_state,
            summary=None,
            draft_id=None,
            draft=None,
        )
    except Exception:
        logger.exception("IA Assistant draft generation failed after fallback.")
        return AssistantChatResponse(
            reply="Draft generation failed after fallback. Please simplify the request and try again.",
            state="error",
            conversation_state=conversation_state,
            summary=None,
            draft_id=None,
            draft=None,
        )

    return AssistantChatResponse(
        reply="Here is the proposed action plan summary. Do you want me to create it?",
        state="ready_to_create",
        conversation_state=conversation_state,
        summary=build_assistant_summary(draft, slots),
        draft_id=None,
        draft=draft,
    )


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
    plan = sanitize_plan_or_fallback(plan, plan.plan_title, plan.inserted_by)
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


async def assistant_create_service(
    payload: AssistantCreateRequest,
    db: Session,
    directory_db,
):
    plan = payload.draft.model_copy(update={"inserted_by": payload.inserted_by})
    result = await create_action_plan_service(plan, db, directory_db)

    return {
        "created": True,
        "root_sujet_id": result.get("root_sujet_id"),
        "root_sujet_ids": result.get("root_sujet_ids", []),
        "created_sujet_ids": result.get("created_sujet_ids", []),
        "created_action_ids": result.get("created_action_ids", []),
        "plan_title": result.get("plan_title"),
        "message": "Action plan created successfully.",
        "warnings": result.get("warnings", []),
    }
