import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)

KNOWLEDGE_PATH = Path(__file__).resolve().parents[1] / "ai" / "ia_assistant_knowledge.json"
REQUIRED_SECTIONS = {
    "application_context",
    "recursive_model",
    "required_action_fields",
    "allowed_values",
    "default_values",
    "question_strategy",
    "examples",
}


def build_fallback_knowledge(error: str | None = None) -> dict[str, Any]:
    return {
        "available": False,
        "load_error": error or "Knowledge file unavailable.",
        "application_context": "AVOCarbon Action Plan organizes recursive topics and actions.",
        "recursive_model": {
            "sujet": "A topic or workstream that can contain child sujets and actions.",
            "action": "An executable task that can contain sub-actions.",
        },
        "required_action_fields": [
            "titre",
            "status",
            "responsable",
            "email_responsable",
            "due_date",
            "importance",
            "urgency",
            "type",
        ],
        "allowed_values": {
            "status": ["open", "blocked", "closed"],
            "urgency": ["Urgent", "Flexible", "Secondaire"],
            "importance": ["haute", "moyenne", "faible"],
        },
        "default_values": {
            "status": "open",
            "importance": "moyenne",
            "urgency": "Flexible",
        },
        "question_strategy": {
            "principles": [
                "Ask for missing objective, owner, deadline, sub-actions, and urgency.",
                "Do not show raw JSON to users.",
            ]
        },
        "examples": [],
    }


def validate_knowledge(data: dict[str, Any]) -> None:
    missing = sorted(section for section in REQUIRED_SECTIONS if section not in data)

    if missing:
        raise ValueError(f"Missing IA Assistant knowledge sections: {', '.join(missing)}")


@lru_cache(maxsize=1)
def get_ia_assistant_knowledge() -> dict[str, Any]:
    try:
        with KNOWLEDGE_PATH.open("r", encoding="utf-8") as file:
            data = json.load(file)

        validate_knowledge(data)
        data["available"] = True
        return data
    except Exception as exc:
        logger.warning("IA Assistant knowledge unavailable: %s", exc)
        return build_fallback_knowledge(str(exc))


def get_ia_assistant_prompt_context(max_chars: int = 6000) -> str:
    knowledge = get_ia_assistant_knowledge()
    summary = json.dumps(knowledge, ensure_ascii=True, indent=2, default=str)

    if len(summary) <= max_chars:
        return summary

    return summary[:max_chars] + "\n...truncated..."
