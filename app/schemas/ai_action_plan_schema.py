from __future__ import annotations

import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class AIActionPlanDraftRequest(BaseModel):
    prompt: str = Field(..., min_length=1)
    inserted_by: str = Field(..., min_length=1)
    scope: Literal["my", "team"] = "my"
    business_objective: str | None = None
    generation_context: str | None = None
    responsible_display_name: str | None = None
    responsible_email: str | None = None
    responsible_type: Literal["person", "team", "unknown"] | None = None
    responsible_department: str | None = None

    @field_validator("prompt", "inserted_by")
    @classmethod
    def strip_required_text(cls, value: str):
        normalized = value.strip()

        if not normalized:
            raise ValueError("Value is required")

        return normalized

    @field_validator(
        "business_objective",
        "generation_context",
        "responsible_display_name",
        "responsible_email",
        "responsible_type",
        "responsible_department",
    )
    @classmethod
    def strip_optional_text(cls, value: str | None):
        if value is None:
            return None

        normalized = value.strip()
        return normalized or None


class ActionNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    titre: str = Field(..., min_length=1)
    description: str | None = None
    status: str = "open"
    priorite: int | None = None
    responsable: str | None = None
    email_responsable: str | None = None
    demandeur: str | None = None
    email_demandeur: str | None = None
    due_date: datetime.date | None = None
    ordre: int | None = None
    importance: str = "moyenne"
    urgency: str = "Flexible"
    escalation_level: int = 0
    priority_index: int | None = None
    type: str | None = None
    sub_actions: list["ActionNode"] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

    @field_validator("titre")
    @classmethod
    def strip_title(cls, value: str):
        normalized = value.strip()

        if not normalized:
            raise ValueError("Action title is required")

        return normalized


class SujetNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    titre: str = Field(..., min_length=1)
    code: str | None = None
    description: str | None = None
    sujets: list["SujetNode"] = Field(default_factory=list)
    actions: list[ActionNode] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

    @field_validator("titre")
    @classmethod
    def strip_title(cls, value: str):
        normalized = value.strip()

        if not normalized:
            raise ValueError("Sujet title is required")

        return normalized


class PlanV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal["1.0"] = "1.0"
    plan_title: str = Field(..., min_length=1)
    plan_code: str | None = None
    inserted_by: str = Field(..., min_length=1)
    sujets: list[SujetNode] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

    @field_validator("plan_title", "inserted_by")
    @classmethod
    def strip_required_text(cls, value: str):
        normalized = value.strip()

        if not normalized:
            raise ValueError("Value is required")

        return normalized

class AssistantMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str = Field(..., min_length=1)

    @field_validator("content")
    @classmethod
    def strip_content(cls, value: str):
        normalized = value.strip()

        if not normalized:
            raise ValueError("Message content is required")

        return normalized


class AssistantResponsibleCandidate(BaseModel):
    type: Literal["person", "team", "unknown"] = "person"
    display_name: str | None = None
    email: str | None = None
    department: str | None = None
    job_title: str | None = None
    site: str | None = None
    confidence: float = 0
    reason: str | None = None


class AssistantConversationState(BaseModel):
    objective: str | None = None
    responsible_team: str | None = None
    responsible_type: Literal["person", "team", "unknown"] | None = None
    responsible_display_name: str | None = None
    responsible_email: str | None = None
    responsible_department: str | None = None
    responsible_confidence: float | None = None
    responsible_candidates: list[AssistantResponsibleCandidate] = Field(default_factory=list)
    pending_responsible_query: str | None = None
    responsible_needs_confirmation: bool = False
    deadline: str | None = None
    include_subactions: bool | None = None
    include_monitoring: bool | None = None
    include_escalation: bool | None = None
    urgency: str | None = None
    scope: Literal["my", "team"] = "my"
    current_step: Literal[
        "objective",
        "responsible_team",
        "responsible_confirmation",
        "deadline",
        "subactions",
        "urgency",
        "ready_to_create",
    ] = "objective"


class AssistantChatRequest(BaseModel):
    messages: list[AssistantMessage] = Field(default_factory=list)
    inserted_by: str = Field(..., min_length=1)
    scope: Literal["my", "team"] = "my"
    conversation_state: AssistantConversationState | None = None

    @field_validator("inserted_by")
    @classmethod
    def strip_inserted_by(cls, value: str):
        normalized = value.strip()

        if not normalized:
            raise ValueError("Value is required")

        return normalized


class AssistantSummary(BaseModel):
    plan_title: str | None = None
    topics: list[str] = Field(default_factory=list)
    actions: list[str] = Field(default_factory=list)
    features: list[str] = Field(default_factory=list)
    actions_count: int = 0
    main_responsible: str | None = None
    main_responsible_email: str | None = None
    responsible_resolution_status: str | None = None
    deadline: str | None = None
    urgency: str | None = None
    sub_actions_included: bool = False


class AssistantChatResponse(BaseModel):
    reply: str
    state: Literal["collecting_info", "ready_to_create", "created", "error"]
    conversation_state: AssistantConversationState | None = None
    responsible_candidates: list[AssistantResponsibleCandidate] = Field(default_factory=list)
    summary: AssistantSummary | None = None
    draft_id: str | None = None
    draft: PlanV1 | None = None


class AssistantCreateRequest(BaseModel):
    draft: PlanV1
    inserted_by: str = Field(..., min_length=1)
    scope: Literal["my", "team"] = "my"

    @field_validator("inserted_by")
    @classmethod
    def strip_create_inserted_by(cls, value: str):
        normalized = value.strip()

        if not normalized:
            raise ValueError("Value is required")

        return normalized
