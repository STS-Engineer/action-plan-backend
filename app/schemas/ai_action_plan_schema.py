from __future__ import annotations

import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class AIActionPlanDraftRequest(BaseModel):
    prompt: str = Field(..., min_length=1)
    inserted_by: str = Field(..., min_length=1)
    scope: Literal["my", "team"] = "my"

    @field_validator("prompt", "inserted_by")
    @classmethod
    def strip_required_text(cls, value: str):
        normalized = value.strip()

        if not normalized:
            raise ValueError("Value is required")

        return normalized


class ActionNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    titre: str = Field(..., min_length=1)
    description: str | None = None
    status: str = "open"
    priorite: int | None = None
    responsable: str | None = None
    email_responsable: str | None = None
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
