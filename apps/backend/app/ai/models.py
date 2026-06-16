from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class AiChatAdvancedFilters(BaseModel):
    model_config = ConfigDict(extra="ignore")

    min_rating: float | None = None
    min_budget_places: int | None = None
    max_passing_score: int | None = None
    dormitory: bool | None = None
    university_type: str | None = None
    program_query: str | None = None


class AiChatFilters(BaseModel):
    model_config = ConfigDict(extra="ignore")

    query: str | None = None
    city: str | None = None
    country: str | None = None
    source_type: str | None = None
    direction: str | None = None
    study_form: str | None = None
    budget_type: str | None = None
    min_ege_score: int | None = None
    advanced: AiChatAdvancedFilters = Field(default_factory=AiChatAdvancedFilters)


class AiChatHistoryMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    role: Literal["user", "assistant"]
    content: str = Field(min_length=1, max_length=1200)


class AiChatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message: str = Field(min_length=1, max_length=500)
    history: list[AiChatHistoryMessage] = Field(default_factory=list, max_length=20)
    client_id: str | None = Field(default=None, max_length=120)


class AiChatResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    intent: Literal["search", "clarify", "general"] = "search"
    message_to_user: str = ""
    filters: AiChatFilters = Field(default_factory=AiChatFilters)
    missing_fields: list[str] = Field(default_factory=list)
    confidence: float = Field(default=0.7, ge=0, le=1)
    model_used: str | None = None
    trial_remaining: int | None = None
