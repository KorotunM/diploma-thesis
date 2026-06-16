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

    # The text search box accepts ONLY a university name or abbreviation (e.g. "МГУ").
    query: str | None = None
    city: str | None = None
    region: str | None = None
    country: str | None = None
    source_type: str | None = None
    # A study direction maps to program codes, NOT to the text query.
    direction: str | None = None
    program_codes: list[str] = Field(default_factory=list)
    study_form: str | None = None
    budget_type: str | None = None
    min_ege_score: int | None = None
    # Filters that map to the "hidden"/advanced search panel on the frontend.
    ege_subjects: list[str] = Field(default_factory=list)
    ege_scores: dict[str, int] = Field(default_factory=dict)
    dormitory: bool | None = None
    military_department: bool | None = None
    sort_by: str | None = None
    advanced: AiChatAdvancedFilters = Field(default_factory=AiChatAdvancedFilters)


class AiChatUniversity(BaseModel):
    """A concrete university the assistant can offer with a link to its card."""

    model_config = ConfigDict(extra="ignore")

    university_id: str
    name: str
    full_name: str | None = None
    city: str | None = None
    score: float | None = None


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
    # Short quick-reply options offered to the user (used for clarifying questions).
    suggestions: list[str] = Field(default_factory=list, max_length=4)
    # Concrete universities matching the plan, so the user can jump straight to a card.
    universities: list[AiChatUniversity] = Field(default_factory=list)
    confidence: float = Field(default=0.7, ge=0, le=1)
    model_used: str | None = None
    trial_remaining: int | None = None
