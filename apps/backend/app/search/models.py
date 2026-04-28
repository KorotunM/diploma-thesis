from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class UniversitySearchHitRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university_id: UUID
    card_version: int
    canonical_name: str
    website_url: str | None = None
    website_domain: str | None = None
    country_code: str | None = None
    city_name: str | None = None
    aliases: list[str] = Field(default_factory=list)
    generated_at: datetime
    text_rank: float = 0.0
    trigram_score: float = 0.0
    combined_score: float = 0.0
    total_count: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)


class UniversitySearchResultItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university_id: UUID
    card_version: int
    canonical_name: str
    city: str | None = None
    country_code: str | None = None
    website: str | None = None
    aliases: list[str] = Field(default_factory=list)
    score: float
    match_signals: list[str] = Field(default_factory=list)


class UniversitySearchFilters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    city: str | None = None
    country: str | None = None
    source_type: str | None = None


class UniversitySearchResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str
    total: int
    page: int
    page_size: int
    has_more: bool
    filters: UniversitySearchFilters = Field(default_factory=UniversitySearchFilters)
    items: list[UniversitySearchResultItem] = Field(default_factory=list)
