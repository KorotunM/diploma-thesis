from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class RankingItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rank: int
    trend: str  # "up" | "down" | "neutral"
    trend_delta: int = 0
    university_id: str
    canonical_name: str
    logo_url: str | None = None
    city: str | None = None
    region: str | None = None
    composite_score: float
    category: str  # "А+" | "А" | "B+" | "B" | "C"


class RankingsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total: int
    page: int
    page_size: int
    has_more: bool
    updated_at: datetime | None = None
    source_label: str = "Рейтинги из карточек вузов"
    source_names: list[str] = Field(default_factory=list)
    items: list[RankingItem]
