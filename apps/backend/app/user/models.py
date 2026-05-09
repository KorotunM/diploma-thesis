from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class FavoriteItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university_id: str
    created_at: datetime
    card_version: int | None = None
    canonical_name: str | None = None
    city: str | None = None
    country_code: str | None = None
    website: str | None = None
    logo_url: str | None = None


class FavoritesResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[FavoriteItem]


class ComparisonItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university_id: str
    added_at: datetime


class ComparisonResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[ComparisonItem]


class SavedSearchCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    query: str = ""
    city: str | None = None
    country: str | None = None
    source_type: str | None = None
    page_size: int = Field(default=20, ge=1, le=50)


class SavedSearchItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    saved_search_id: UUID
    name: str
    query: str
    city: str | None = None
    country: str | None = None
    source_type: str | None = None
    page_size: int
    created_at: datetime
    updated_at: datetime


class SavedSearchesResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[SavedSearchItem]
