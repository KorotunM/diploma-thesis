from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class FavoriteItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university_id: str
    created_at: datetime


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
