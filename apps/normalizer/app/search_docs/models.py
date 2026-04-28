from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class UniversitySearchDocRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university_id: UUID
    card_version: int
    canonical_name: str
    canonical_name_normalized: str
    website_url: str | None = None
    website_domain: str | None = None
    country_code: str | None = None
    city_name: str | None = None
    aliases: list[str] = Field(default_factory=list)
    search_document: dict[str, Any] = Field(default_factory=dict)
    generated_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)
