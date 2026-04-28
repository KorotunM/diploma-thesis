from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from apps.normalizer.app.search_docs import UniversitySearchDocRecord
from libs.domain.university import UniversityCard


class CardVersionRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university_id: UUID
    card_version: int
    normalizer_version: str
    generated_at: datetime


class CardProjectionRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university_id: UUID
    card_version: int
    card: UniversityCard
    generated_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class UniversityCardProjectionResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    card_version: CardVersionRecord
    projection: CardProjectionRecord
    search_doc: UniversitySearchDocRecord
