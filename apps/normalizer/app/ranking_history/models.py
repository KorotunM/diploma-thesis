from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class RankingHistoryRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university_id: UUID | None
    source_key: str
    provider: str
    year: int
    rank: int
    score: float | None = None
    category: str | None = None
    change_direction: str | None = None
    change_delta: int = 0
    canonical_name: str
    external_id: str | None = None
    captured_at: datetime | None = None
