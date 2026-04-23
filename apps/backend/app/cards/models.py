from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from libs.domain.university import UniversityCard


class DeliveryUniversityCardRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university_id: UUID
    card_version: int
    card: UniversityCard
    generated_at: datetime
