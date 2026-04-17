from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from libs.domain.provenance import ClaimEvidence


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Claim(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim_id: UUID = Field(default_factory=uuid4)
    field_name: str
    value: Any
    value_type: str
    entity_hint: str | None = None
    source_key: str
    parser_version: str
    normalizer_version: str | None = None
    parser_confidence: float = 1.0
    created_at: datetime = Field(default_factory=utc_now)
    evidence: list[ClaimEvidence] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ResolvedFact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resolved_fact_id: UUID = Field(default_factory=uuid4)
    university_id: UUID
    field_name: str
    value: Any
    fact_score: float
    resolution_policy: str
    selected_claim_ids: list[UUID] = Field(default_factory=list)
    card_version: int
    resolved_at: datetime = Field(default_factory=utc_now)
