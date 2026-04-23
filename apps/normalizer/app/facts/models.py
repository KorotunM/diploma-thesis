from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from apps.normalizer.app.universities import UniversityRecord


class ResolvedFactCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resolved_fact_id: UUID
    university_id: UUID
    field_name: str
    value: Any
    value_type: str
    fact_score: float = Field(ge=0.0, le=1.0)
    resolution_policy: str
    card_version: int = Field(ge=1)
    selected_claim_ids: list[UUID] = Field(default_factory=list)
    selected_evidence_ids: list[UUID] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ResolvedFactRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resolved_fact_id: UUID
    university_id: UUID
    field_name: str
    value: Any
    value_type: str
    fact_score: float = Field(ge=0.0, le=1.0)
    resolution_policy: str
    selected_claim_ids: list[UUID] = Field(default_factory=list)
    selected_evidence_ids: list[UUID] = Field(default_factory=list)
    card_version: int = Field(ge=1)
    resolved_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class ResolvedFactBuildResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university: UniversityRecord
    facts: list[ResolvedFactRecord]
