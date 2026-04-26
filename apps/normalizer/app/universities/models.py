from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from apps.normalizer.app.claims import ClaimEvidenceRecord, ClaimRecord
from apps.normalizer.app.resolution import SourceTrustTier


class SourceAuthorityRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: UUID
    source_key: str
    source_type: str
    trust_tier: SourceTrustTier
    is_active: bool
    metadata: dict[str, Any] = Field(default_factory=dict)


class UniversityBootstrapCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university_id: UUID
    canonical_name: str
    canonical_domain: str | None = None
    country_code: str | None = None
    city_name: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class UniversityRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university_id: UUID
    canonical_name: str
    canonical_domain: str | None = None
    country_code: str | None = None
    city_name: str | None = None
    created_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class UniversityBootstrapResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: SourceAuthorityRecord
    university: UniversityRecord
    claims_used: list[ClaimRecord]
    evidence_used: list[ClaimEvidenceRecord] = Field(default_factory=list)
