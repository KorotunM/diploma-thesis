from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from libs.domain.university import UniversityCard


class DeliveryUniversityCardRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university_id: UUID
    card_version: int
    card: UniversityCard
    generated_at: datetime


class ResolvedFactSelectionRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field_name: str
    resolution_policy: str
    fact_score: float
    selected_claim_ids: list[UUID] = Field(default_factory=list)
    selected_evidence_ids: list[UUID] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class UniversityCardFieldAttribution(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field_name: str
    source_key: str | None = None
    source_trust_tier: str | None = None
    source_urls: list[str] = Field(default_factory=list)
    selected_claim_ids: list[UUID] = Field(default_factory=list)
    selected_evidence_ids: list[UUID] = Field(default_factory=list)
    resolution_policy: str
    resolution_strategy: str | None = None
    rationale: str


class AdmissionContactsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    website: str | None = None
    emails: list[str] = Field(default_factory=list)
    phones: list[str] = Field(default_factory=list)
    field_attribution: dict[str, UniversityCardFieldAttribution] = Field(
        default_factory=dict
    )


class AdmissionProgramResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field_name: str
    faculty: str | None = None
    code: str | None = None
    name: str | None = None
    budget_places: int | None = None
    passing_score: int | None = None
    year: int | None = None
    confidence: float | None = None
    sources: list[dict[str, Any]] = Field(default_factory=list)
    field_attribution: UniversityCardFieldAttribution | None = None


class AdmissionSectionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contacts: AdmissionContactsResponse = Field(default_factory=AdmissionContactsResponse)
    programs: list[AdmissionProgramResponse] = Field(default_factory=list)


class UniversityCardSourceRationale(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_key: str
    trust_tier: str | None = None
    selected_fields: list[str] = Field(default_factory=list)
    source_urls: list[str] = Field(default_factory=list)
    rationale: str


class UniversityCardResponse(UniversityCard):
    admission: AdmissionSectionResponse = Field(default_factory=AdmissionSectionResponse)
    field_attribution: dict[str, UniversityCardFieldAttribution] = Field(
        default_factory=dict
    )
    source_rationale: list[UniversityCardSourceRationale] = Field(default_factory=list)
