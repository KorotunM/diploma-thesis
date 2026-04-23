from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from libs.domain.university import UniversityCard


class DeliveryProjectionTrace(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university_id: UUID
    card_version: int
    card: UniversityCard
    projection_generated_at: datetime
    card_generated_at: datetime | None = None
    normalizer_version: str | None = None


class ResolvedFactTrace(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resolved_fact_id: UUID
    university_id: UUID
    field_name: str
    value: Any
    value_type: str
    fact_score: float
    resolution_policy: str
    selected_claim_ids: list[UUID] = Field(default_factory=list)
    selected_evidence_ids: list[UUID] = Field(default_factory=list)
    card_version: int
    resolved_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class ClaimTrace(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim_id: UUID
    parsed_document_id: UUID
    source_key: str
    field_name: str
    value: Any
    value_type: str
    entity_hint: str | None = None
    parser_version: str
    normalizer_version: str | None = None
    parser_confidence: float
    created_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class ClaimEvidenceTrace(BaseModel):
    model_config = ConfigDict(extra="forbid")

    evidence_id: UUID
    claim_id: UUID
    raw_artifact_id: UUID
    fragment_id: UUID | None = None
    source_key: str
    source_url: str
    captured_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class ParsedDocumentTrace(BaseModel):
    model_config = ConfigDict(extra="forbid")

    parsed_document_id: UUID
    crawl_run_id: UUID
    raw_artifact_id: UUID
    source_key: str
    parser_profile: str
    parser_version: str
    entity_type: str
    entity_hint: str | None = None
    extracted_fragment_count: int
    parsed_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class RawArtifactTrace(BaseModel):
    model_config = ConfigDict(extra="forbid")

    raw_artifact_id: UUID
    crawl_run_id: UUID
    source_key: str
    source_url: str
    final_url: str | None = None
    http_status: int | None = None
    content_type: str
    content_length: int | None = None
    sha256: str
    storage_bucket: str
    storage_object_key: str
    etag: str | None = None
    last_modified: str | None = None
    fetched_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class UniversityProvenanceTrace(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university_id: UUID
    chain: list[str] = Field(
        default_factory=lambda: [
            "raw",
            "parsed",
            "claims",
            "resolved_facts",
            "card_version",
            "delivery_projection",
        ]
    )
    delivery_projection: DeliveryProjectionTrace
    resolved_facts: list[ResolvedFactTrace] = Field(default_factory=list)
    claims: list[ClaimTrace] = Field(default_factory=list)
    claim_evidence: list[ClaimEvidenceTrace] = Field(default_factory=list)
    parsed_documents: list[ParsedDocumentTrace] = Field(default_factory=list)
    raw_artifacts: list[RawArtifactTrace] = Field(default_factory=list)
