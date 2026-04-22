from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ParsedDocumentSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    parsed_document_id: UUID
    crawl_run_id: UUID
    raw_artifact_id: UUID
    source_key: str
    parser_profile: str
    parser_version: str
    entity_type: str
    entity_hint: str | None = None
    parsed_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExtractedFragmentSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fragment_id: UUID
    parsed_document_id: UUID
    raw_artifact_id: UUID
    source_key: str
    field_name: str
    value: Any
    value_type: str
    locator: str | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ClaimRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim_id: UUID
    parsed_document_id: UUID
    source_key: str
    field_name: str
    value: Any
    value_type: str
    entity_hint: str | None = None
    parser_version: str
    normalizer_version: str
    parser_confidence: float = Field(ge=0.0, le=1.0)
    created_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class ClaimBuildResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    parsed_document: ParsedDocumentSnapshot
    claims: list[ClaimRecord]
