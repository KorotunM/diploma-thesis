from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ParsedDocumentRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    parsed_document_id: UUID
    crawl_run_id: UUID
    raw_artifact_id: UUID
    source_key: str
    parser_profile: str
    parser_version: str
    entity_type: str
    entity_hint: str | None = None
    extracted_fragment_count: int = Field(ge=0)
    parsed_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExtractedFragmentRecord(BaseModel):
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
