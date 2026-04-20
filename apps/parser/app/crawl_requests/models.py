from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from apps.parser.app.raw_artifacts import RawArtifactRecord


class CrawlRequestProcessingResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    trace_id: UUID | None = None
    crawl_run_id: UUID
    source_key: str
    endpoint_url: str
    parser_profile: str
    raw_artifact: RawArtifactRecord
    processed_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)
