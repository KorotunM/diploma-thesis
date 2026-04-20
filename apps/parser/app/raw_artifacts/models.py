from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class RawArtifactRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    raw_artifact_id: UUID
    crawl_run_id: UUID
    source_key: str
    source_url: str
    final_url: str | None = None
    http_status: int | None = Field(default=None, ge=100, le=599)
    content_type: str
    content_length: int | None = Field(default=None, ge=0)
    sha256: str
    storage_bucket: str
    storage_object_key: str
    etag: str | None = None
    last_modified: str | None = None
    fetched_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)
