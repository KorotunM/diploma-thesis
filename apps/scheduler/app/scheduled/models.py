from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from apps.scheduler.app.sources.models import CrawlPolicy, SOURCE_KEY_PATTERN


def utc_now() -> datetime:
    return datetime.now(UTC)


class ScheduledEndpointRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    endpoint_id: UUID
    source_id: UUID
    source_key: str = Field(pattern=SOURCE_KEY_PATTERN)
    endpoint_url: str
    parser_profile: str
    crawl_policy: CrawlPolicy
    last_observed_at: datetime | None = None
    last_attempted_at: datetime | None = None


class ScheduledCrawlSweepRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requested_at: datetime = Field(default_factory=utc_now)
    priority: Literal["high", "bulk"] = "bulk"
    limit: int = Field(default=100, ge=1, le=1000)
    dry_run: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class ScheduledCrawlJobResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_key: str = Field(pattern=SOURCE_KEY_PATTERN)
    endpoint_id: UUID
    endpoint_url: str
    crawl_run_id: UUID | None = None
    queue_name: str | None = None
    scheduled: bool
    due: bool
    reason: str


class ScheduledCrawlSweepResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requested_at: datetime
    scanned_endpoint_count: int
    due_endpoint_count: int
    scheduled_count: int
    items: list[ScheduledCrawlJobResult] = Field(default_factory=list)
