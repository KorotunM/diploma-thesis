from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from apps.scheduler.app.sources.models import SOURCE_KEY_PATTERN
from libs.contracts.events import CrawlRequestEvent


def utc_now() -> datetime:
    return datetime.now(UTC)


class PipelineRunType(StrEnum):
    CRAWL = "crawl"
    REPLAY = "replay"
    BACKFILL = "backfill"


class PipelineRunStatus(StrEnum):
    QUEUED = "queued"
    PUBLISHED = "published"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


class PipelineTriggerType(StrEnum):
    MANUAL = "manual"
    SCHEDULE = "schedule"
    REPLAY = "replay"


class PipelineRunRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: UUID
    run_type: PipelineRunType
    status: PipelineRunStatus
    trigger_type: PipelineTriggerType
    source_key: str | None = Field(default=None, pattern=SOURCE_KEY_PATTERN)
    started_at: datetime
    finished_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ManualCrawlTriggerRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    crawl_run_id: UUID = Field(default_factory=uuid4)
    source_key: str = Field(pattern=SOURCE_KEY_PATTERN)
    endpoint_id: UUID
    priority: Literal["high", "bulk"] = "bulk"
    requested_at: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


class PipelineRunResponse(PipelineRunRecord):
    pass


class CrawlJobAcceptedResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_run: PipelineRunResponse
    event: CrawlRequestEvent
