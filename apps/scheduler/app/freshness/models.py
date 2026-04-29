from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from apps.scheduler.app.sources.models import SourceTrustTier, SourceType


def utc_now() -> datetime:
    return datetime.now(UTC)


class FreshnessState(StrEnum):
    FRESH = "fresh"
    AGING = "aging"
    STALE = "stale"
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    UNKNOWN = "unknown"
    INACTIVE = "inactive"


class SourceFreshnessContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: UUID
    source_key: str
    source_type: SourceType
    trust_tier: SourceTrustTier
    is_active: bool
    endpoint_count: int = 0
    scheduled_endpoint_count: int = 0
    refresh_interval_seconds: int | None = None
    endpoint_urls: list[str] = Field(default_factory=list)
    last_observed_at: datetime | None = None
    last_attempted_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SourceFreshnessSnapshot(SourceFreshnessContext):
    freshness_state: FreshnessState
    freshness_reason: str
    stale_since: datetime | None = None
    checked_at: datetime


class FreshnessOverviewResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    checked_at: datetime
    total_sources: int
    active_sources: int
    scheduled_sources: int
    fresh_sources: int
    aging_sources: int
    stale_sources: int
    policy_only_sources: int
    inactive_sources: int
    sources: list[SourceFreshnessSnapshot]


class StaleSourceMonitoringRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    monitor_run_id: UUID = Field(default_factory=uuid4)
    dry_run: bool = False
    include_inactive: bool = True
    emit_review_required: bool = True


class StaleSourceMonitoringJobResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_key: str
    freshness_state: FreshnessState
    emitted_review_required: bool = False
    metadata_updated: bool = False
    stale_since: datetime | None = None


class StaleSourceMonitoringRunResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    monitor_run_id: UUID
    checked_at: datetime
    total_sources: int
    stale_sources: int
    emitted_review_required_count: int
    metadata_updated_count: int
    items: list[StaleSourceMonitoringJobResult]
