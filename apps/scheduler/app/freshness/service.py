from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from .emitter import StaleSourceReviewRequiredEmitter
from .models import (
    FreshnessOverviewResponse,
    FreshnessState,
    SourceFreshnessContext,
    SourceFreshnessSnapshot,
    StaleSourceMonitoringJobResult,
    StaleSourceMonitoringRunRequest,
    StaleSourceMonitoringRunResponse,
    utc_now,
)
from .repository import SourceFreshnessRepository

DEFAULT_FRESHNESS_INTERVAL_SECONDS = 24 * 60 * 60


class SourceFreshnessService:
    def __init__(self, *, repository: SourceFreshnessRepository) -> None:
        self._repository = repository

    def build_overview(self, *, include_inactive: bool = True) -> FreshnessOverviewResponse:
        checked_at = utc_now()
        snapshots = self.list_snapshots(
            include_inactive=include_inactive,
            checked_at=checked_at,
        )
        return FreshnessOverviewResponse(
            checked_at=checked_at,
            total_sources=len(snapshots),
            active_sources=sum(1 for item in snapshots if item.is_active),
            scheduled_sources=sum(1 for item in snapshots if item.scheduled_endpoint_count > 0),
            fresh_sources=sum(
                1 for item in snapshots if item.freshness_state == FreshnessState.FRESH
            ),
            aging_sources=sum(
                1 for item in snapshots if item.freshness_state == FreshnessState.AGING
            ),
            stale_sources=sum(
                1 for item in snapshots if item.freshness_state == FreshnessState.STALE
            ),
            policy_only_sources=sum(
                1
                for item in snapshots
                if item.freshness_state
                in {
                    FreshnessState.SCHEDULED,
                    FreshnessState.MANUAL,
                    FreshnessState.UNKNOWN,
                }
            ),
            inactive_sources=sum(
                1 for item in snapshots if item.freshness_state == FreshnessState.INACTIVE
            ),
            sources=snapshots,
        )

    def list_snapshots(
        self,
        *,
        include_inactive: bool = True,
        checked_at: datetime | None = None,
    ) -> list[SourceFreshnessSnapshot]:
        resolved_checked_at = checked_at or utc_now()
        snapshots = [
            self._build_snapshot(item, checked_at=resolved_checked_at)
            for item in self._repository.list_sources(include_inactive=include_inactive)
        ]
        return sorted(snapshots, key=_freshness_sort_key)

    def _build_snapshot(
        self,
        item: SourceFreshnessContext,
        *,
        checked_at: datetime,
    ) -> SourceFreshnessSnapshot:
        previous_monitor = _monitoring_metadata(item.metadata)
        freshness_state = self._resolve_state(item=item, checked_at=checked_at)
        stale_since = self._resolve_stale_since(
            previous_monitor=previous_monitor,
            freshness_state=freshness_state,
            checked_at=checked_at,
        )
        return SourceFreshnessSnapshot(
            **item.model_dump(),
            freshness_state=freshness_state,
            freshness_reason=self._describe_reason(
                item=item,
                freshness_state=freshness_state,
                checked_at=checked_at,
            ),
            stale_since=stale_since,
            checked_at=checked_at,
        )

    def _resolve_state(
        self,
        *,
        item: SourceFreshnessContext,
        checked_at: datetime,
    ) -> FreshnessState:
        if not item.is_active:
            return FreshnessState.INACTIVE

        if item.last_observed_at is not None:
            return _age_to_state(
                reference_time=item.last_observed_at,
                checked_at=checked_at,
                interval_seconds=item.refresh_interval_seconds,
            )

        if item.last_attempted_at is not None and item.scheduled_endpoint_count > 0:
            return _age_to_state(
                reference_time=item.last_attempted_at,
                checked_at=checked_at,
                interval_seconds=item.refresh_interval_seconds,
            )

        if item.scheduled_endpoint_count > 0:
            return FreshnessState.SCHEDULED
        if item.endpoint_count > 0:
            return FreshnessState.MANUAL
        return FreshnessState.UNKNOWN

    def _describe_reason(
        self,
        *,
        item: SourceFreshnessContext,
        freshness_state: FreshnessState,
        checked_at: datetime,
    ) -> str:
        if freshness_state in {
            FreshnessState.FRESH,
            FreshnessState.AGING,
            FreshnessState.STALE,
        }:
            if item.last_observed_at is not None:
                return (
                    f"Observed {_format_age(item.last_observed_at, checked_at)} "
                    f"against {_format_interval(item.refresh_interval_seconds)} budget."
                )
            if item.last_attempted_at is not None:
                return (
                    "No successful artifact yet; "
                    f"last crawl attempt {_format_age(item.last_attempted_at, checked_at)} "
                    f"against {_format_interval(item.refresh_interval_seconds)} budget."
                )
            return "Freshness state resolved without observed timestamps."
        if freshness_state == FreshnessState.SCHEDULED:
            return (
                "Scheduled source with no observed crawl artifact yet."
                if item.refresh_interval_seconds is None
                else (
                    f"Scheduled every {_format_interval(item.refresh_interval_seconds)} "
                    "but no observed crawl artifact yet."
                )
            )
        if freshness_state == FreshnessState.MANUAL:
            return "Manual-only source with no scheduled freshness budget."
        if freshness_state == FreshnessState.INACTIVE:
            return "Source is disabled and excluded from stale-source monitoring."
        return "No endpoints or observed crawl activity available yet."

    @staticmethod
    def _resolve_stale_since(
        *,
        previous_monitor: dict[str, Any],
        freshness_state: FreshnessState,
        checked_at: datetime,
    ) -> datetime | None:
        if freshness_state != FreshnessState.STALE:
            return None
        previous_state = previous_monitor.get("freshness_state")
        if previous_state == FreshnessState.STALE.value:
            parsed = _parse_datetime(previous_monitor.get("stale_since"))
            if parsed is not None:
                return parsed
        return checked_at


class StaleSourceMonitoringService:
    def __init__(
        self,
        *,
        repository: SourceFreshnessRepository,
        freshness_service: SourceFreshnessService,
        emitter: StaleSourceReviewRequiredEmitter,
    ) -> None:
        self._repository = repository
        self._freshness_service = freshness_service
        self._emitter = emitter

    def run(
        self,
        request: StaleSourceMonitoringRunRequest,
    ) -> StaleSourceMonitoringRunResponse:
        checked_at = utc_now()
        snapshots = self._freshness_service.list_snapshots(
            include_inactive=request.include_inactive,
            checked_at=checked_at,
        )
        items: list[StaleSourceMonitoringJobResult] = []
        emitted_count = 0
        metadata_updated_count = 0

        for snapshot in snapshots:
            emitted_review_required = False
            previous_monitor = _monitoring_metadata(snapshot.metadata)
            if request.emit_review_required and self._should_emit_review_required(snapshot):
                emission = self._emitter.emit(
                    source=snapshot,
                    trace_id=request.monitor_run_id,
                )
                emitted_review_required = True
                emitted_count += 1
                review_metadata = {
                    "last_review_required_emitted_at": checked_at.isoformat(),
                    "last_review_required_event_id": str(emission.event.header.event_id),
                    "last_review_required_reason": emission.event.payload.reason,
                }
            else:
                review_metadata = {}

            metadata_patch = _build_monitoring_metadata_patch(
                snapshot=snapshot,
                previous_monitoring_metadata=previous_monitor,
                review_metadata=review_metadata,
            )
            metadata_updated = not request.dry_run
            if metadata_updated:
                self._repository.merge_metadata(
                    source_key=snapshot.source_key,
                    metadata_patch=metadata_patch,
                )
                metadata_updated_count += 1

            items.append(
                StaleSourceMonitoringJobResult(
                    source_key=snapshot.source_key,
                    freshness_state=snapshot.freshness_state,
                    emitted_review_required=emitted_review_required,
                    metadata_updated=metadata_updated,
                    stale_since=snapshot.stale_since,
                )
            )

        return StaleSourceMonitoringRunResponse(
            monitor_run_id=request.monitor_run_id,
            checked_at=checked_at,
            total_sources=len(snapshots),
            stale_sources=sum(
                1 for snapshot in snapshots if snapshot.freshness_state == FreshnessState.STALE
            ),
            emitted_review_required_count=emitted_count,
            metadata_updated_count=metadata_updated_count,
            items=items,
        )

    @staticmethod
    def _should_emit_review_required(snapshot: SourceFreshnessSnapshot) -> bool:
        if snapshot.freshness_state != FreshnessState.STALE:
            return False
        if not snapshot.is_active or snapshot.scheduled_endpoint_count == 0:
            return False
        previous_monitor = _monitoring_metadata(snapshot.metadata)
        previous_state = previous_monitor.get("freshness_state")
        if previous_state != FreshnessState.STALE.value:
            return True
        return not previous_monitor.get("last_review_required_emitted_at")


def _age_to_state(
    *,
    reference_time: datetime,
    checked_at: datetime,
    interval_seconds: int | None,
) -> FreshnessState:
    interval = interval_seconds or DEFAULT_FRESHNESS_INTERVAL_SECONDS
    age_seconds = max(0.0, (checked_at - reference_time).total_seconds())
    if age_seconds <= interval * 1.25:
        return FreshnessState.FRESH
    if age_seconds <= interval * 2:
        return FreshnessState.AGING
    return FreshnessState.STALE


def _format_age(reference_time: datetime, checked_at: datetime) -> str:
    age_seconds = max(60, int((checked_at - reference_time).total_seconds()))
    if age_seconds < 3600:
        return f"{round(age_seconds / 60)}m ago"
    if age_seconds < 172800:
        return f"{round(age_seconds / 3600)}h ago"
    return f"{round(age_seconds / 86400)}d ago"


def _format_interval(interval_seconds: int | None) -> str:
    value = interval_seconds or DEFAULT_FRESHNESS_INTERVAL_SECONDS
    if value < 3600:
        return f"{round(value / 60)}m"
    if value < 86400:
        return f"{round(value / 3600)}h"
    return f"{round(value / 86400)}d"


def _freshness_sort_key(snapshot: SourceFreshnessSnapshot) -> tuple[int, str]:
    severity = {
        FreshnessState.STALE: 0,
        FreshnessState.AGING: 1,
        FreshnessState.UNKNOWN: 2,
        FreshnessState.MANUAL: 3,
        FreshnessState.SCHEDULED: 4,
        FreshnessState.FRESH: 5,
        FreshnessState.INACTIVE: 6,
    }
    return severity[snapshot.freshness_state], snapshot.source_key


def _build_monitoring_metadata_patch(
    *,
    snapshot: SourceFreshnessSnapshot,
    previous_monitoring_metadata: dict[str, Any],
    review_metadata: dict[str, Any],
) -> dict[str, Any]:
    carried_review_metadata = {
        key: value
        for key, value in previous_monitoring_metadata.items()
        if key.startswith("last_review_required_")
    }
    freshness_monitor = {
        **carried_review_metadata,
        "freshness_state": snapshot.freshness_state.value,
        "freshness_reason": snapshot.freshness_reason,
        "freshness_checked_at": snapshot.checked_at.isoformat(),
        "refresh_interval_seconds": snapshot.refresh_interval_seconds,
        "last_observed_at": _datetime_or_none(snapshot.last_observed_at),
        "last_attempted_at": _datetime_or_none(snapshot.last_attempted_at),
        "stale_since": _datetime_or_none(snapshot.stale_since),
        "endpoint_count": snapshot.endpoint_count,
        "scheduled_endpoint_count": snapshot.scheduled_endpoint_count,
        "endpoint_urls": snapshot.endpoint_urls,
        **review_metadata,
    }
    return {
        "freshness_monitor": freshness_monitor,
        "freshness_state": snapshot.freshness_state.value,
        "freshness_reason": snapshot.freshness_reason,
        "freshness_checked_at": snapshot.checked_at.isoformat(),
        "last_success_at": _datetime_or_none(snapshot.last_observed_at),
        "last_fetched_at": _datetime_or_none(snapshot.last_observed_at),
        "last_attempted_run_at": _datetime_or_none(snapshot.last_attempted_at),
    }


def _monitoring_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    value = metadata.get("freshness_monitor")
    if isinstance(value, dict):
        return value
    return {}


def _datetime_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed
