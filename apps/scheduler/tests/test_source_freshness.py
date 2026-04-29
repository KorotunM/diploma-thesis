from datetime import UTC, datetime, timedelta
from uuid import uuid4

from apps.scheduler.app.freshness import (
    FreshnessState,
    SourceFreshnessContext,
    SourceFreshnessService,
)
from apps.scheduler.app.sources.models import SourceTrustTier, SourceType


class FakeFreshnessRepository:
    def __init__(self, items: list[SourceFreshnessContext]) -> None:
        self.items = items

    def list_sources(self, *, include_inactive: bool):
        if include_inactive:
            return list(self.items)
        return [item for item in self.items if item.is_active]


def build_context(
    *,
    source_key: str,
    is_active: bool = True,
    endpoint_count: int = 1,
    scheduled_endpoint_count: int = 1,
    refresh_interval_seconds: int | None = 3600,
    last_observed_at: datetime | None = None,
    last_attempted_at: datetime | None = None,
    metadata: dict | None = None,
) -> SourceFreshnessContext:
    return SourceFreshnessContext(
        source_id=uuid4(),
        source_key=source_key,
        source_type=SourceType.OFFICIAL_SITE,
        trust_tier=SourceTrustTier.AUTHORITATIVE,
        is_active=is_active,
        endpoint_count=endpoint_count,
        scheduled_endpoint_count=scheduled_endpoint_count,
        refresh_interval_seconds=refresh_interval_seconds,
        endpoint_urls=["https://example.edu"],
        last_observed_at=last_observed_at,
        last_attempted_at=last_attempted_at,
        metadata=metadata or {},
    )


def test_source_freshness_service_builds_overview_from_artifacts_and_policy() -> None:
    checked_at = datetime.now(UTC)
    service = SourceFreshnessService(
        repository=FakeFreshnessRepository(
            [
                build_context(
                    source_key="fresh-source",
                    last_observed_at=checked_at - timedelta(minutes=30),
                ),
                build_context(
                    source_key="aging-source",
                    last_observed_at=checked_at - timedelta(minutes=80),
                ),
                build_context(
                    source_key="stale-source",
                    last_observed_at=checked_at - timedelta(hours=3),
                ),
                build_context(
                    source_key="scheduled-source",
                    last_observed_at=None,
                ),
                build_context(
                    source_key="manual-source",
                    scheduled_endpoint_count=0,
                    refresh_interval_seconds=None,
                    last_observed_at=None,
                ),
                build_context(
                    source_key="inactive-source",
                    is_active=False,
                    last_observed_at=checked_at - timedelta(days=3),
                ),
            ]
        )
    )

    overview = service.build_overview(include_inactive=True)
    states = {item.source_key: item.freshness_state for item in overview.sources}

    assert overview.total_sources == 6
    assert overview.fresh_sources == 1
    assert overview.aging_sources == 1
    assert overview.stale_sources == 1
    assert overview.policy_only_sources == 2
    assert overview.inactive_sources == 1
    assert states["fresh-source"] == FreshnessState.FRESH
    assert states["aging-source"] == FreshnessState.AGING
    assert states["stale-source"] == FreshnessState.STALE
    assert states["scheduled-source"] == FreshnessState.SCHEDULED
    assert states["manual-source"] == FreshnessState.MANUAL
    assert states["inactive-source"] == FreshnessState.INACTIVE


def test_source_freshness_service_marks_failed_scheduled_source_stale_from_last_attempt() -> None:
    checked_at = datetime.now(UTC)
    service = SourceFreshnessService(
        repository=FakeFreshnessRepository(
            [
                build_context(
                    source_key="failed-source",
                    last_observed_at=None,
                    last_attempted_at=checked_at - timedelta(hours=4),
                )
            ]
        )
    )

    snapshot = service.list_snapshots(checked_at=checked_at)[0]

    assert snapshot.freshness_state == FreshnessState.STALE
    assert snapshot.stale_since == checked_at
    assert "No successful artifact yet" in snapshot.freshness_reason
