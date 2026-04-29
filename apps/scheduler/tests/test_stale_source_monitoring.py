from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

from apps.scheduler.app.freshness import (
    FreshnessState,
    SourceFreshnessContext,
    SourceFreshnessService,
    StaleSourceMonitoringRunRequest,
    StaleSourceMonitoringService,
    StaleSourceReviewRequiredEmitter,
)
from apps.scheduler.app.sources.models import SourceTrustTier, SourceType


class FakeFreshnessRepository:
    def __init__(self, items: list[SourceFreshnessContext]) -> None:
        self.items_by_source_key = {item.source_key: item for item in items}
        self.merged_patches: list[tuple[str, dict]] = []

    def list_sources(self, *, include_inactive: bool):
        items = list(self.items_by_source_key.values())
        if include_inactive:
            return items
        return [item for item in items if item.is_active]

    def merge_metadata(self, *, source_key: str, metadata_patch: dict) -> None:
        self.merged_patches.append((source_key, metadata_patch))
        source = self.items_by_source_key[source_key]
        source.metadata = {**source.metadata, **metadata_patch}


class FakePublisher:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def publish(self, payload, *, queue_name: str, headers: dict | None = None):
        self.calls.append(
            {
                "payload": payload,
                "queue_name": queue_name,
                "headers": headers or {},
            }
        )
        return SimpleNamespace(
            queue_name=queue_name,
            exchange_name="delivery.events",
            routing_key="review.required",
        )


def build_context(
    *,
    source_key: str,
    last_observed_at: datetime | None,
    metadata: dict | None = None,
) -> SourceFreshnessContext:
    return SourceFreshnessContext(
        source_id=uuid4(),
        source_key=source_key,
        source_type=SourceType.OFFICIAL_SITE,
        trust_tier=SourceTrustTier.AUTHORITATIVE,
        is_active=True,
        endpoint_count=1,
        scheduled_endpoint_count=1,
        refresh_interval_seconds=3600,
        endpoint_urls=["https://example.edu"],
        last_observed_at=last_observed_at,
        last_attempted_at=last_observed_at,
        metadata=metadata or {},
    )


def test_stale_source_monitoring_emits_review_required_once_per_stale_transition() -> None:
    stale_observed_at = datetime(2026, 4, 29, 8, 0, tzinfo=UTC)
    repository = FakeFreshnessRepository(
        [
            build_context(
                source_key="msu-official",
                last_observed_at=stale_observed_at,
                metadata={"freshness_monitor": {"freshness_state": FreshnessState.AGING.value}},
            )
        ]
    )
    publisher = FakePublisher()
    service = StaleSourceMonitoringService(
        repository=repository,
        freshness_service=SourceFreshnessService(repository=repository),
        emitter=StaleSourceReviewRequiredEmitter(publisher=publisher),
    )

    first = service.run(StaleSourceMonitoringRunRequest())
    second = service.run(StaleSourceMonitoringRunRequest())

    assert first.stale_sources == 1
    assert first.emitted_review_required_count == 1
    assert first.metadata_updated_count == 1
    assert second.stale_sources == 1
    assert second.emitted_review_required_count == 0
    assert second.metadata_updated_count == 1
    assert len(publisher.calls) == 1
    assert publisher.calls[0]["queue_name"] == "review.required"
    assert publisher.calls[0]["payload"]["payload"]["reason"] == "stale_source_monitoring"
    assert repository.items_by_source_key["msu-official"].metadata["freshness_state"] == "stale"
    freshness_monitor = repository.items_by_source_key["msu-official"].metadata["freshness_monitor"]
    assert "last_review_required_emitted_at" in freshness_monitor


def test_stale_source_monitoring_skips_manual_and_inactive_sources() -> None:
    checked_at = datetime.now(UTC)
    repository = FakeFreshnessRepository(
        [
            SourceFreshnessContext(
                source_id=uuid4(),
                source_key="manual-source",
                source_type=SourceType.AGGREGATOR,
                trust_tier=SourceTrustTier.TRUSTED,
                is_active=True,
                endpoint_count=1,
                scheduled_endpoint_count=0,
                refresh_interval_seconds=None,
                endpoint_urls=["https://example.edu"],
                last_observed_at=None,
                last_attempted_at=None,
                metadata={},
            ),
            SourceFreshnessContext(
                source_id=uuid4(),
                source_key="inactive-source",
                source_type=SourceType.RANKING,
                trust_tier=SourceTrustTier.AUXILIARY,
                is_active=False,
                endpoint_count=1,
                scheduled_endpoint_count=1,
                refresh_interval_seconds=3600,
                endpoint_urls=["https://example.edu/ranking"],
                last_observed_at=checked_at - timedelta(days=2),
                last_attempted_at=checked_at - timedelta(days=2),
                metadata={},
            ),
        ]
    )
    publisher = FakePublisher()
    service = StaleSourceMonitoringService(
        repository=repository,
        freshness_service=SourceFreshnessService(repository=repository),
        emitter=StaleSourceReviewRequiredEmitter(publisher=publisher),
    )

    response = service.run(StaleSourceMonitoringRunRequest())
    states = {item.source_key: item.freshness_state for item in response.items}

    assert response.emitted_review_required_count == 0
    assert states["manual-source"] == FreshnessState.MANUAL
    assert states["inactive-source"] == FreshnessState.INACTIVE
    assert publisher.calls == []
