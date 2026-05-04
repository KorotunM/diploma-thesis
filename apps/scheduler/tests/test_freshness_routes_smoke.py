from datetime import UTC, datetime
from uuid import uuid4

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
pytest.importorskip("prometheus_client")

from fastapi.testclient import TestClient

from apps.scheduler.app.freshness.models import (
    FreshnessOverviewResponse,
    FreshnessState,
    SourceFreshnessSnapshot,
    StaleSourceMonitoringJobResult,
    StaleSourceMonitoringRunResponse,
)
from apps.scheduler.app.freshness.routes import (
    get_source_freshness_service,
    get_stale_source_monitoring_service,
)
from apps.scheduler.app.main import app
from apps.scheduler.app.sources.models import SourceTrustTier, SourceType


class FakeFreshnessService:
    def build_overview(self, *, include_inactive: bool = True) -> FreshnessOverviewResponse:
        snapshot = SourceFreshnessSnapshot(
            source_id=uuid4(),
            source_key="msu-official",
            source_type=SourceType.OFFICIAL_SITE,
            trust_tier=SourceTrustTier.AUTHORITATIVE,
            is_active=True,
            endpoint_count=1,
            scheduled_endpoint_count=1,
            refresh_interval_seconds=3600,
            endpoint_urls=["https://example.edu"],
            last_observed_at=datetime(2026, 4, 29, 11, 30, tzinfo=UTC),
            last_attempted_at=datetime(2026, 4, 29, 11, 30, tzinfo=UTC),
            metadata={},
            freshness_state=FreshnessState.FRESH,
            freshness_reason="Observed 30m ago against 1h budget.",
            stale_since=None,
            checked_at=datetime(2026, 4, 29, 12, 0, tzinfo=UTC),
        )
        return FreshnessOverviewResponse(
            checked_at=datetime(2026, 4, 29, 12, 0, tzinfo=UTC),
            total_sources=1,
            active_sources=1,
            scheduled_sources=1,
            fresh_sources=1,
            aging_sources=0,
            stale_sources=0,
            policy_only_sources=0,
            inactive_sources=0,
            sources=[snapshot],
        )


class FakeMonitoringService:
    def run(self, request):
        return StaleSourceMonitoringRunResponse(
            monitor_run_id=request.monitor_run_id,
            checked_at=datetime(2026, 4, 29, 12, 0, tzinfo=UTC),
            total_sources=1,
            stale_sources=1,
            emitted_review_required_count=1,
            metadata_updated_count=1,
            items=[
                StaleSourceMonitoringJobResult(
                    source_key="msu-official",
                    freshness_state=FreshnessState.STALE,
                    emitted_review_required=True,
                    metadata_updated=True,
                    stale_since=datetime(2026, 4, 29, 12, 0, tzinfo=UTC),
                )
            ],
        )


def test_freshness_routes_return_server_side_snapshot_and_monitoring_job_summary(
    admin_auth_headers: dict[str, str],
) -> None:
    app.dependency_overrides[get_source_freshness_service] = lambda: FakeFreshnessService()
    app.dependency_overrides[get_stale_source_monitoring_service] = lambda: FakeMonitoringService()
    try:
        client = TestClient(app, headers=admin_auth_headers)
        overview_response = client.get("/admin/v1/freshness")
        monitoring_response = client.post("/admin/v1/freshness/monitor-jobs", json={})
    finally:
        app.dependency_overrides.clear()

    assert overview_response.status_code == 200
    assert overview_response.json()["fresh_sources"] == 1
    assert overview_response.json()["sources"][0]["source_key"] == "msu-official"

    assert monitoring_response.status_code == 202
    assert monitoring_response.json()["stale_sources"] == 1
    assert monitoring_response.json()["items"][0]["emitted_review_required"] is True
