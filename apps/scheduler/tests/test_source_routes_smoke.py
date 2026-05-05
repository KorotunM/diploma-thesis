from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient

from apps.scheduler.app.main import app
from apps.scheduler.app.sources.models import (
    CrawlPolicy,
    SourceEndpointRecord,
    SourceRecord,
    SourceTrustTier,
    SourceType,
)
from apps.scheduler.app.sources.routes import (
    get_source_endpoint_repository,
    get_source_repository,
)


class FakeSourceRepository:
    def list(self, *, limit: int, offset: int, include_inactive: bool):
        return (
            [
                SourceRecord(
                    source_id=uuid4(),
                    source_key="tabiturient-aggregator",
                    source_type=SourceType.AGGREGATOR,
                    trust_tier=SourceTrustTier.TRUSTED,
                    is_active=True,
                    metadata={"seed_kind": "live_mvp"},
                )
            ],
            1,
        )


class FakeSourceEndpointRepository:
    def list(self, source_key: str, *, limit: int, offset: int):
        return (
            [
                SourceEndpointRecord(
                    endpoint_id=uuid4(),
                    source_id=uuid4(),
                    source_key=source_key,
                    endpoint_url="https://tabiturient.ru/map/sitemap.php",
                    parser_profile="aggregator.tabiturient.sitemap_xml",
                    crawl_policy=CrawlPolicy(
                        schedule_enabled=True,
                        interval_seconds=86400,
                        timeout_seconds=60,
                        allowed_content_types=["text/html"],
                        request_headers={"accept": "text/html"},
                    ),
                )
            ],
            1,
        )


def test_list_sources_route_serializes_source_records(
    admin_auth_headers: dict[str, str],
) -> None:
    app.dependency_overrides[get_source_repository] = lambda: FakeSourceRepository()
    try:
        with TestClient(app, headers=admin_auth_headers) as client:
            response = client.get("/admin/v1/sources?limit=50&offset=0&include_inactive=true")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["source_key"] == "tabiturient-aggregator"
    assert payload["items"][0]["source_type"] == "aggregator"
    assert payload["items"][0]["trust_tier"] == "trusted"


def test_list_source_endpoints_route_serializes_endpoint_records(
    admin_auth_headers: dict[str, str],
) -> None:
    app.dependency_overrides[get_source_endpoint_repository] = (
        lambda: FakeSourceEndpointRepository()
    )
    try:
        with TestClient(app, headers=admin_auth_headers) as client:
            response = client.get(
                "/admin/v1/sources/tabiturient-aggregator/endpoints?limit=50&offset=0"
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["source_key"] == "tabiturient-aggregator"
    assert payload["items"][0]["endpoint_url"] == "https://tabiturient.ru/map/sitemap.php"
    assert payload["items"][0]["parser_profile"] == "aggregator.tabiturient.sitemap_xml"
    assert payload["items"][0]["crawl_policy"]["schedule_enabled"] is True
    assert payload["items"][0]["crawl_policy"]["interval_seconds"] == 86400
