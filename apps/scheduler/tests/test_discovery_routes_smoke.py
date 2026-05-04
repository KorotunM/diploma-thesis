from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient

from apps.scheduler.app.discovery.models import (
    DiscoveryMaterializationAction,
    DiscoveryMaterializationResponse,
    DiscoveryMaterializationResultItem,
)
from apps.scheduler.app.discovery.routes import (
    get_source_endpoint_discovery_service,
)
from apps.scheduler.app.main import app


class FakeDiscoveryService:
    def materialize_discovered_endpoints(self, request):
        return DiscoveryMaterializationResponse(
            discovery_run_id=request.discovery_run_id or uuid4(),
            source_key=request.source_key,
            parent_endpoint_url="https://tabiturient.ru/map/sitemap.php",
            dry_run=request.dry_run,
            discovered_total=2,
            materialized_count=1,
            existing_count=1,
            items=[
                DiscoveryMaterializationResultItem(
                    endpoint_url="https://tabiturient.ru/vuzu/kubsu",
                    parser_profile="aggregator.tabiturient.university_html",
                    action=DiscoveryMaterializationAction.EXISTING,
                ),
                DiscoveryMaterializationResultItem(
                    endpoint_url="https://tabiturient.ru/vuzu/altgaki",
                    parser_profile="aggregator.tabiturient.university_html",
                    action=DiscoveryMaterializationAction.CREATED,
                ),
            ],
        )


def test_discovery_route_returns_materialization_summary(
    admin_auth_headers: dict[str, str],
) -> None:
    app.dependency_overrides[get_source_endpoint_discovery_service] = (
        lambda: FakeDiscoveryService()
    )
    try:
        with TestClient(app, headers=admin_auth_headers) as client:
            response = client.post(
                "/admin/v1/discovery/materialize-jobs",
                json={"source_key": "tabiturient-aggregator", "dry_run": False},
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 202
    payload = response.json()
    assert payload["source_key"] == "tabiturient-aggregator"
    assert payload["discovered_total"] == 2
    assert payload["materialized_count"] == 1
    assert payload["items"][1]["action"] == "created"
