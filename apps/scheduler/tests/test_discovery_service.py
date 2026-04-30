from __future__ import annotations

from uuid import UUID, uuid4

from apps.scheduler.app.discovery.models import DiscoveryMaterializationRequest
from apps.scheduler.app.discovery.service import SourceEndpointDiscoveryService
from apps.scheduler.app.sources.models import (
    CrawlPolicy,
    SourceEndpointRecord,
    SourceRecord,
    SourceTrustTier,
    SourceType,
    UpdateSourceRequest,
)


class FakeSourceRepository:
    def __init__(self, source: SourceRecord | None) -> None:
        self.source = source
        self.updates: list[tuple[str, UpdateSourceRequest]] = []

    def get_by_key(self, source_key: str) -> SourceRecord | None:
        if self.source is None or self.source.source_key != source_key:
            return None
        return self.source

    def update(self, source_key: str, request: UpdateSourceRequest) -> SourceRecord | None:
        self.updates.append((source_key, request))
        if self.source is None or self.source.source_key != source_key:
            return None
        metadata = request.metadata if request.metadata is not None else self.source.metadata
        next_is_active = (
            self.source.is_active if request.is_active is None else request.is_active
        )
        self.source = self.source.model_copy(
            update={
                "source_type": request.source_type or self.source.source_type,
                "trust_tier": request.trust_tier or self.source.trust_tier,
                "is_active": next_is_active,
                "metadata": metadata,
            }
        )
        return self.source


class FakeEndpointRepository:
    def __init__(self, source_key: str, endpoints: list[SourceEndpointRecord]) -> None:
        self.source_key = source_key
        self.endpoints = {endpoint.endpoint_url: endpoint for endpoint in endpoints}
        self.created: list[tuple[str, str, CrawlPolicy]] = []
        self.updated: list[tuple[str, str, CrawlPolicy]] = []

    def get_by_url(self, source_key: str, endpoint_url: str) -> SourceEndpointRecord | None:
        assert source_key == self.source_key
        return self.endpoints.get(endpoint_url)

    def create(self, source_key: str, request):
        assert source_key == self.source_key
        record = SourceEndpointRecord(
            endpoint_id=request.endpoint_id,
            source_id=uuid4(),
            source_key=source_key,
            endpoint_url=request.normalized_endpoint_url,
            parser_profile=request.parser_profile,
            crawl_policy=request.crawl_policy,
        )
        self.endpoints[record.endpoint_url] = record
        self.created.append((record.endpoint_url, record.parser_profile, record.crawl_policy))
        return record

    def update(self, source_key: str, endpoint_id: UUID, request):
        assert source_key == self.source_key
        existing = next(
            (
                endpoint
                for endpoint in self.endpoints.values()
                if endpoint.endpoint_id == endpoint_id
            ),
            None,
        )
        if existing is None:
            return None
        updated = existing.model_copy(
            update={
                "endpoint_url": request.normalized_endpoint_url or existing.endpoint_url,
                "parser_profile": request.parser_profile or existing.parser_profile,
                "crawl_policy": request.crawl_policy or existing.crawl_policy,
            }
        )
        self.endpoints.pop(existing.endpoint_url, None)
        self.endpoints[updated.endpoint_url] = updated
        self.updated.append((updated.endpoint_url, updated.parser_profile, updated.crawl_policy))
        return updated


class FakeFetcher:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.calls: list[tuple[str, int, dict[str, str], list[str]]] = []

    def fetch_bytes(
        self,
        *,
        url: str,
        timeout_seconds: int,
        request_headers: dict[str, str],
        allowed_content_types: list[str],
    ) -> bytes:
        self.calls.append((url, timeout_seconds, request_headers, allowed_content_types))
        return self.payload


def build_tabiturient_source() -> SourceRecord:
    return SourceRecord(
        source_id=uuid4(),
        source_key="tabiturient-aggregator",
        source_type=SourceType.AGGREGATOR,
        trust_tier=SourceTrustTier.TRUSTED,
        is_active=True,
        metadata={
            "discovery_rules": [
                {
                    "parent_endpoint_url": "https://tabiturient.ru/map/sitemap.php",
                    "child_parser_profile": "aggregator.tabiturient.university_html",
                    "include_url_pattern": r"^https://tabiturient\.ru/vuzu/[a-z0-9_-]+/?$",
                    "exclude_url_patterns": [
                        r"^https://tabiturient\.ru/vuzu/[a-z0-9_-]+/(about|proxodnoi|dod|obsh)/?$",
                        r"^https://tabiturient\.ru/vuzu/[a-z0-9_-]+/.+\?.+$",
                    ],
                }
            ],
            "endpoint_seed_specs": [
                {
                    "endpoint_url": "https://tabiturient.ru/map/sitemap.php",
                    "parser_profile": "aggregator.tabiturient.sitemap_xml",
                    "crawl_policy": {
                        "schedule_enabled": True,
                        "interval_seconds": 86400,
                        "timeout_seconds": 60,
                        "max_retries": 2,
                        "retry_backoff_seconds": 120,
                        "render_mode": "http",
                        "respect_robots_txt": True,
                        "allowed_content_types": ["application/xml", "text/xml"],
                        "request_headers": {
                            "accept": "application/xml, text/xml;q=0.9, */*;q=0.1"
                        },
                    },
                },
                {
                    "endpoint_url": "https://tabiturient.ru/vuzu/<slug>",
                    "parser_profile": "aggregator.tabiturient.university_html",
                    "crawl_policy": {
                        "schedule_enabled": True,
                        "interval_seconds": 86400,
                        "timeout_seconds": 45,
                        "max_retries": 2,
                        "retry_backoff_seconds": 120,
                        "render_mode": "http",
                        "respect_robots_txt": True,
                        "allowed_content_types": ["text/html"],
                        "request_headers": {
                            "accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1"
                        },
                    },
                },
            ],
        },
    )


def build_sitemap_parent_endpoint() -> SourceEndpointRecord:
    return SourceEndpointRecord(
        endpoint_id=uuid4(),
        source_id=uuid4(),
        source_key="tabiturient-aggregator",
        endpoint_url="https://tabiturient.ru/map/sitemap.php",
        parser_profile="aggregator.tabiturient.sitemap_xml",
        crawl_policy=CrawlPolicy(
            schedule_enabled=True,
            interval_seconds=86400,
            timeout_seconds=60,
            max_retries=2,
            retry_backoff_seconds=120,
            render_mode="http",
            respect_robots_txt=True,
            allowed_content_types=["application/xml", "text/xml"],
            request_headers={"accept": "application/xml, text/xml;q=0.9, */*;q=0.1"},
        ),
    )


def test_source_endpoint_discovery_service_materializes_primary_tabiturient_pages_only() -> None:
    source = build_tabiturient_source()
    parent_endpoint = build_sitemap_parent_endpoint()
    existing_child = SourceEndpointRecord(
        endpoint_id=uuid4(),
        source_id=parent_endpoint.source_id,
        source_key="tabiturient-aggregator",
        endpoint_url="https://tabiturient.ru/vuzu/kubsu",
        parser_profile="aggregator.tabiturient.university_html",
        crawl_policy=CrawlPolicy(
            schedule_enabled=True,
            interval_seconds=86400,
            timeout_seconds=45,
            max_retries=2,
            retry_backoff_seconds=120,
            render_mode="http",
            respect_robots_txt=True,
            allowed_content_types=["text/html"],
            request_headers={"accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1"},
        ),
    )
    repository = FakeEndpointRepository(
        "tabiturient-aggregator",
        [parent_endpoint, existing_child],
    )
    fetcher = FakeFetcher(
        b"""
        <urlset>
          <url><loc>https://tabiturient.ru/vuzu/kubsu</loc></url>
          <url><loc>https://tabiturient.ru/vuzu/altgaki</loc></url>
          <url><loc>https://tabiturient.ru/vuzu/altgaki/about</loc></url>
          <url><loc>https://tabiturient.ru/vuzu/altgaki/proxodnoi</loc></url>
        </urlset>
        """
    )
    service = SourceEndpointDiscoveryService(
        source_repository=FakeSourceRepository(source),
        endpoint_repository=repository,
        fetcher=fetcher,
    )

    response = service.materialize_discovered_endpoints(
        DiscoveryMaterializationRequest(source_key="tabiturient-aggregator")
    )

    assert response.discovered_total == 2
    assert response.materialized_count == 1
    assert response.existing_count == 1
    assert [item.endpoint_url for item in response.items] == [
        "https://tabiturient.ru/vuzu/kubsu",
        "https://tabiturient.ru/vuzu/altgaki",
    ]
    assert [item.action.value for item in response.items] == ["existing", "created"]
    assert repository.created[0][0] == "https://tabiturient.ru/vuzu/altgaki"
    assert repository.created[0][1] == "aggregator.tabiturient.university_html"
    assert fetcher.calls[0][0] == "https://tabiturient.ru/map/sitemap.php"


def test_source_endpoint_discovery_service_supports_dry_run_without_writes() -> None:
    source = build_tabiturient_source()
    parent_endpoint = build_sitemap_parent_endpoint()
    repository = FakeEndpointRepository("tabiturient-aggregator", [parent_endpoint])
    service = SourceEndpointDiscoveryService(
        source_repository=FakeSourceRepository(source),
        endpoint_repository=repository,
        fetcher=FakeFetcher(
            b"<urlset><url><loc>https://tabiturient.ru/vuzu/altgaki</loc></url></urlset>"
        ),
    )

    response = service.materialize_discovered_endpoints(
        DiscoveryMaterializationRequest(
            source_key="tabiturient-aggregator",
            dry_run=True,
        )
    )

    assert response.discovered_total == 1
    assert response.materialized_count == 0
    assert response.items[0].action.value == "dry_run"
    assert repository.created == []
