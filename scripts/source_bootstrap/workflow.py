from __future__ import annotations

import argparse
import json
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from typing import Any

from apps.scheduler.app.sources.endpoint_repository import SourceEndpointRepository
from apps.scheduler.app.sources.models import (
    CrawlPolicy,
    CreateSourceEndpointRequest,
    CreateSourceRequest,
    SourceTrustTier,
    SourceType,
    UpdateSourceEndpointRequest,
    UpdateSourceRequest,
)
from apps.scheduler.app.sources.repository import SourceRepository
from libs.source_catalog import EndpointBlueprint, SourceBlueprint, build_live_mvp_source_catalog
from libs.storage import get_postgres_session_factory


@dataclass(frozen=True, slots=True)
class LiveSourceSeedEndpointSpec:
    endpoint_url: str
    parser_profile: str
    crawl_policy: CrawlPolicy
    role: str
    content_kind: str
    implementation_status: str
    target_fields: tuple[str, ...] = ()
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "endpoint_url": self.endpoint_url,
            "parser_profile": self.parser_profile,
            "crawl_policy": self.crawl_policy.model_dump(mode="json"),
            "role": self.role,
            "content_kind": self.content_kind,
            "implementation_status": self.implementation_status,
            "target_fields": list(self.target_fields),
            "notes": self.notes,
        }


@dataclass(frozen=True, slots=True)
class LiveSourceSeedSourceSpec:
    source_key: str
    source_type: SourceType
    trust_tier: SourceTrustTier
    is_active: bool
    metadata: dict[str, Any]
    endpoints: tuple[LiveSourceSeedEndpointSpec, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_key": self.source_key,
            "source_type": self.source_type.value,
            "trust_tier": self.trust_tier.value,
            "is_active": self.is_active,
            "metadata": self.metadata,
            "endpoints": [endpoint.to_dict() for endpoint in self.endpoints],
        }


@dataclass(frozen=True, slots=True)
class LiveSourceSeedResult:
    source_count: int
    endpoint_count: int
    source_keys: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_count": self.source_count,
            "endpoint_count": self.endpoint_count,
            "source_keys": list(self.source_keys),
        }


def build_live_mvp_source_seed_specs() -> tuple[LiveSourceSeedSourceSpec, ...]:
    return tuple(
        _seed_source_spec_from_blueprint(blueprint)
        for blueprint in build_live_mvp_source_catalog()
    )


def _seed_source_spec_from_blueprint(
    blueprint: SourceBlueprint,
) -> LiveSourceSeedSourceSpec:
    concrete_endpoints = tuple(
        _seed_endpoint_spec_from_blueprint(endpoint)
        for endpoint in blueprint.endpoints
        if "<" not in endpoint.endpoint_url and ">" not in endpoint.endpoint_url
    )
    metadata = {
        "seed_kind": "live_mvp",
        "catalog_version": 1,
        "notes": blueprint.notes,
        "discovery_rules": [asdict(rule) for rule in blueprint.discovery_rules],
        "endpoint_blueprints": [asdict(endpoint) for endpoint in blueprint.endpoints],
        "endpoint_seed_specs": [
            _seed_endpoint_spec_from_blueprint(endpoint).to_dict()
            for endpoint in blueprint.endpoints
        ],
        "seeded_endpoint_count": len(concrete_endpoints),
    }
    return LiveSourceSeedSourceSpec(
        source_key=blueprint.source_key,
        source_type=SourceType(blueprint.source_type),
        trust_tier=SourceTrustTier(blueprint.trust_tier),
        is_active=True,
        metadata=metadata,
        endpoints=concrete_endpoints,
    )


def _seed_endpoint_spec_from_blueprint(
    blueprint: EndpointBlueprint,
) -> LiveSourceSeedEndpointSpec:
    return LiveSourceSeedEndpointSpec(
        endpoint_url=blueprint.endpoint_url,
        parser_profile=blueprint.parser_profile,
        crawl_policy=_crawl_policy_for_blueprint(blueprint),
        role=blueprint.role,
        content_kind=blueprint.content_kind,
        implementation_status=blueprint.implementation_status,
        target_fields=blueprint.target_fields,
        notes=blueprint.notes,
    )


def _crawl_policy_for_blueprint(blueprint: EndpointBlueprint) -> CrawlPolicy:
    request_headers: dict[str, str] = {}
    allowed_content_types: list[str]
    timeout_seconds = 45

    if blueprint.content_kind == "xml":
        allowed_content_types = ["application/xml", "text/xml"]
        timeout_seconds = 60
        request_headers["accept"] = "application/xml, text/xml;q=0.9, */*;q=0.1"
    elif blueprint.content_kind == "pdf":
        allowed_content_types = ["application/pdf"]
        timeout_seconds = 90
        request_headers["accept"] = "application/pdf, */*;q=0.1"
    elif blueprint.content_kind == "html":
        allowed_content_types = ["text/html"]
        request_headers["accept"] = "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1"
    else:
        allowed_content_types = ["application/json"]
        request_headers["accept"] = "application/json, */*;q=0.1"

    return CrawlPolicy(
        schedule_enabled=True,
        interval_seconds=86400,
        timeout_seconds=timeout_seconds,
        max_retries=2,
        retry_backoff_seconds=120,
        render_mode="http",
        respect_robots_txt=True,
        allowed_content_types=allowed_content_types,
        request_headers=request_headers,
    )


class SchedulerSourceRegistryGateway:
    def __init__(
        self,
        *,
        session: Any,
        source_repository: SourceRepository,
        endpoint_repository: SourceEndpointRepository,
    ) -> None:
        self._session = session
        self._source_repository = source_repository
        self._endpoint_repository = endpoint_repository

    def ensure_source(self, spec: LiveSourceSeedSourceSpec) -> None:
        existing = self._source_repository.get_by_key(spec.source_key)
        if existing is None:
            self._source_repository.create(
                CreateSourceRequest(
                    source_key=spec.source_key,
                    source_type=spec.source_type,
                    trust_tier=spec.trust_tier,
                    is_active=spec.is_active,
                    metadata=spec.metadata,
                )
            )
            return

        self._source_repository.update(
            spec.source_key,
            UpdateSourceRequest(
                source_type=spec.source_type,
                trust_tier=spec.trust_tier,
                is_active=spec.is_active,
                metadata={
                    **existing.metadata,
                    **spec.metadata,
                },
            ),
        )

    def ensure_endpoint(
        self,
        *,
        source_key: str,
        spec: LiveSourceSeedEndpointSpec,
    ) -> None:
        existing = self._endpoint_repository.get_by_url(source_key, spec.endpoint_url.rstrip("/"))
        if existing is None:
            self._endpoint_repository.create(
                source_key,
                CreateSourceEndpointRequest(
                    endpoint_url=spec.endpoint_url,
                    parser_profile=spec.parser_profile,
                    crawl_policy=spec.crawl_policy,
                ),
            )
            return

        self._endpoint_repository.update(
            source_key,
            existing.endpoint_id,
            UpdateSourceEndpointRequest(
                endpoint_url=spec.endpoint_url,
                parser_profile=spec.parser_profile,
                crawl_policy=spec.crawl_policy,
            ),
        )

    def commit(self) -> None:
        self._session.commit()


class LiveSourceSeedService:
    def __init__(
        self,
        *,
        gateway: SchedulerSourceRegistryGateway,
        seed_specs: tuple[LiveSourceSeedSourceSpec, ...],
    ) -> None:
        self._gateway = gateway
        self._seed_specs = seed_specs

    def bootstrap(self) -> LiveSourceSeedResult:
        endpoint_count = 0
        source_keys: list[str] = []
        for source_spec in self._seed_specs:
            self._gateway.ensure_source(source_spec)
            source_keys.append(source_spec.source_key)
            for endpoint_spec in source_spec.endpoints:
                self._gateway.ensure_endpoint(
                    source_key=source_spec.source_key,
                    spec=endpoint_spec,
                )
                endpoint_count += 1
        self._gateway.commit()
        return LiveSourceSeedResult(
            source_count=len(self._seed_specs),
            endpoint_count=endpoint_count,
            source_keys=tuple(source_keys),
        )


def build_live_source_seed_service(session: Any) -> LiveSourceSeedService:
    return LiveSourceSeedService(
        gateway=SchedulerSourceRegistryGateway(
            session=session,
            source_repository=SourceRepository(session),
            endpoint_repository=SourceEndpointRepository(session),
        ),
        seed_specs=build_live_mvp_source_seed_specs(),
    )


@contextmanager
def managed_session(service_name: str):
    session_factory = get_postgres_session_factory(service_name=service_name)
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


def build_argument_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog="python -m scripts.source_bootstrap",
        description="Seed live MVP sources and endpoints into the scheduler registry.",
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    parser.parse_args(argv)
    with managed_session("scheduler") as session:
        result = build_live_source_seed_service(session).bootstrap()
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
    return 0
