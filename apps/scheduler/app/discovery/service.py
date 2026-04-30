from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

import httpx

from apps.parser.adapters.aggregators import TabiturientSitemapDiscovery
from apps.scheduler.app.sources.endpoint_repository import (
    SourceEndpointRepository,
    SourceNotFoundError,
)
from apps.scheduler.app.sources.models import (
    CrawlPolicy,
    CreateSourceEndpointRequest,
    SourceEndpointRecord,
    UpdateSourceEndpointRequest,
    UpdateSourceRequest,
)
from apps.scheduler.app.sources.repository import SourceRepository
from libs.source_sdk.fetchers.http import content_media_type

from .models import (
    DiscoveryMaterializationAction,
    DiscoveryMaterializationRequest,
    DiscoveryMaterializationResponse,
    DiscoveryMaterializationResultItem,
)


def utc_now() -> datetime:
    return datetime.now(UTC)


class SourceEndpointDiscoveryWorkflowError(ValueError):
    pass


class DiscoveryFetcher(Protocol):
    def fetch_bytes(
        self,
        *,
        url: str,
        timeout_seconds: int,
        request_headers: dict[str, str],
        allowed_content_types: list[str],
    ) -> bytes:
        """Fetch raw bytes for a discovery endpoint."""


class HttpDiscoveryFetcher:
    def __init__(self, *, client_factory=httpx.Client) -> None:
        self._client_factory = client_factory

    def fetch_bytes(
        self,
        *,
        url: str,
        timeout_seconds: int,
        request_headers: dict[str, str],
        allowed_content_types: list[str],
    ) -> bytes:
        headers = {"user-agent": "scheduler-discovery/0.1"}
        headers.update({name.lower(): value for name, value in request_headers.items()})
        with self._client_factory(
            follow_redirects=True,
            timeout=float(timeout_seconds),
            headers=headers,
        ) as client:
            response = client.get(url)
        content_type = response.headers.get("content-type", "application/octet-stream")
        _validate_content_type(content_type, allowed_content_types)
        return response.content


def _validate_content_type(content_type: str, allowed_content_types: list[str]) -> None:
    if not allowed_content_types:
        return
    media_type = content_media_type(content_type)
    allowed_media_types = {content_media_type(value) for value in allowed_content_types}
    if media_type not in allowed_media_types:
        raise SourceEndpointDiscoveryWorkflowError(
            f"Unsupported discovery content type '{content_type}'."
        )


@dataclass(frozen=True, slots=True)
class LoadedDiscoveryRule:
    parent_endpoint_url: str
    child_parser_profile: str
    include_url_pattern: str
    exclude_url_patterns: tuple[str, ...]
    child_crawl_policy: CrawlPolicy


class SourceEndpointDiscoveryService:
    def __init__(
        self,
        *,
        source_repository: SourceRepository,
        endpoint_repository: SourceEndpointRepository,
        fetcher: DiscoveryFetcher,
        sitemap_discovery: TabiturientSitemapDiscovery | None = None,
    ) -> None:
        self._source_repository = source_repository
        self._endpoint_repository = endpoint_repository
        self._fetcher = fetcher
        self._sitemap_discovery = sitemap_discovery or TabiturientSitemapDiscovery()

    def materialize_discovered_endpoints(
        self,
        request: DiscoveryMaterializationRequest,
    ) -> DiscoveryMaterializationResponse:
        source = self._source_repository.get_by_key(request.source_key)
        if source is None:
            raise SourceNotFoundError(request.source_key)

        discovery_rule = self._load_discovery_rule(source.metadata)
        parent_endpoint = self._endpoint_repository.get_by_url(
            request.source_key,
            discovery_rule.parent_endpoint_url.rstrip("/"),
        )
        if parent_endpoint is None:
            raise SourceEndpointDiscoveryWorkflowError(
                "Discovery parent endpoint is not registered for source "
                f"{request.source_key}: {discovery_rule.parent_endpoint_url}"
            )

        payload = self._fetcher.fetch_bytes(
            url=parent_endpoint.endpoint_url,
            timeout_seconds=parent_endpoint.crawl_policy.timeout_seconds,
            request_headers=parent_endpoint.crawl_policy.request_headers,
            allowed_content_types=parent_endpoint.crawl_policy.allowed_content_types,
        )
        discovered_pages = self._sitemap_discovery.discover(payload)
        if request.limit is not None:
            discovered_pages = discovered_pages[: request.limit]

        items: list[DiscoveryMaterializationResultItem] = []
        materialized_count = 0
        existing_count = 0
        for page in discovered_pages:
            existing = self._endpoint_repository.get_by_url(request.source_key, page.url)
            if existing is None:
                action = (
                    DiscoveryMaterializationAction.DRY_RUN
                    if request.dry_run
                    else DiscoveryMaterializationAction.CREATED
                )
                if not request.dry_run:
                    self._endpoint_repository.create(
                        request.source_key,
                        CreateSourceEndpointRequest(
                            endpoint_url=page.url,
                            parser_profile=discovery_rule.child_parser_profile,
                            crawl_policy=discovery_rule.child_crawl_policy,
                        ),
                    )
                    materialized_count += 1
            elif self._endpoint_requires_update(existing, discovery_rule):
                action = (
                    DiscoveryMaterializationAction.DRY_RUN
                    if request.dry_run
                    else DiscoveryMaterializationAction.UPDATED
                )
                if not request.dry_run:
                    self._endpoint_repository.update(
                        request.source_key,
                        existing.endpoint_id,
                        UpdateSourceEndpointRequest(
                            endpoint_url=page.url,
                            parser_profile=discovery_rule.child_parser_profile,
                            crawl_policy=discovery_rule.child_crawl_policy,
                        ),
                    )
                    materialized_count += 1
            else:
                action = DiscoveryMaterializationAction.EXISTING
                existing_count += 1

            items.append(
                DiscoveryMaterializationResultItem(
                    endpoint_url=page.url,
                    parser_profile=discovery_rule.child_parser_profile,
                    action=action,
                )
            )

        self._update_source_discovery_metadata(
            source_key=request.source_key,
            metadata=source.metadata,
            parent_endpoint=parent_endpoint,
            discovered_total=len(discovered_pages),
            materialized_count=materialized_count,
            existing_count=existing_count,
            dry_run=request.dry_run,
            fetched_at=utc_now(),
        )

        return DiscoveryMaterializationResponse(
            discovery_run_id=request.discovery_run_id,
            source_key=request.source_key,
            parent_endpoint_url=parent_endpoint.endpoint_url,
            fetched_at=utc_now(),
            dry_run=request.dry_run,
            discovered_total=len(discovered_pages),
            materialized_count=materialized_count,
            existing_count=existing_count,
            items=items,
        )

    @staticmethod
    def _endpoint_requires_update(
        existing: SourceEndpointRecord,
        discovery_rule: LoadedDiscoveryRule,
    ) -> bool:
        return (
            existing.parser_profile != discovery_rule.child_parser_profile
            or existing.crawl_policy != discovery_rule.child_crawl_policy
        )

    def _load_discovery_rule(self, metadata: dict[str, Any]) -> LoadedDiscoveryRule:
        rules = metadata.get("discovery_rules")
        if not isinstance(rules, list) or not rules:
            raise SourceEndpointDiscoveryWorkflowError(
                "Source metadata does not contain discovery_rules."
            )
        first_rule = rules[0]
        if not isinstance(first_rule, dict):
            raise SourceEndpointDiscoveryWorkflowError(
                "Source discovery rule payload must be an object."
            )
        parent_endpoint_url = _required_str(first_rule, "parent_endpoint_url")
        child_parser_profile = _required_str(first_rule, "child_parser_profile")
        include_url_pattern = _required_str(first_rule, "include_url_pattern")
        exclude_url_patterns = tuple(_string_list(first_rule.get("exclude_url_patterns")))

        try:
            re.compile(include_url_pattern)
            for value in exclude_url_patterns:
                re.compile(value)
        except re.error as exc:
            raise SourceEndpointDiscoveryWorkflowError(
                f"Invalid discovery regex in source metadata: {exc}"
            ) from exc

        endpoint_specs = metadata.get("endpoint_seed_specs")
        if not isinstance(endpoint_specs, list):
            raise SourceEndpointDiscoveryWorkflowError(
                "Source metadata does not contain endpoint_seed_specs."
            )
        child_spec = next(
            (
                value
                for value in endpoint_specs
                if isinstance(value, dict)
                and value.get("parser_profile") == child_parser_profile
            ),
            None,
        )
        if child_spec is None:
            raise SourceEndpointDiscoveryWorkflowError(
                "Could not resolve child endpoint seed spec for discovery parser profile "
                f"{child_parser_profile}."
            )
        crawl_policy_payload = child_spec.get("crawl_policy")
        if not isinstance(crawl_policy_payload, dict):
            raise SourceEndpointDiscoveryWorkflowError(
                "Child endpoint seed spec does not contain crawl_policy."
            )
        return LoadedDiscoveryRule(
            parent_endpoint_url=parent_endpoint_url,
            child_parser_profile=child_parser_profile,
            include_url_pattern=include_url_pattern,
            exclude_url_patterns=exclude_url_patterns,
            child_crawl_policy=CrawlPolicy.model_validate(crawl_policy_payload),
        )

    def _update_source_discovery_metadata(
        self,
        *,
        source_key: str,
        metadata: dict[str, Any],
        parent_endpoint: SourceEndpointRecord,
        discovered_total: int,
        materialized_count: int,
        existing_count: int,
        dry_run: bool,
        fetched_at: datetime,
    ) -> None:
        next_metadata = {
            **metadata,
            "endpoint_discovery": {
                **_dict_value(metadata.get("endpoint_discovery")),
                "last_materialization_run_at": fetched_at.astimezone(UTC).isoformat(),
                "last_parent_endpoint_url": parent_endpoint.endpoint_url,
                "last_discovered_total": discovered_total,
                "last_materialized_count": materialized_count,
                "last_existing_count": existing_count,
                "last_dry_run": dry_run,
            },
        }
        self._source_repository.update(
            source_key,
            UpdateSourceRequest(metadata=next_metadata),
        )


def _required_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise SourceEndpointDiscoveryWorkflowError(
            f"Source discovery metadata key '{key}' must be a non-empty string."
        )
    return value.strip()


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _dict_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
