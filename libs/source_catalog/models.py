from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

ImplementationStatus = Literal["implemented", "raw_only", "planned"]


@dataclass(frozen=True, slots=True)
class DiscoveryRule:
    parent_endpoint_url: str
    child_parser_profile: str
    include_url_pattern: str
    exclude_url_patterns: tuple[str, ...] = ()
    allowed_hosts: tuple[str, ...] = ()
    notes: str | None = None


@dataclass(frozen=True, slots=True)
class EndpointBlueprint:
    endpoint_url: str
    parser_profile: str
    role: str
    content_kind: Literal["html", "json", "xml", "pdf"]
    implementation_status: ImplementationStatus
    target_fields: tuple[str, ...] = ()
    notes: str | None = None
    request_headers: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SourceBlueprint:
    source_key: str
    source_type: Literal["official_site", "aggregator", "ranking"]
    trust_tier: Literal["authoritative", "trusted", "auxiliary", "experimental"]
    endpoints: tuple[EndpointBlueprint, ...]
    discovery_rules: tuple[DiscoveryRule, ...] = ()
    notes: str | None = None
