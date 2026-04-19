from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import AnyUrl, BaseModel, ConfigDict, Field, model_validator

SOURCE_KEY_PATTERN = r"^[a-z0-9][a-z0-9._-]{1,62}[a-z0-9]$"
PARSER_PROFILE_PATTERN = r"^[a-z0-9][a-z0-9._-]{1,126}[a-z0-9]$"


class SourceType(StrEnum):
    OFFICIAL_SITE = "official_site"
    AGGREGATOR = "aggregator"
    RANKING = "ranking"
    GOVERNMENT_REGISTRY = "government_registry"
    MANUAL_SEED = "manual_seed"


class SourceTrustTier(StrEnum):
    AUTHORITATIVE = "authoritative"
    TRUSTED = "trusted"
    AUXILIARY = "auxiliary"
    EXPERIMENTAL = "experimental"


class SourceRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: UUID
    source_key: str = Field(pattern=SOURCE_KEY_PATTERN)
    source_type: SourceType
    trust_tier: SourceTrustTier
    is_active: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


class CrawlPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schedule_enabled: bool = False
    interval_seconds: int | None = Field(default=None, ge=300)
    timeout_seconds: int = Field(default=30, ge=1, le=180)
    max_retries: int = Field(default=3, ge=0, le=10)
    retry_backoff_seconds: int = Field(default=60, ge=1, le=3600)
    render_mode: Literal["http", "browser", "auto"] = "http"
    respect_robots_txt: bool = True
    allowed_content_types: list[str] = Field(
        default_factory=lambda: ["text/html", "application/json"]
    )
    request_headers: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_schedule_interval(self) -> CrawlPolicy:
        if self.schedule_enabled and self.interval_seconds is None:
            raise ValueError("interval_seconds is required when schedule_enabled is true.")
        return self


class SourceEndpointRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    endpoint_id: UUID
    source_id: UUID
    source_key: str = Field(pattern=SOURCE_KEY_PATTERN)
    endpoint_url: str
    parser_profile: str = Field(pattern=PARSER_PROFILE_PATTERN)
    crawl_policy: CrawlPolicy = Field(default_factory=CrawlPolicy)


class CreateSourceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: UUID = Field(default_factory=uuid4)
    source_key: str = Field(pattern=SOURCE_KEY_PATTERN)
    source_type: SourceType
    trust_tier: SourceTrustTier
    is_active: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


class CreateSourceEndpointRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    endpoint_id: UUID = Field(default_factory=uuid4)
    endpoint_url: AnyUrl
    parser_profile: str = Field(default="default", pattern=PARSER_PROFILE_PATTERN)
    crawl_policy: CrawlPolicy = Field(default_factory=CrawlPolicy)

    @property
    def normalized_endpoint_url(self) -> str:
        return str(self.endpoint_url).rstrip("/")


class UpdateSourceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_type: SourceType | None = None
    trust_tier: SourceTrustTier | None = None
    is_active: bool | None = None
    metadata: dict[str, Any] | None = None

    @model_validator(mode="after")
    def require_at_least_one_field(self) -> UpdateSourceRequest:
        if not self.model_fields_set:
            raise ValueError("At least one source field must be provided for update.")
        return self


class UpdateSourceEndpointRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    endpoint_url: AnyUrl | None = None
    parser_profile: str | None = Field(default=None, pattern=PARSER_PROFILE_PATTERN)
    crawl_policy: CrawlPolicy | None = None

    @model_validator(mode="after")
    def require_at_least_one_field(self) -> UpdateSourceEndpointRequest:
        if not self.model_fields_set:
            raise ValueError("At least one endpoint field must be provided for update.")
        return self

    @property
    def normalized_endpoint_url(self) -> str | None:
        if self.endpoint_url is None:
            return None
        return str(self.endpoint_url).rstrip("/")


class SourceResponse(SourceRecord):
    pass


class SourceEndpointResponse(SourceEndpointRecord):
    pass


class SourceListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total: int
    limit: int
    offset: int
    items: list[SourceResponse]


class SourceEndpointListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total: int
    limit: int
    offset: int
    items: list[SourceEndpointResponse]
