from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from apps.scheduler.app.sources.models import CrawlPolicy, SourceTrustTier, SourceType


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class MvpSourceSpec:
    fixture_id: str
    source_key: str
    source_type: SourceType
    trust_tier: SourceTrustTier
    endpoint_url: str
    parser_profile: str
    content_type: str
    fixture_file_name: str
    priority: Literal["high", "bulk"] = "bulk"
    crawl_policy: CrawlPolicy = field(default_factory=CrawlPolicy)
    metadata: dict[str, Any] | None = None


class FixtureBundleEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fixture_id: str
    source_key: str
    source_type: SourceType
    trust_tier: SourceTrustTier
    endpoint_url: str
    parser_profile: str
    content_type: str
    fixture_file: str
    priority: Literal["high", "bulk"] = "bulk"
    crawl_policy: CrawlPolicy = Field(default_factory=CrawlPolicy)
    raw_artifact_id: UUID
    crawl_run_id: UUID
    requested_at: datetime
    fetched_at: datetime
    sha256: str
    content_length: int = Field(ge=0)
    final_url: str | None = None
    http_status: int | None = Field(default=None, ge=100, le=599)
    etag: str | None = None
    last_modified: str | None = None
    response_headers: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def normalized_fixture_file(self) -> str:
        return self.fixture_file.replace("\\", "/")


class FixtureBundleManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bundle_name: str
    schema_version: int = 1
    captured_at: datetime = Field(default_factory=utc_now)
    entries: list[FixtureBundleEntry]

    @model_validator(mode="after")
    def validate_unique_fixture_ids(self) -> FixtureBundleManifest:
        fixture_ids = [entry.fixture_id for entry in self.entries]
        if len(fixture_ids) != len(set(fixture_ids)):
            raise ValueError("Fixture bundle entries must have unique fixture_id values.")
        return self

    def write(self, manifest_path: Path) -> None:
        manifest_path.write_text(
            json.dumps(
                self.model_dump(mode="json"),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    @classmethod
    def read(cls, manifest_path: Path) -> FixtureBundleManifest:
        return cls.model_validate_json(manifest_path.read_text(encoding="utf-8"))


DEFAULT_MVP_SOURCE_SPECS = (
    MvpSourceSpec(
        fixture_id="official",
        source_key="msu-official",
        source_type=SourceType.OFFICIAL_SITE,
        trust_tier=SourceTrustTier.AUTHORITATIVE,
        endpoint_url="https://example.edu/admissions",
        parser_profile="official_site.default",
        content_type="text/html; charset=utf-8",
        fixture_file_name="official_site_admissions.html",
        priority="high",
        crawl_policy=CrawlPolicy(
            timeout_seconds=45,
            max_retries=2,
            retry_backoff_seconds=30,
            render_mode="http",
            allowed_content_types=["text/html"],
            request_headers={"x-fixture-run": "mvp-capture"},
        ),
    ),
    MvpSourceSpec(
        fixture_id="aggregator",
        source_key="study-aggregator",
        source_type=SourceType.AGGREGATOR,
        trust_tier=SourceTrustTier.TRUSTED,
        endpoint_url="https://aggregator.example/universities/example-university",
        parser_profile="aggregator.default",
        content_type="application/json",
        fixture_file_name="aggregator_university_profile.json",
        priority="bulk",
        crawl_policy=CrawlPolicy(
            timeout_seconds=30,
            max_retries=2,
            retry_backoff_seconds=30,
            render_mode="http",
            allowed_content_types=["application/json"],
        ),
    ),
    MvpSourceSpec(
        fixture_id="ranking",
        source_key="qs-world-ranking",
        source_type=SourceType.RANKING,
        trust_tier=SourceTrustTier.TRUSTED,
        endpoint_url="https://rankings.example.com/universities/example-university",
        parser_profile="ranking.default",
        content_type="application/json",
        fixture_file_name="ranking_provider_university_profile.json",
        priority="bulk",
        crawl_policy=CrawlPolicy(
            timeout_seconds=30,
            max_retries=2,
            retry_backoff_seconds=30,
            render_mode="http",
            allowed_content_types=["application/json"],
        ),
    ),
)


def default_mvp_source_specs() -> tuple[MvpSourceSpec, ...]:
    return DEFAULT_MVP_SOURCE_SPECS


def mvp_source_spec_by_fixture_id(fixture_id: str) -> MvpSourceSpec:
    try:
        return next(spec for spec in DEFAULT_MVP_SOURCE_SPECS if spec.fixture_id == fixture_id)
    except StopIteration as exc:
        raise ValueError(f"Unknown MVP fixture_id: {fixture_id}") from exc
