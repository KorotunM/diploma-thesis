from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal, Protocol
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, computed_field

from libs.contracts.events import CrawlRequestPayload


def utc_now() -> datetime:
    return datetime.now(UTC)


RenderMode = Literal["http", "browser", "auto"]
ExecutionStepName = Literal["fetch", "store_raw", "extract", "map_to_intermediate"]


class ParserExecutionStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ParserExecutionError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stage: ExecutionStepName
    message: str
    error_type: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class FetchContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    crawl_run_id: UUID
    source_key: str
    endpoint_url: str
    priority: Literal["high", "bulk"] = "bulk"
    trigger: Literal["schedule", "manual", "replay"] = "schedule"
    parser_profile: str = "default"
    requested_at: datetime = Field(default_factory=utc_now)
    render_mode: RenderMode = "http"
    timeout_seconds: int = Field(default=60, ge=1, le=300)
    max_retries: int = Field(default=3, ge=0, le=10)
    retry_backoff_seconds: int = Field(default=60, ge=1, le=3600)
    respect_robots_txt: bool = True
    allowed_content_types: list[str] = Field(
        default_factory=lambda: ["text/html", "application/json"]
    )
    request_headers: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_crawl_request(cls, payload: CrawlRequestPayload) -> FetchContext:
        crawl_policy = payload.metadata.get("crawl_policy")
        policy = crawl_policy if isinstance(crawl_policy, dict) else {}
        return cls(
            crawl_run_id=payload.crawl_run_id,
            source_key=payload.source_key,
            endpoint_url=payload.endpoint_url,
            priority=payload.priority,
            trigger=payload.trigger,
            parser_profile=payload.parser_profile,
            requested_at=payload.requested_at,
            render_mode=policy.get("render_mode", "http"),
            timeout_seconds=policy.get("timeout_seconds", 60),
            max_retries=policy.get("max_retries", 3),
            retry_backoff_seconds=policy.get("retry_backoff_seconds", 60),
            respect_robots_txt=policy.get("respect_robots_txt", True),
            allowed_content_types=policy.get(
                "allowed_content_types",
                ["text/html", "application/json"],
            ),
            request_headers=policy.get("request_headers", {}),
            metadata={
                key: value for key, value in payload.metadata.items() if key != "crawl_policy"
            },
        )


class FetchedArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    raw_artifact_id: UUID = Field(default_factory=uuid4)
    crawl_run_id: UUID | None = None
    source_key: str | None = None
    source_url: str
    final_url: str | None = None
    http_status: int | None = Field(default=None, ge=100, le=599)
    content_type: str
    response_headers: dict[str, str] = Field(default_factory=dict)
    content_length: int | None = Field(default=None, ge=0)
    sha256: str
    fetched_at: datetime = Field(default_factory=utc_now)
    render_mode: RenderMode = "http"
    etag: str | None = None
    last_modified: str | None = None
    storage_bucket: str | None = None
    storage_object_key: str | None = None
    content: bytes | None = Field(default=None, exclude=True)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExtractedFragment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fragment_id: UUID = Field(default_factory=uuid4)
    raw_artifact_id: UUID | None = None
    source_key: str | None = None
    source_url: str | None = None
    field_name: str
    value: Any
    locator: str | None = None
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    metadata: dict[str, Any] = Field(default_factory=dict)


class IntermediateRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    record_id: UUID = Field(default_factory=uuid4)
    source_key: str | None = None
    entity_type: str = "university"
    entity_hint: str | None = None
    claims: list[dict[str, Any]] = Field(default_factory=list)
    fragment_ids: list[UUID] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ParserExecutionPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    execution_id: UUID = Field(default_factory=uuid4)
    crawl_run_id: UUID
    source_key: str
    endpoint_url: str
    parser_profile: str
    adapter_key: str
    adapter_version: str
    fetch_context: FetchContext
    steps: tuple[ExecutionStepName, ...] = (
        "fetch",
        "store_raw",
        "extract",
        "map_to_intermediate",
    )
    created_at: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def for_adapter(
        cls,
        *,
        context: FetchContext,
        adapter_key: str,
        adapter_version: str,
        metadata: dict[str, Any] | None = None,
    ) -> ParserExecutionPlan:
        return cls(
            crawl_run_id=context.crawl_run_id,
            source_key=context.source_key,
            endpoint_url=context.endpoint_url,
            parser_profile=context.parser_profile,
            adapter_key=adapter_key,
            adapter_version=adapter_version,
            fetch_context=context,
            metadata=metadata or {},
        )


class ParserExecutionResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    execution_id: UUID
    crawl_run_id: UUID
    status: ParserExecutionStatus
    adapter_key: str
    adapter_version: str
    artifact: FetchedArtifact | None = None
    fragments: list[ExtractedFragment] = Field(default_factory=list)
    intermediate_records: list[IntermediateRecord] = Field(default_factory=list)
    errors: list[ParserExecutionError] = Field(default_factory=list)
    started_at: datetime = Field(default_factory=utc_now)
    completed_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @computed_field
    @property
    def extracted_fragments(self) -> int:
        return len(self.fragments)


class RawFetcher(Protocol):
    async def fetch(self, context: FetchContext) -> FetchedArtifact:
        """Fetch raw source payload for a crawl request."""


class RawArtifactStore(Protocol):
    async def store_raw(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> FetchedArtifact:
        """Persist raw source payload and return artifact metadata with storage pointers."""


class SourceAdapter(ABC):
    source_key: str
    adapter_version: str = "0.1.0"
    supported_parser_profiles: tuple[str, ...] = ()

    @property
    def adapter_key(self) -> str:
        return f"{self.source_key}:{self.adapter_version}"

    def can_handle(self, context: FetchContext) -> bool:
        if context.source_key != self.source_key:
            return False
        if not self.supported_parser_profiles:
            return True
        return context.parser_profile in self.supported_parser_profiles

    def build_execution_plan(self, context: FetchContext) -> ParserExecutionPlan:
        return ParserExecutionPlan.for_adapter(
            context=context,
            adapter_key=self.adapter_key,
            adapter_version=self.adapter_version,
        )

    @abstractmethod
    async def fetch(self, context: FetchContext) -> FetchedArtifact:
        raise NotImplementedError

    async def store_raw(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> FetchedArtifact:
        return artifact

    @abstractmethod
    async def extract(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> Sequence[ExtractedFragment]:
        raise NotImplementedError

    @abstractmethod
    async def map_to_intermediate(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
        fragments: Sequence[ExtractedFragment],
    ) -> Sequence[IntermediateRecord]:
        raise NotImplementedError

    async def execute(self, context: FetchContext) -> ParserExecutionResult:
        plan = self.build_execution_plan(context)
        started_at = utc_now()
        current_stage: ExecutionStepName = "fetch"
        artifact: FetchedArtifact | None = None
        stored_artifact: FetchedArtifact | None = None
        fragments: list[ExtractedFragment] = []
        try:
            artifact = await self.fetch(context)
            current_stage = "store_raw"
            stored_artifact = await self.store_raw(context, artifact)
            current_stage = "extract"
            fragments = list(await self.extract(context, stored_artifact))
            current_stage = "map_to_intermediate"
            records = list(
                await self.map_to_intermediate(context, stored_artifact, fragments)
            )
        except Exception as exc:
            return ParserExecutionResult(
                execution_id=plan.execution_id,
                crawl_run_id=context.crawl_run_id,
                status=ParserExecutionStatus.FAILED,
                adapter_key=plan.adapter_key,
                adapter_version=plan.adapter_version,
                artifact=stored_artifact or artifact,
                fragments=fragments,
                errors=[
                    ParserExecutionError(
                        stage=current_stage,
                        message=str(exc),
                        error_type=type(exc).__name__,
                    )
                ],
                started_at=started_at,
                completed_at=utc_now(),
            )

        return ParserExecutionResult(
            execution_id=plan.execution_id,
            crawl_run_id=context.crawl_run_id,
            status=ParserExecutionStatus.SUCCEEDED,
            adapter_key=plan.adapter_key,
            adapter_version=plan.adapter_version,
            artifact=stored_artifact,
            fragments=fragments,
            intermediate_records=records,
            started_at=started_at,
            completed_at=utc_now(),
        )
