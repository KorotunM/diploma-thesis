import asyncio
from datetime import UTC, datetime
from uuid import uuid4

from libs.contracts.events import CrawlRequestPayload
from libs.source_sdk import (
    ExtractedFragment,
    FetchContext,
    FetchedArtifact,
    IntermediateRecord,
    ParserExecutionStatus,
    SourceAdapter,
)


class ExampleAdapter(SourceAdapter):
    source_key = "msu-official"
    adapter_version = "0.2.0"
    supported_parser_profiles = ("official_site.default",)

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def fetch(self, context: FetchContext) -> FetchedArtifact:
        self.calls.append("fetch")
        return FetchedArtifact(
            crawl_run_id=context.crawl_run_id,
            source_key=context.source_key,
            source_url=context.endpoint_url,
            content_type="text/html",
            content_length=26,
            sha256="a" * 64,
            render_mode=context.render_mode,
            content=b"<html><title>MSU</title></html>",
        )

    async def store_raw(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> FetchedArtifact:
        self.calls.append("store_raw")
        return artifact.model_copy(
            update={
                "storage_bucket": "raw-html",
                "storage_object_key": f"{context.source_key}/{artifact.sha256}.html",
            }
        )

    async def extract(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> list[ExtractedFragment]:
        self.calls.append("extract")
        return [
            ExtractedFragment(
                raw_artifact_id=artifact.raw_artifact_id,
                source_key=context.source_key,
                source_url=artifact.source_url,
                field_name="canonical_name",
                value="MSU",
                locator="html > title",
            )
        ]

    async def map_to_intermediate(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
        fragments: list[ExtractedFragment],
    ) -> list[IntermediateRecord]:
        self.calls.append("map_to_intermediate")
        return [
            IntermediateRecord(
                source_key=context.source_key,
                entity_hint="MSU",
                claims=[
                    {
                        "field_name": fragments[0].field_name,
                        "value": fragments[0].value,
                        "raw_artifact_id": str(artifact.raw_artifact_id),
                    }
                ],
                fragment_ids=[fragments[0].fragment_id],
            )
        ]


class FailingExtractAdapter(ExampleAdapter):
    async def extract(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> list[ExtractedFragment]:
        self.calls.append("extract")
        raise ValueError("selector missing")


def test_fetch_context_is_derived_from_crawl_request_policy() -> None:
    crawl_run_id = uuid4()
    requested_at = datetime(2026, 4, 20, 9, 30, tzinfo=UTC)

    context = FetchContext.from_crawl_request(
        CrawlRequestPayload(
            crawl_run_id=crawl_run_id,
            source_key="msu-official",
            endpoint_url="https://example.edu",
            priority="high",
            trigger="manual",
            parser_profile="official_site.default",
            requested_at=requested_at,
            metadata={
                "endpoint_id": str(uuid4()),
                "crawl_policy": {
                    "render_mode": "browser",
                    "timeout_seconds": 45,
                    "max_retries": 2,
                    "retry_backoff_seconds": 30,
                    "respect_robots_txt": False,
                    "allowed_content_types": ["text/html"],
                    "request_headers": {"user-agent": "parser-smoke"},
                },
            },
        )
    )

    assert context.crawl_run_id == crawl_run_id
    assert context.source_key == "msu-official"
    assert context.endpoint_url == "https://example.edu"
    assert context.priority == "high"
    assert context.trigger == "manual"
    assert context.parser_profile == "official_site.default"
    assert context.requested_at == requested_at
    assert context.render_mode == "browser"
    assert context.timeout_seconds == 45
    assert context.max_retries == 2
    assert context.retry_backoff_seconds == 30
    assert context.respect_robots_txt is False
    assert context.allowed_content_types == ["text/html"]
    assert context.request_headers == {"user-agent": "parser-smoke"}
    assert "crawl_policy" not in context.metadata


def test_source_adapter_execution_model_runs_pipeline_in_order() -> None:
    context = FetchContext(
        crawl_run_id=uuid4(),
        source_key="msu-official",
        endpoint_url="https://example.edu",
        parser_profile="official_site.default",
    )
    adapter = ExampleAdapter()

    result = asyncio.run(adapter.execute(context))

    assert adapter.can_handle(context) is True
    assert adapter.calls == ["fetch", "store_raw", "extract", "map_to_intermediate"]
    assert result.status == ParserExecutionStatus.SUCCEEDED
    assert result.adapter_key == "msu-official:0.2.0"
    assert result.adapter_version == "0.2.0"
    assert result.crawl_run_id == context.crawl_run_id
    assert result.artifact is not None
    assert result.artifact.storage_bucket == "raw-html"
    assert result.extracted_fragments == 1
    assert result.fragments[0].field_name == "canonical_name"
    assert result.intermediate_records[0].claims[0]["value"] == "MSU"
    assert result.completed_at is not None


def test_source_adapter_execution_model_returns_failed_result_with_stage() -> None:
    context = FetchContext(
        crawl_run_id=uuid4(),
        source_key="msu-official",
        endpoint_url="https://example.edu",
        parser_profile="official_site.default",
    )
    adapter = FailingExtractAdapter()

    result = asyncio.run(adapter.execute(context))

    assert adapter.calls == ["fetch", "store_raw", "extract"]
    assert result.status == ParserExecutionStatus.FAILED
    assert result.artifact is not None
    assert result.artifact.storage_bucket == "raw-html"
    assert result.errors[0].stage == "extract"
    assert result.errors[0].error_type == "ValueError"
    assert result.errors[0].message == "selector missing"
    assert result.completed_at is not None
