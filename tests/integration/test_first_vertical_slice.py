from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

from apps.normalizer.app.main import prepare_normalization
from apps.scheduler.app.runs.models import (
    ManualCrawlTriggerRequest,
    PipelineRunStatus,
    PipelineRunType,
    PipelineTriggerType,
)
from apps.scheduler.app.runs.service import ManualCrawlTriggerService
from apps.scheduler.app.sources.models import CrawlPolicy, SourceEndpointRecord
from libs.contracts.events import CrawlRequestEvent, ParseCompletedEvent
from tests.integration.parser_ingestion_harness import (
    ParserIngestionHarness,
    load_bytes_fixture,
)


class InMemoryEndpointRepository:
    def __init__(self, endpoint: SourceEndpointRecord) -> None:
        self._endpoint = endpoint

    def get(self, source_key: str, endpoint_id: UUID) -> SourceEndpointRecord | None:
        if (
            self._endpoint.source_key == source_key
            and self._endpoint.endpoint_id == endpoint_id
        ):
            return self._endpoint
        return None


class InMemoryPipelineRunRepository:
    def __init__(self) -> None:
        self.record: dict[str, Any] | None = None
        self.commit_count = 0

    def create(self, **kwargs) -> dict[str, Any]:
        self.record = {
            "run_id": kwargs["run_id"],
            "run_type": kwargs["run_type"],
            "status": kwargs["status"],
            "trigger_type": kwargs["trigger_type"],
            "source_key": kwargs["source_key"],
            "started_at": datetime(2026, 4, 21, 9, 0, tzinfo=UTC),
            "finished_at": None,
            "metadata": kwargs["metadata"],
        }
        return self.record

    def transition(self, **kwargs) -> dict[str, Any] | None:
        if self.record is None:
            return None
        self.record = {
            **self.record,
            "status": kwargs["status"],
            "metadata": {
                **self.record["metadata"],
                **(kwargs.get("metadata_patch") or {}),
            },
        }
        if kwargs.get("finish"):
            self.record["finished_at"] = datetime(2026, 4, 21, 9, 5, tzinfo=UTC)
        return self.record

    def commit(self) -> None:
        self.commit_count += 1


class CapturingPublisher:
    def __init__(self, *, exchange_name: str) -> None:
        self.exchange_name = exchange_name
        self.calls: list[dict[str, Any]] = []

    def publish(
        self,
        payload: Any,
        *,
        queue_name: str,
        headers: dict[str, Any] | None = None,
    ) -> SimpleNamespace:
        self.calls.append(
            {
                "payload": payload,
                "queue_name": queue_name,
                "headers": headers or {},
            }
        )
        routing_key = "high" if queue_name.endswith(".high") else "bulk"
        return SimpleNamespace(
            queue_name=queue_name,
            exchange_name=self.exchange_name,
            routing_key=routing_key,
        )


def test_first_vertical_slice_from_scheduler_trigger_to_normalize_request() -> None:
    endpoint_id = uuid4()
    crawl_run_id = uuid4()
    endpoint = SourceEndpointRecord(
        endpoint_id=endpoint_id,
        source_id=uuid4(),
        source_key="msu-official",
        endpoint_url="https://example.edu/admissions",
        parser_profile="official_site.default",
        crawl_policy=CrawlPolicy(
            timeout_seconds=45,
            max_retries=2,
            allowed_content_types=["text/html"],
            request_headers={"x-fixture-run": "parser-ingestion"},
        ),
    )
    scheduler_publisher = CapturingPublisher(exchange_name="parser.jobs")
    scheduler_service = ManualCrawlTriggerService(
        endpoint_repository=InMemoryEndpointRepository(endpoint),
        run_repository=InMemoryPipelineRunRepository(),
        publisher=scheduler_publisher,
    )

    accepted = scheduler_service.trigger_manual_crawl(
        ManualCrawlTriggerRequest(
            crawl_run_id=crawl_run_id,
            source_key="msu-official",
            endpoint_id=endpoint_id,
            priority="high",
            requested_at=datetime(2026, 4, 21, 9, 10, tzinfo=UTC),
            metadata={"requested_by": "integration-test"},
        )
    )

    assert accepted.pipeline_run.status == PipelineRunStatus.PUBLISHED
    assert accepted.pipeline_run.run_type == PipelineRunType.CRAWL
    assert accepted.pipeline_run.trigger_type == PipelineTriggerType.MANUAL
    assert scheduler_publisher.calls[0]["queue_name"] == "parser.high"
    crawl_request = CrawlRequestEvent.model_validate(
        scheduler_publisher.calls[0]["payload"]
    )

    parse_completed_publisher = CapturingPublisher(exchange_name="normalize.jobs")
    parser_harness = ParserIngestionHarness(
        endpoint_url=endpoint.endpoint_url,
        response_body=load_bytes_fixture("official_site_admissions.html"),
        enable_official_parser=True,
        parse_completed_publisher=parse_completed_publisher,
    )
    parser_result = parser_harness.build_consumer().handle_message(
        scheduler_publisher.calls[0]["payload"]
    )

    assert parser_result.parsed_document is not None
    assert parser_result.parsed_document.entity_hint == "Example University"
    assert parser_result.raw_artifact.storage_bucket == "raw-html"
    assert len(parser_result.extracted_fragments) == 4
    assert len(parser_harness.raw_artifact_session.rows) == 1
    assert len(parser_harness.raw_artifact_session.parsed_documents) == 1
    assert len(parser_harness.raw_artifact_session.extracted_fragments) == 4
    assert parser_harness.requested_headers[0]["x-fixture-run"] == "parser-ingestion"

    assert parse_completed_publisher.calls[0]["queue_name"] == "normalize.high"
    parse_completed = ParseCompletedEvent.model_validate(
        parse_completed_publisher.calls[0]["payload"]
    )
    assert parse_completed.header.trace_id == crawl_request.header.event_id
    assert parse_completed.payload.crawl_run_id == crawl_run_id
    assert parse_completed.payload.raw_artifact_id == parser_result.raw_artifact.raw_artifact_id
    assert parse_completed.payload.parsed_document_id == (
        parser_result.parsed_document.parsed_document_id
    )
    assert parse_completed.payload.extracted_fragments == 4
    assert parse_completed.payload.metadata["entity_hint"] == "Example University"

    normalize_request = prepare_normalization(parse_completed)

    assert normalize_request.event_name == "normalize.request.v1"
    assert normalize_request.header.trace_id == crawl_request.header.event_id
    assert normalize_request.payload.crawl_run_id == crawl_run_id
    assert normalize_request.payload.source_key == "msu-official"
    assert normalize_request.payload.parsed_document_id == (
        parser_result.parsed_document.parsed_document_id
    )
    assert normalize_request.payload.parser_version == (
        parser_result.parsed_document.parser_version
    )
