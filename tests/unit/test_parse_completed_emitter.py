from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from apps.parser.app.parse_completed import (
    NORMALIZE_BULK_QUEUE,
    NORMALIZE_HIGH_QUEUE,
    ParseCompletedEmitter,
    normalize_queue_for_priority,
)
from apps.parser.app.parsed_documents import (
    ExtractedFragmentRecord,
    ParsedDocumentRecord,
)
from apps.parser.app.raw_artifacts import RawArtifactRecord
from libs.contracts.events import ParseCompletedEvent
from libs.source_sdk import FetchContext


@dataclass(frozen=True)
class FakePublishResult:
    queue_name: str
    exchange_name: str
    routing_key: str


class FakePublisher:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def publish(self, payload, *, queue_name: str, headers: dict | None = None):
        self.calls.append(
            {
                "payload": payload,
                "queue_name": queue_name,
                "headers": headers,
            }
        )
        return FakePublishResult(
            queue_name=queue_name,
            exchange_name="normalize.jobs",
            routing_key="high" if queue_name == NORMALIZE_HIGH_QUEUE else "bulk",
        )


def build_context(priority: str = "high") -> FetchContext:
    return FetchContext(
        crawl_run_id=uuid4(),
        source_key="msu-official",
        endpoint_url="https://example.edu/admissions",
        priority=priority,
        trigger="manual",
        parser_profile="official_site.default",
    )


def build_raw_artifact(context: FetchContext) -> RawArtifactRecord:
    return RawArtifactRecord(
        raw_artifact_id=uuid4(),
        crawl_run_id=context.crawl_run_id,
        source_key=context.source_key,
        source_url=context.endpoint_url,
        final_url=context.endpoint_url,
        http_status=200,
        content_type="text/html",
        content_length=128,
        sha256="a" * 64,
        storage_bucket="raw-html",
        storage_object_key="msu-official/aa/aa/payload.html",
        etag=None,
        last_modified=None,
        fetched_at=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
        metadata={},
    )


def build_parsed_document(context: FetchContext, raw_artifact: RawArtifactRecord):
    return ParsedDocumentRecord(
        parsed_document_id=uuid4(),
        crawl_run_id=context.crawl_run_id,
        raw_artifact_id=raw_artifact.raw_artifact_id,
        source_key=context.source_key,
        parser_profile=context.parser_profile,
        parser_version="0.1.0",
        entity_type="university",
        entity_hint="Example University",
        extracted_fragment_count=1,
        parsed_at=datetime(2026, 4, 21, 10, 1, tzinfo=UTC),
        metadata={},
    )


def build_fragment(parsed_document: ParsedDocumentRecord):
    return ExtractedFragmentRecord(
        fragment_id=uuid4(),
        parsed_document_id=parsed_document.parsed_document_id,
        raw_artifact_id=parsed_document.raw_artifact_id,
        source_key=parsed_document.source_key,
        field_name="canonical_name",
        value="Example University",
        value_type="str",
        locator="h1",
        confidence=0.95,
        metadata={},
    )


def test_normalize_queue_for_priority_routes_only_high_to_high_queue() -> None:
    assert normalize_queue_for_priority("high") == NORMALIZE_HIGH_QUEUE
    assert normalize_queue_for_priority("bulk") == NORMALIZE_BULK_QUEUE
    assert normalize_queue_for_priority("unknown") == NORMALIZE_BULK_QUEUE


def test_parse_completed_emitter_publishes_contract_event_to_normalizer_queue() -> None:
    context = build_context(priority="high")
    raw_artifact = build_raw_artifact(context)
    parsed_document = build_parsed_document(context, raw_artifact)
    fragment = build_fragment(parsed_document)
    publisher = FakePublisher()
    emitter = ParseCompletedEmitter(publisher=publisher)
    trace_id = uuid4()

    emission = emitter.emit(
        context=context,
        raw_artifact=raw_artifact,
        parsed_document=parsed_document,
        extracted_fragments=[fragment],
        trace_id=trace_id,
    )

    assert emission.queue_name == "normalize.high"
    assert emission.exchange_name == "normalize.jobs"
    assert emission.routing_key == "high"
    assert emission.event.header.producer == "parser"
    assert emission.event.header.trace_id == trace_id
    assert emission.event.payload.raw_artifact_id == raw_artifact.raw_artifact_id
    assert emission.event.payload.parsed_document_id == parsed_document.parsed_document_id
    assert emission.event.payload.parser_version == "0.1.0"
    assert emission.event.payload.raw_bucket == "raw-html"
    assert emission.event.payload.parsed_bucket == "parsed-snapshots"
    assert emission.event.payload.extracted_fragments == 1
    assert emission.event.payload.metadata["parser_profile"] == "official_site.default"
    assert emission.event.payload.metadata["fragment_ids"] == [str(fragment.fragment_id)]

    published_event = ParseCompletedEvent.model_validate(publisher.calls[0]["payload"])
    assert published_event == emission.event
    assert publisher.calls[0]["queue_name"] == "normalize.high"
    assert publisher.calls[0]["headers"] == {
        "event_name": "parse.completed.v1",
        "event_id": str(emission.event.header.event_id),
        "schema_version": "1",
        "crawl_run_id": str(context.crawl_run_id),
        "source_key": "msu-official",
        "priority": "high",
    }
