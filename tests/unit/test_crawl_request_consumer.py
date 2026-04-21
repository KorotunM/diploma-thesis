import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from apps.parser.adapters.official_sites import OfficialSiteAdapter
from apps.parser.app.crawl_requests import (
    PARSER_HIGH_QUEUE,
    CrawlRequestConsumer,
    CrawlRequestProcessingService,
    build_crawl_request_consumer,
    build_crawl_request_consumers,
)
from apps.parser.app.parse_completed import ParseCompletedEmitter
from apps.parser.app.parsed_documents import (
    ParsedDocumentPersistenceService,
    ParsedDocumentRepository,
)
from apps.parser.app.raw_artifacts import RawArtifactPersistenceService, RawArtifactRepository
from libs.contracts.events import CrawlRequestEvent, CrawlRequestPayload, EventHeader
from libs.source_sdk import FetchContext, FetchedArtifact

FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "parser_ingestion"


class FakeMappingResult:
    def __init__(self, *, row=None) -> None:
        self._row = row

    def mappings(self) -> "FakeMappingResult":
        return self

    def one(self):
        return self._row


class FakeRawArtifactSession:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], dict] = {}
        self.documents: dict[tuple[str, str], dict] = {}
        self.fragments: dict[str, dict] = {}
        self.commit_count = 0

    def commit(self) -> None:
        self.commit_count += 1

    def execute(self, statement: str, params: dict):
        normalized_statement = " ".join(statement.split()).lower()
        if "insert into ingestion.raw_artifact" in normalized_statement:
            return FakeMappingResult(row=self._upsert_raw_artifact(params))
        if "insert into parsing.parsed_document" in normalized_statement:
            return FakeMappingResult(row=self._upsert_parsed_document(params))
        if "insert into parsing.extracted_fragment" in normalized_statement:
            return FakeMappingResult(row=self._upsert_extracted_fragment(params))
        raise AssertionError(f"Unexpected statement: {statement}")

    def _upsert_raw_artifact(self, params: dict):
        key = (params["source_key"], params["sha256"])
        existing = self.rows.get(key)
        if existing is None:
            row = dict(params)
        else:
            existing_metadata = json.loads(existing["metadata"])
            existing_metadata.update(json.loads(params["metadata"]))
            row = {
                **existing,
                **params,
                "raw_artifact_id": existing["raw_artifact_id"],
                "metadata": json.dumps(existing_metadata),
            }
        self.rows[key] = row
        return row

    def _upsert_parsed_document(self, params: dict):
        key = (str(params["raw_artifact_id"]), params["parser_version"])
        existing = self.documents.get(key)
        row = dict(params)
        if existing is not None:
            existing_metadata = json.loads(existing["metadata"])
            existing_metadata.update(json.loads(params["metadata"]))
            row = {
                **existing,
                **params,
                "parsed_document_id": existing["parsed_document_id"],
                "metadata": json.dumps(existing_metadata),
            }
        self.documents[key] = row
        return row

    def _upsert_extracted_fragment(self, params: dict):
        key = str(params["fragment_id"])
        existing = self.fragments.get(key)
        row = dict(params)
        if existing is not None:
            existing_metadata = json.loads(existing["metadata"])
            existing_metadata.update(json.loads(params["metadata"]))
            row["metadata"] = json.dumps(existing_metadata)
        self.fragments[key] = row
        return row


class FakeFetcher:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.calls: list[FetchContext] = []

    async def fetch(self, context: FetchContext) -> FetchedArtifact:
        self.calls.append(context)
        return FetchedArtifact(
            raw_artifact_id=uuid4(),
            crawl_run_id=context.crawl_run_id,
            source_key=context.source_key,
            source_url=context.endpoint_url,
            final_url=context.endpoint_url,
            http_status=200,
            content_type="text/html; charset=utf-8",
            response_headers={"content-type": "text/html; charset=utf-8"},
            content_length=len(self.payload),
            sha256=hashlib.sha256(self.payload).hexdigest(),
            fetched_at=datetime(2026, 4, 20, 13, 0, tzinfo=UTC),
            render_mode=context.render_mode,
            content=self.payload,
        )


class FakeRawStore:
    def __init__(self) -> None:
        self.calls: list[tuple[FetchContext, FetchedArtifact]] = []

    async def store_raw(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> FetchedArtifact:
        self.calls.append((context, artifact))
        return artifact.model_copy(
            update={
                "storage_bucket": "raw-html",
                "storage_object_key": (
                    f"{context.source_key}/{artifact.sha256[0:2]}/"
                    f"{artifact.sha256[2:4]}/{artifact.sha256}.html"
                ),
                "metadata": {"minio_etag": "etag-1"},
            }
        )


class FakeRabbitMQConsumer:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def build_consumer(self, **kwargs):
        self.calls.append(kwargs)
        return {"consumer": "parser", **kwargs}


class FakeParseCompletedPublisher:
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
        return type(
            "PublishResult",
            (),
            {
                "queue_name": queue_name,
                "exchange_name": "normalize.jobs",
                "routing_key": "high" if queue_name == "normalize.high" else "bulk",
            },
        )()


def build_event(crawl_run_id=None) -> CrawlRequestEvent:
    return CrawlRequestEvent(
        header=EventHeader(producer="scheduler"),
        payload=CrawlRequestPayload(
            crawl_run_id=crawl_run_id or uuid4(),
            source_key="msu-official",
            endpoint_url="https://example.edu",
            priority="high",
            trigger="manual",
            parser_profile="official_site.default",
            requested_at=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
            metadata={
                "crawl_policy": {
                    "render_mode": "http",
                    "timeout_seconds": 45,
                    "allowed_content_types": ["text/html"],
                }
            },
        ),
    )


def build_processing_service(
    *,
    fetcher: FakeFetcher,
    raw_store: FakeRawStore,
    session: FakeRawArtifactSession,
    enable_official_parser: bool = False,
    parse_completed_publisher: FakeParseCompletedPublisher | None = None,
) -> CrawlRequestProcessingService:
    repository = RawArtifactRepository(session, sql_text=lambda statement: statement)
    raw_artifact_service = RawArtifactPersistenceService(
        raw_store=raw_store,
        repository=repository,
    )
    parsed_document_service = None
    source_adapters = ()
    if enable_official_parser:
        parsed_document_repository = ParsedDocumentRepository(
            session,
            sql_text=lambda statement: statement,
        )
        parsed_document_service = ParsedDocumentPersistenceService(
            parsed_document_repository
        )
        source_adapters = (OfficialSiteAdapter(fetcher=fetcher),)
    return CrawlRequestProcessingService(
        fetcher=fetcher,
        raw_artifact_service=raw_artifact_service,
        parsed_document_service=parsed_document_service,
        source_adapters=source_adapters,
        parse_completed_emitter=(
            ParseCompletedEmitter(publisher=parse_completed_publisher)
            if parse_completed_publisher
            else None
        ),
    )


@pytest.mark.asyncio
async def test_crawl_request_processing_is_idempotent_for_same_source_and_sha256() -> None:
    fetcher = FakeFetcher(payload=b"<html>same</html>")
    raw_store = FakeRawStore()
    session = FakeRawArtifactSession()
    service = build_processing_service(fetcher=fetcher, raw_store=raw_store, session=session)
    event = build_event()

    first = await service.process(event)
    second = await service.process(event)

    assert len(fetcher.calls) == 2
    assert len(raw_store.calls) == 2
    assert len(session.rows) == 1
    assert session.commit_count == 2
    assert first.raw_artifact.raw_artifact_id == second.raw_artifact.raw_artifact_id
    assert first.raw_artifact.sha256 == hashlib.sha256(b"<html>same</html>").hexdigest()
    assert first.metadata["idempotency_key"] == f"msu-official:{first.raw_artifact.sha256}"
    assert second.metadata["raw_object_key"] == first.metadata["raw_object_key"]


@pytest.mark.asyncio
async def test_crawl_request_processing_persists_official_parsed_document() -> None:
    fetcher = FakeFetcher(
        payload=(FIXTURE_ROOT / "official_site_admissions.html").read_bytes()
    )
    raw_store = FakeRawStore()
    session = FakeRawArtifactSession()
    parse_completed_publisher = FakeParseCompletedPublisher()
    service = build_processing_service(
        fetcher=fetcher,
        raw_store=raw_store,
        session=session,
        enable_official_parser=True,
        parse_completed_publisher=parse_completed_publisher,
    )

    event = build_event()
    result = await service.process(event)

    assert result.parsed_document is not None
    assert result.parsed_document.entity_hint == "Example University"
    assert result.parsed_document.extracted_fragment_count == 4
    assert len(result.extracted_fragments) == 4
    assert result.metadata["parsed_document_id"] == str(
        result.parsed_document.parsed_document_id
    )
    fragments_by_field = {fragment.field_name: fragment for fragment in result.extracted_fragments}
    assert fragments_by_field["contacts.emails"].value == ["admissions@example.edu"]
    assert result.parse_completed is not None
    assert result.parse_completed.event.payload.parsed_document_id == (
        result.parsed_document.parsed_document_id
    )
    assert result.parse_completed.event.header.trace_id == event.header.event_id
    assert result.parse_completed.queue_name == "normalize.high"
    assert parse_completed_publisher.calls[0]["queue_name"] == "normalize.high"
    assert parse_completed_publisher.calls[0]["headers"]["event_name"] == "parse.completed.v1"
    assert len(session.documents) == 1
    assert len(session.fragments) == 4
    assert session.commit_count == 2


def test_crawl_request_consumer_validates_body_and_runs_processing_service() -> None:
    fetcher = FakeFetcher(payload=b"<html>consumer</html>")
    raw_store = FakeRawStore()
    session = FakeRawArtifactSession()
    service = build_processing_service(fetcher=fetcher, raw_store=raw_store, session=session)
    consumer = CrawlRequestConsumer(service=service)
    event = build_event()

    result = consumer.handle_message(event.model_dump(mode="json"))

    assert result.event_id == event.header.event_id
    assert result.crawl_run_id == event.payload.crawl_run_id
    assert result.source_key == "msu-official"
    assert result.raw_artifact.storage_bucket == "raw-html"
    assert session.commit_count == 1


def test_build_crawl_request_consumer_uses_declared_parser_queue() -> None:
    rabbitmq_consumer = FakeRabbitMQConsumer()
    service = build_processing_service(
        fetcher=FakeFetcher(payload=b"payload"),
        raw_store=FakeRawStore(),
        session=FakeRawArtifactSession(),
    )

    consumer = build_crawl_request_consumer(
        rabbitmq_consumer=rabbitmq_consumer,
        service=service,
        queue_name=PARSER_HIGH_QUEUE,
        prefetch_count=4,
        requeue_on_error=True,
    )

    assert consumer["queue_name"] == "parser.high"
    assert consumer["accept"] == ("json",)
    assert consumer["prefetch_count"] == 4
    assert consumer["requeue_on_error"] is True
    assert callable(rabbitmq_consumer.calls[0]["handler"])


def test_build_crawl_request_consumers_registers_high_and_bulk_queues() -> None:
    rabbitmq_consumer = FakeRabbitMQConsumer()
    service = build_processing_service(
        fetcher=FakeFetcher(payload=b"payload"),
        raw_store=FakeRawStore(),
        session=FakeRawArtifactSession(),
    )

    consumers = build_crawl_request_consumers(
        rabbitmq_consumer=rabbitmq_consumer,
        service=service,
        prefetch_count=8,
    )

    assert set(consumers) == {"parser.high", "parser.bulk"}
    assert [call["queue_name"] for call in rabbitmq_consumer.calls] == [
        "parser.high",
        "parser.bulk",
    ]
    assert all(call["prefetch_count"] == 8 for call in rabbitmq_consumer.calls)
