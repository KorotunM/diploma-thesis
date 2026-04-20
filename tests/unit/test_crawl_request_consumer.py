import hashlib
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from apps.parser.app.crawl_requests import (
    PARSER_HIGH_QUEUE,
    CrawlRequestConsumer,
    CrawlRequestProcessingService,
    build_crawl_request_consumer,
    build_crawl_request_consumers,
)
from apps.parser.app.raw_artifacts import RawArtifactPersistenceService, RawArtifactRepository
from libs.contracts.events import CrawlRequestEvent, CrawlRequestPayload, EventHeader
from libs.source_sdk import FetchContext, FetchedArtifact


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
        self.commit_count = 0

    def commit(self) -> None:
        self.commit_count += 1

    def execute(self, statement: str, params: dict):
        normalized_statement = " ".join(statement.split()).lower()
        if normalized_statement.startswith("insert"):
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
            return FakeMappingResult(row=row)
        raise AssertionError(f"Unexpected statement: {statement}")


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
) -> CrawlRequestProcessingService:
    repository = RawArtifactRepository(session, sql_text=lambda statement: statement)
    raw_artifact_service = RawArtifactPersistenceService(
        raw_store=raw_store,
        repository=repository,
    )
    return CrawlRequestProcessingService(
        fetcher=fetcher,
        raw_artifact_service=raw_artifact_service,
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
