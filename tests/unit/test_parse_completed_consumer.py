from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

from apps.normalizer.app.parse_completed import (
    NORMALIZE_HIGH_QUEUE,
    ParseCompletedConsumer,
    ParseCompletedProcessingService,
    build_normalize_request_event,
    build_parse_completed_consumer,
    build_parse_completed_consumers,
)
from libs.contracts.events import EventHeader, ParseCompletedEvent, ParseCompletedPayload


class FakeRabbitMQConsumer:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def build_consumer(self, **kwargs):
        self.calls.append(kwargs)
        return {"consumer": "normalizer", **kwargs}


class FakeClaimBuildService:
    def __init__(self, result) -> None:
        self.result = result
        self.calls = []

    def build_claims_from_extracted_fragments(self, payload):
        self.calls.append(payload)
        return self.result


class FakeUniversityBootstrapService:
    def __init__(self, result) -> None:
        self.result = result
        self.calls = []

    def consolidate_claims(self, claim_result):
        self.calls.append(claim_result)
        return self.result


class FakeResolvedFactGenerationService:
    def __init__(self, result) -> None:
        self.result = result
        self.calls = []

    def generate_for_bootstrap(self, bootstrap_result):
        self.calls.append(bootstrap_result)
        return self.result


class FakeUniversityCardProjectionService:
    def __init__(self, result) -> None:
        self.result = result
        self.calls = []

    def create_projection(self, fact_result):
        self.calls.append(fact_result)
        return self.result


class FakeCardUpdatedEmitter:
    def __init__(self, result) -> None:
        self.result = result
        self.calls = []

    def emit(self, **kwargs):
        self.calls.append(kwargs)
        return self.result


class FakeParseCompletedProcessingService:
    def __init__(self) -> None:
        self.calls = []

    def process(self, event):
        self.calls.append(event)
        return {"status": "ok", "parsed_document_id": str(event.payload.parsed_document_id)}


def build_event() -> ParseCompletedEvent:
    return ParseCompletedEvent(
        header=EventHeader(
            producer="parser",
            trace_id=uuid4(),
        ),
        payload=ParseCompletedPayload(
            crawl_run_id=uuid4(),
            source_key="msu-official",
            endpoint_url="https://example.edu/admissions",
            raw_artifact_id=uuid4(),
            parsed_document_id=uuid4(),
            parser_version="official.0.1.0",
            raw_bucket="raw-html",
            parsed_bucket="parsed-snapshots",
            extracted_fragments=4,
            metadata={"entity_hint": "Example University"},
        ),
    )


def test_build_normalize_request_event_preserves_parser_context() -> None:
    event = build_event()

    normalize_request = build_normalize_request_event(event)

    assert normalize_request.event_name == "normalize.request.v1"
    assert normalize_request.header.trace_id == event.header.trace_id
    assert normalize_request.payload.source_key == "msu-official"
    assert normalize_request.payload.parsed_document_id == event.payload.parsed_document_id
    assert normalize_request.payload.parser_version == "official.0.1.0"
    assert normalize_request.payload.metadata["entity_hint"] == "Example University"
    assert normalize_request.payload.metadata["endpoint_url"] == (
        "https://example.edu/admissions"
    )
    assert normalize_request.payload.metadata["extracted_fragments"] == 4


def test_parse_completed_processing_service_runs_full_normalization_chain() -> None:
    event = build_event()
    # Claims must have a .metadata dict so _split_by_entity can inspect record_group_key.
    fake_claim = SimpleNamespace(metadata={})
    claim_result = SimpleNamespace(claims=[fake_claim], evidence=[])
    bootstrap_result = SimpleNamespace(university=SimpleNamespace(university_id=uuid4()))
    fact_result = SimpleNamespace(
        facts=[
            SimpleNamespace(field_name="canonical_name"),
            SimpleNamespace(field_name="contacts.website"),
        ]
    )
    projection_result = SimpleNamespace(
        projection=SimpleNamespace(
            university_id=uuid4(),
            card_version=2,
        )
    )
    card_updated_result = SimpleNamespace(queue_name="card.updated")
    claim_build_service = FakeClaimBuildService(claim_result)
    bootstrap_service = FakeUniversityBootstrapService(bootstrap_result)
    fact_service = FakeResolvedFactGenerationService(fact_result)
    projection_service = FakeUniversityCardProjectionService(projection_result)
    card_updated_emitter = FakeCardUpdatedEmitter(card_updated_result)
    service = ParseCompletedProcessingService(
        claim_build_service=claim_build_service,
        university_bootstrap_service=bootstrap_service,
        resolved_fact_generation_service=fact_service,
        university_card_projection_service=projection_service,
        card_updated_emitter=card_updated_emitter,
        normalizer_version="normalizer.0.2.0",
    )

    results = service.process(event)

    assert len(results) == 1
    result = results[0]
    assert claim_build_service.calls[0].normalizer_version == "normalizer.0.2.0"
    assert bootstrap_service.calls == [claim_result]
    assert fact_service.calls == [bootstrap_result]
    assert projection_service.calls == [fact_result]
    assert result.projection_result is projection_result
    assert result.card_updated is card_updated_result
    assert card_updated_emitter.calls[0]["card_version"] == 2
    assert card_updated_emitter.calls[0]["updated_fields"] == [
        "canonical_name",
        "contacts.website",
    ]
    assert card_updated_emitter.calls[0]["trace_id"] == event.header.trace_id


def test_parse_completed_consumer_validates_body_and_runs_processing_service() -> None:
    event = build_event()
    service = FakeParseCompletedProcessingService()
    consumer = ParseCompletedConsumer(service=service)

    result = consumer.handle_message(event.model_dump(mode="json"))

    assert result["status"] == "ok"
    assert service.calls[0].payload.parsed_document_id == event.payload.parsed_document_id


def test_build_parse_completed_consumer_uses_declared_normalizer_queue() -> None:
    rabbitmq_consumer = FakeRabbitMQConsumer()
    service = FakeParseCompletedProcessingService()

    consumer = build_parse_completed_consumer(
        rabbitmq_consumer=rabbitmq_consumer,
        service=service,
        queue_name=NORMALIZE_HIGH_QUEUE,
        prefetch_count=4,
        requeue_on_error=True,
    )

    assert consumer["queue_name"] == "normalize.high"
    assert consumer["accept"] == ("json",)
    assert consumer["prefetch_count"] == 4
    assert consumer["requeue_on_error"] is True
    assert callable(rabbitmq_consumer.calls[0]["handler"])


def test_build_parse_completed_consumers_registers_high_and_bulk_queues() -> None:
    rabbitmq_consumer = FakeRabbitMQConsumer()
    service = FakeParseCompletedProcessingService()

    consumers = build_parse_completed_consumers(
        rabbitmq_consumer=rabbitmq_consumer,
        service=service,
        prefetch_count=8,
    )

    assert set(consumers) == {"normalize.high", "normalize.bulk"}
    assert [call["queue_name"] for call in rabbitmq_consumer.calls] == [
        "normalize.high",
        "normalize.bulk",
    ]
    assert all(call["prefetch_count"] == 8 for call in rabbitmq_consumer.calls)
