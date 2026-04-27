from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from apps.normalizer.app.claims import (
    ClaimBuildResult,
    ClaimEvidenceRecord,
    ClaimRecord,
    ParsedDocumentSnapshot,
)
from apps.normalizer.app.matching import UniversityMatchDecision
from apps.normalizer.app.review_required import ReviewRequiredEmitter
from apps.normalizer.app.resolution import SourceTrustTier
from apps.normalizer.app.universities import (
    SourceAuthorityRecord,
    UniversityRecord,
    UniversitySimilarityCandidate,
)
from libs.contracts.events import ReviewRequiredEvent


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
            exchange_name="delivery.events",
            routing_key="review.required",
        )


def build_claim_result() -> ClaimBuildResult:
    parsed_document_id = uuid4()
    raw_artifact_id = uuid4()
    claim = ClaimRecord(
        claim_id=uuid4(),
        parsed_document_id=parsed_document_id,
        source_key="msu-aggregator",
        field_name="canonical_name",
        value="Example Univ",
        value_type="str",
        entity_hint="Example Univ",
        parser_version="aggregator.0.1.0",
        normalizer_version="normalizer.0.1.0",
        parser_confidence=0.8,
        created_at=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        metadata={},
    )
    evidence = ClaimEvidenceRecord(
        evidence_id=uuid4(),
        claim_id=claim.claim_id,
        source_key="msu-aggregator",
        source_url="https://directory.example.com/universities/example",
        raw_artifact_id=raw_artifact_id,
        fragment_id=uuid4(),
        captured_at=datetime(2026, 4, 27, 9, 55, tzinfo=UTC),
        metadata={},
    )
    return ClaimBuildResult(
        parsed_document=ParsedDocumentSnapshot(
            parsed_document_id=parsed_document_id,
            crawl_run_id=uuid4(),
            raw_artifact_id=raw_artifact_id,
            source_key="msu-aggregator",
            parser_profile="aggregator.default",
            parser_version="aggregator.0.1.0",
            entity_type="university",
            entity_hint="Example Univ",
            parsed_at=datetime(2026, 4, 27, 9, 58, tzinfo=UTC),
            metadata={},
        ),
        claims=[claim],
        evidence=[evidence],
    )


def test_review_required_emitter_publishes_contract_event() -> None:
    claim_result = build_claim_result()
    source = SourceAuthorityRecord(
        source_id=uuid4(),
        source_key="msu-aggregator",
        source_type="aggregator",
        trust_tier=SourceTrustTier.TRUSTED,
        is_active=True,
        metadata={},
    )
    candidate_university = UniversityRecord(
        university_id=uuid4(),
        canonical_name="Example University",
        canonical_domain="example.edu",
        country_code="RU",
        city_name="Moscow",
        created_at=datetime(2026, 4, 27, 9, 0, tzinfo=UTC),
        metadata={},
    )
    decision = UniversityMatchDecision(
        status="review_required",
        matched_by="canonical_name",
        matched_value="Example Univ",
        strategy="trigram",
        similarity_score=0.8,
        review_candidates=[
            UniversitySimilarityCandidate(
                university=candidate_university,
                similarity_score=0.8,
            )
        ],
    )
    publisher = FakePublisher()
    emitter = ReviewRequiredEmitter(publisher=publisher)
    trace_id = uuid4()

    emission = emitter.emit_gray_zone_match(
        source=source,
        claim_result=claim_result,
        decision=decision,
        trace_id=trace_id,
    )

    assert emission.queue_name == "review.required"
    assert emission.exchange_name == "delivery.events"
    assert emission.routing_key == "review.required"
    assert emission.event.header.producer == "normalizer"
    assert emission.event.header.trace_id == trace_id
    assert emission.event.payload.reason == "gray_zone_trigram_match"
    assert emission.event.payload.priority == "high"
    assert emission.event.payload.university_id == candidate_university.university_id
    assert emission.event.payload.evidence_ids == [claim_result.evidence[0].evidence_id]
    assert emission.event.payload.metadata["matched_value"] == "Example Univ"
    assert emission.event.payload.metadata["candidates"][0]["canonical_name"] == (
        "Example University"
    )

    published_event = ReviewRequiredEvent.model_validate(publisher.calls[0]["payload"])
    assert published_event == emission.event
    assert publisher.calls[0]["headers"] == {
        "event_name": "review.required.v1",
        "event_id": str(emission.event.header.event_id),
        "schema_version": "1",
        "source_key": "msu-aggregator",
        "priority": "high",
        "reason": "gray_zone_trigram_match",
    }
