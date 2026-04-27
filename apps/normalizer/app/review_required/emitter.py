from __future__ import annotations

from typing import Any, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from apps.normalizer.app.claims import ClaimBuildResult
from apps.normalizer.app.matching import UniversityMatchDecision
from apps.normalizer.app.resolution import SourceTrustTier
from apps.normalizer.app.universities.models import SourceAuthorityRecord
from libs.contracts.events import (
    EventHeader,
    ReviewRequiredEvent,
    ReviewRequiredPayload,
)

REVIEW_REQUIRED_QUEUE = "review.required"
GRAY_ZONE_REASON = "gray_zone_trigram_match"


class ReviewRequiredPublisher(Protocol):
    def publish(
        self,
        payload: Any,
        *,
        queue_name: str,
        headers: dict[str, Any] | None = None,
    ) -> Any:
        """Publish review.required event into the delivery topology."""


class ReviewRequiredEmission(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event: ReviewRequiredEvent
    queue_name: str
    exchange_name: str
    routing_key: str


class ReviewRequiredEmitter:
    def __init__(
        self,
        *,
        publisher: ReviewRequiredPublisher,
        producer: str = "normalizer",
    ) -> None:
        self._publisher = publisher
        self._producer = producer

    def emit_gray_zone_match(
        self,
        *,
        source: SourceAuthorityRecord,
        claim_result: ClaimBuildResult,
        decision: UniversityMatchDecision,
        trace_id: UUID | None,
    ) -> ReviewRequiredEmission:
        event = self._build_event(
            source=source,
            claim_result=claim_result,
            decision=decision,
            trace_id=trace_id,
        )
        publish_result = self._publisher.publish(
            event.model_dump(mode="json"),
            queue_name=REVIEW_REQUIRED_QUEUE,
            headers=self._headers(event=event, source=source),
        )
        return ReviewRequiredEmission(
            event=event,
            queue_name=getattr(publish_result, "queue_name", REVIEW_REQUIRED_QUEUE),
            exchange_name=getattr(publish_result, "exchange_name", "delivery.events"),
            routing_key=getattr(publish_result, "routing_key", "review.required"),
        )

    def _build_event(
        self,
        *,
        source: SourceAuthorityRecord,
        claim_result: ClaimBuildResult,
        decision: UniversityMatchDecision,
        trace_id: UUID | None,
    ) -> ReviewRequiredEvent:
        payload = ReviewRequiredPayload(
            reason=GRAY_ZONE_REASON,
            priority=self._priority(source),
            university_id=(
                decision.review_candidates[0].university.university_id
                if decision.review_candidates
                else None
            ),
            evidence_ids=[record.evidence_id for record in claim_result.evidence],
            metadata=self._metadata(
                source=source,
                claim_result=claim_result,
                decision=decision,
            ),
        )
        return ReviewRequiredEvent(
            header=EventHeader(producer=self._producer, trace_id=trace_id),
            payload=payload,
        )

    def _metadata(
        self,
        *,
        source: SourceAuthorityRecord,
        claim_result: ClaimBuildResult,
        decision: UniversityMatchDecision,
    ) -> dict[str, Any]:
        return {
            "source_key": source.source_key,
            "source_type": source.source_type,
            "trust_tier": source.trust_tier.value,
            "parsed_document_id": str(claim_result.parsed_document.parsed_document_id),
            "parser_version": claim_result.parsed_document.parser_version,
            "entity_hint": claim_result.parsed_document.entity_hint,
            "match_strategy": decision.strategy,
            "matched_by": decision.matched_by,
            "matched_value": decision.matched_value,
            "similarity_score": decision.similarity_score,
            "candidates": [
                {
                    "university_id": str(candidate.university.university_id),
                    "canonical_name": candidate.university.canonical_name,
                    "canonical_domain": candidate.university.canonical_domain,
                    "similarity_score": candidate.similarity_score,
                }
                for candidate in decision.review_candidates
            ],
        }

    @staticmethod
    def _priority(source: SourceAuthorityRecord) -> str:
        if source.trust_tier in {
            SourceTrustTier.AUTHORITATIVE,
            SourceTrustTier.TRUSTED,
        }:
            return "high"
        return "normal"

    @staticmethod
    def _headers(
        *,
        event: ReviewRequiredEvent,
        source: SourceAuthorityRecord,
    ) -> dict[str, str]:
        return {
            "event_name": event.event_name,
            "event_id": str(event.header.event_id),
            "schema_version": str(event.header.schema_version),
            "source_key": source.source_key,
            "priority": event.payload.priority,
            "reason": event.payload.reason,
        }
