from __future__ import annotations

from typing import Any, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from apps.scheduler.app.sources.models import SourceTrustTier
from libs.contracts.events import EventHeader, ReviewRequiredEvent, ReviewRequiredPayload

from .models import SourceFreshnessSnapshot

REVIEW_REQUIRED_QUEUE = "review.required"
STALE_SOURCE_REASON = "stale_source_monitoring"


class StaleSourceReviewPublisher(Protocol):
    def publish(
        self,
        payload: Any,
        *,
        queue_name: str,
        headers: dict[str, Any] | None = None,
    ) -> Any:
        """Publish review.required event for stale source monitoring."""


class StaleSourceReviewEmission(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event: ReviewRequiredEvent
    queue_name: str
    exchange_name: str
    routing_key: str


class StaleSourceReviewRequiredEmitter:
    def __init__(
        self,
        *,
        publisher: StaleSourceReviewPublisher,
        producer: str = "scheduler",
    ) -> None:
        self._publisher = publisher
        self._producer = producer

    def emit(
        self,
        *,
        source: SourceFreshnessSnapshot,
        trace_id: UUID | None,
    ) -> StaleSourceReviewEmission:
        event = self._build_event(source=source, trace_id=trace_id)
        publish_result = self._publisher.publish(
            event.model_dump(mode="json"),
            queue_name=REVIEW_REQUIRED_QUEUE,
            headers=self._headers(event=event, source=source),
        )
        return StaleSourceReviewEmission(
            event=event,
            queue_name=getattr(publish_result, "queue_name", REVIEW_REQUIRED_QUEUE),
            exchange_name=getattr(publish_result, "exchange_name", "delivery.events"),
            routing_key=getattr(publish_result, "routing_key", "review.required"),
        )

    def _build_event(
        self,
        *,
        source: SourceFreshnessSnapshot,
        trace_id: UUID | None,
    ) -> ReviewRequiredEvent:
        payload = ReviewRequiredPayload(
            reason=STALE_SOURCE_REASON,
            priority=self._priority(source),
            metadata={
                "source_key": source.source_key,
                "source_type": source.source_type.value,
                "trust_tier": source.trust_tier.value,
                "freshness_state": source.freshness_state.value,
                "freshness_reason": source.freshness_reason,
                "refresh_interval_seconds": source.refresh_interval_seconds,
                "last_observed_at": _isoformat(source.last_observed_at),
                "last_attempted_at": _isoformat(source.last_attempted_at),
                "stale_since": _isoformat(source.stale_since),
                "checked_at": source.checked_at.isoformat(),
                "endpoint_count": source.endpoint_count,
                "scheduled_endpoint_count": source.scheduled_endpoint_count,
                "endpoint_urls": source.endpoint_urls,
            },
        )
        return ReviewRequiredEvent(
            header=EventHeader(producer=self._producer, trace_id=trace_id),
            payload=payload,
        )

    @staticmethod
    def _priority(source: SourceFreshnessSnapshot) -> str:
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
        source: SourceFreshnessSnapshot,
    ) -> dict[str, str]:
        return {
            "event_name": event.event_name,
            "event_id": str(event.header.event_id),
            "schema_version": str(event.header.schema_version),
            "source_key": source.source_key,
            "priority": event.payload.priority,
            "reason": event.payload.reason,
        }


def _isoformat(value) -> str | None:
    if value is None:
        return None
    return value.isoformat()
