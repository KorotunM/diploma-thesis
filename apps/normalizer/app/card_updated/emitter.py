from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from libs.contracts.events import CardUpdatedEvent, CardUpdatedPayload, EventHeader

CARD_UPDATED_QUEUE = "card.updated"


class CardUpdatedPublisher(Protocol):
    def publish(
        self,
        payload: Any,
        *,
        queue_name: str,
        headers: dict[str, Any] | None = None,
    ) -> Any:
        """Publish card.updated event into the delivery topology."""


@dataclass(frozen=True)
class CardUpdatedEmission:
    event: CardUpdatedEvent
    queue_name: str
    exchange_name: str
    routing_key: str


class CardUpdatedEmitter:
    def __init__(
        self,
        *,
        publisher: CardUpdatedPublisher,
        producer: str = "normalizer",
    ) -> None:
        self._publisher = publisher
        self._producer = producer

    def emit(
        self,
        *,
        university_id: UUID,
        card_version: int,
        updated_fields: list[str],
        trace_id: UUID | None,
    ) -> CardUpdatedEmission:
        event = CardUpdatedEvent(
            header=EventHeader(
                producer=self._producer,
                trace_id=trace_id,
            ),
            payload=CardUpdatedPayload(
                university_id=university_id,
                card_version=card_version,
                updated_fields=sorted(set(updated_fields)),
            ),
        )
        publish_result = self._publisher.publish(
            event.model_dump(mode="json"),
            queue_name=CARD_UPDATED_QUEUE,
            headers=self._headers(event),
        )
        return CardUpdatedEmission(
            event=event,
            queue_name=getattr(publish_result, "queue_name", CARD_UPDATED_QUEUE),
            exchange_name=getattr(publish_result, "exchange_name", "delivery.events"),
            routing_key=getattr(publish_result, "routing_key", "card.updated"),
        )

    @staticmethod
    def _headers(event: CardUpdatedEvent) -> dict[str, str]:
        return {
            "event_name": event.event_name,
            "event_id": str(event.header.event_id),
            "schema_version": str(event.header.schema_version),
            "university_id": str(event.payload.university_id),
            "card_version": str(event.payload.card_version),
        }
