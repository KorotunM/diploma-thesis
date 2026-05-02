from __future__ import annotations

from uuid import uuid4

from apps.normalizer.app.card_updated import CARD_UPDATED_QUEUE, CardUpdatedEmitter


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
        return type(
            "PublishResult",
            (),
            {
                "queue_name": queue_name,
                "exchange_name": "delivery.events",
                "routing_key": "card.updated",
            },
        )()


def test_card_updated_emitter_publishes_delivery_contract_event() -> None:
    university_id = uuid4()
    trace_id = uuid4()
    publisher = FakePublisher()
    emitter = CardUpdatedEmitter(publisher=publisher)

    emission = emitter.emit(
        university_id=university_id,
        card_version=3,
        updated_fields=["contacts.website", "canonical_name", "contacts.website"],
        trace_id=trace_id,
    )

    assert emission.queue_name == CARD_UPDATED_QUEUE
    assert emission.exchange_name == "delivery.events"
    assert emission.routing_key == "card.updated"
    assert emission.event.header.trace_id == trace_id
    assert emission.event.payload.university_id == university_id
    assert emission.event.payload.card_version == 3
    assert emission.event.payload.updated_fields == [
        "canonical_name",
        "contacts.website",
    ]
    assert publisher.calls[0]["queue_name"] == CARD_UPDATED_QUEUE
    assert publisher.calls[0]["headers"]["event_name"] == "card.updated.v1"
