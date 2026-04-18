from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ExchangeDefinition:
    name: str
    exchange_type: str
    durable: bool = True
    auto_delete: bool = False
    arguments: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "type": self.exchange_type,
            "durable": self.durable,
            "auto_delete": self.auto_delete,
            "arguments": dict(self.arguments),
        }


@dataclass(frozen=True)
class QueueDefinition:
    name: str
    exchange: str
    routing_key: str
    durable: bool = True
    auto_delete: bool = False
    queue_type: str | None = None
    dead_letter_exchange: str | None = None
    arguments: dict[str, Any] = field(default_factory=dict)

    def queue_arguments(self) -> dict[str, Any]:
        combined_arguments = dict(self.arguments)
        if self.queue_type:
            combined_arguments.setdefault("x-queue-type", self.queue_type)
        if self.dead_letter_exchange:
            combined_arguments.setdefault("x-dead-letter-exchange", self.dead_letter_exchange)
        return combined_arguments

    def as_dict(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "routing_key": self.routing_key,
            "durable": self.durable,
            "auto_delete": self.auto_delete,
            "arguments": self.queue_arguments(),
        }


EXCHANGE_DEFINITIONS = (
    ExchangeDefinition(name="parser.jobs", exchange_type="direct"),
    ExchangeDefinition(name="normalize.jobs", exchange_type="direct"),
    ExchangeDefinition(name="delivery.events", exchange_type="topic"),
    ExchangeDefinition(name="parser.dlx", exchange_type="fanout"),
    ExchangeDefinition(name="normalize.dlx", exchange_type="fanout"),
    ExchangeDefinition(name="delivery.dlx", exchange_type="fanout"),
)

QUEUE_DEFINITIONS = (
    QueueDefinition(
        name="parser.high",
        exchange="parser.jobs",
        routing_key="high",
        queue_type="quorum",
        dead_letter_exchange="parser.dlx",
    ),
    QueueDefinition(
        name="parser.bulk",
        exchange="parser.jobs",
        routing_key="bulk",
        queue_type="quorum",
        dead_letter_exchange="parser.dlx",
    ),
    QueueDefinition(
        name="normalize.high",
        exchange="normalize.jobs",
        routing_key="high",
        queue_type="quorum",
        dead_letter_exchange="normalize.dlx",
    ),
    QueueDefinition(
        name="normalize.bulk",
        exchange="normalize.jobs",
        routing_key="bulk",
        queue_type="quorum",
        dead_letter_exchange="normalize.dlx",
    ),
    QueueDefinition(
        name="card.updated",
        exchange="delivery.events",
        routing_key="card.updated",
        queue_type="quorum",
        dead_letter_exchange="delivery.dlx",
    ),
    QueueDefinition(
        name="review.required",
        exchange="delivery.events",
        routing_key="review.required",
        queue_type="quorum",
        dead_letter_exchange="delivery.dlx",
    ),
    QueueDefinition(name="parser.dead", exchange="parser.dlx", routing_key=""),
    QueueDefinition(name="normalize.dead", exchange="normalize.dlx", routing_key=""),
    QueueDefinition(name="delivery.dead", exchange="delivery.dlx", routing_key=""),
)

EXCHANGES = {definition.name: definition.as_dict() for definition in EXCHANGE_DEFINITIONS}
QUEUES = {definition.name: definition.as_dict() for definition in QUEUE_DEFINITIONS}


def get_exchange_definition(name: str) -> ExchangeDefinition:
    for definition in EXCHANGE_DEFINITIONS:
        if definition.name == name:
            return definition
    raise KeyError(f"Unknown RabbitMQ exchange: {name}")


def get_queue_definition(name: str) -> QueueDefinition:
    for definition in QUEUE_DEFINITIONS:
        if definition.name == name:
            return definition
    raise KeyError(f"Unknown RabbitMQ queue: {name}")
