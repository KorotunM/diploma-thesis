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
    dead_letter_routing_key: str | None = None
    arguments: dict[str, Any] = field(default_factory=dict)

    def queue_arguments(self) -> dict[str, Any]:
        combined_arguments = dict(self.arguments)
        if self.queue_type:
            combined_arguments.setdefault("x-queue-type", self.queue_type)
        if self.dead_letter_exchange:
            combined_arguments.setdefault("x-dead-letter-exchange", self.dead_letter_exchange)
        if self.dead_letter_routing_key:
            combined_arguments.setdefault(
                "x-dead-letter-routing-key",
                self.dead_letter_routing_key,
            )
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
    ExchangeDefinition(name="parser.retry", exchange_type="direct"),
    ExchangeDefinition(name="normalize.retry", exchange_type="direct"),
    ExchangeDefinition(name="delivery.retry", exchange_type="direct"),
    ExchangeDefinition(name="parser.dead", exchange_type="direct"),
    ExchangeDefinition(name="normalize.dead", exchange_type="direct"),
    ExchangeDefinition(name="delivery.dead", exchange_type="direct"),
)

QUEUE_DEFINITIONS = (
    QueueDefinition(
        name="parser.high",
        exchange="parser.jobs",
        routing_key="high",
        queue_type="quorum",
        dead_letter_exchange="parser.dead",
        dead_letter_routing_key="high",
    ),
    QueueDefinition(
        name="parser.bulk",
        exchange="parser.jobs",
        routing_key="bulk",
        queue_type="quorum",
        dead_letter_exchange="parser.dead",
        dead_letter_routing_key="bulk",
    ),
    QueueDefinition(
        name="parser.high.retry",
        exchange="parser.retry",
        routing_key="high",
        queue_type="quorum",
        dead_letter_exchange="parser.jobs",
        dead_letter_routing_key="high",
        arguments={"x-message-ttl": 30000},
    ),
    QueueDefinition(
        name="parser.bulk.retry",
        exchange="parser.retry",
        routing_key="bulk",
        queue_type="quorum",
        dead_letter_exchange="parser.jobs",
        dead_letter_routing_key="bulk",
        arguments={"x-message-ttl": 120000},
    ),
    QueueDefinition(
        name="normalize.high",
        exchange="normalize.jobs",
        routing_key="high",
        queue_type="quorum",
        dead_letter_exchange="normalize.dead",
        dead_letter_routing_key="high",
    ),
    QueueDefinition(
        name="normalize.bulk",
        exchange="normalize.jobs",
        routing_key="bulk",
        queue_type="quorum",
        dead_letter_exchange="normalize.dead",
        dead_letter_routing_key="bulk",
    ),
    QueueDefinition(
        name="normalize.high.retry",
        exchange="normalize.retry",
        routing_key="high",
        queue_type="quorum",
        dead_letter_exchange="normalize.jobs",
        dead_letter_routing_key="high",
        arguments={"x-message-ttl": 30000},
    ),
    QueueDefinition(
        name="normalize.bulk.retry",
        exchange="normalize.retry",
        routing_key="bulk",
        queue_type="quorum",
        dead_letter_exchange="normalize.jobs",
        dead_letter_routing_key="bulk",
        arguments={"x-message-ttl": 120000},
    ),
    QueueDefinition(
        name="card.updated",
        exchange="delivery.events",
        routing_key="card.updated",
        queue_type="quorum",
        dead_letter_exchange="delivery.dead",
        dead_letter_routing_key="card.updated",
    ),
    QueueDefinition(
        name="review.required",
        exchange="delivery.events",
        routing_key="review.required",
        queue_type="quorum",
        dead_letter_exchange="delivery.dead",
        dead_letter_routing_key="review.required",
    ),
    QueueDefinition(
        name="card.updated.retry",
        exchange="delivery.retry",
        routing_key="card.updated",
        queue_type="quorum",
        dead_letter_exchange="delivery.events",
        dead_letter_routing_key="card.updated",
        arguments={"x-message-ttl": 30000},
    ),
    QueueDefinition(
        name="review.required.retry",
        exchange="delivery.retry",
        routing_key="review.required",
        queue_type="quorum",
        dead_letter_exchange="delivery.events",
        dead_letter_routing_key="review.required",
        arguments={"x-message-ttl": 120000},
    ),
    QueueDefinition(
        name="parser.high.dead",
        exchange="parser.dead",
        routing_key="high",
        queue_type="quorum",
    ),
    QueueDefinition(
        name="parser.bulk.dead",
        exchange="parser.dead",
        routing_key="bulk",
        queue_type="quorum",
    ),
    QueueDefinition(
        name="normalize.high.dead",
        exchange="normalize.dead",
        routing_key="high",
        queue_type="quorum",
    ),
    QueueDefinition(
        name="normalize.bulk.dead",
        exchange="normalize.dead",
        routing_key="bulk",
        queue_type="quorum",
    ),
    QueueDefinition(
        name="card.updated.dead",
        exchange="delivery.dead",
        routing_key="card.updated",
        queue_type="quorum",
    ),
    QueueDefinition(
        name="review.required.dead",
        exchange="delivery.dead",
        routing_key="review.required",
        queue_type="quorum",
    ),
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


def retry_queue_for(queue_name: str) -> str:
    definition = get_queue_definition(queue_name)
    if definition.name.endswith(".retry"):
        return definition.name
    retry_queue_name = f"{definition.name}.retry"
    get_queue_definition(retry_queue_name)
    return retry_queue_name


def dead_queue_for(queue_name: str) -> str:
    definition = get_queue_definition(queue_name)
    if definition.name.endswith(".dead"):
        return definition.name
    dead_queue_name = f"{definition.name}.dead"
    get_queue_definition(dead_queue_name)
    return dead_queue_name
