from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

from libs.storage.rabbitmq.topology import QUEUE_DEFINITIONS, dead_queue_for, retry_queue_for
from libs.storage.rabbitmq.transport import RabbitMQConsumer, RabbitMQPublisher
from libs.storage.settings import PlatformSettings


@dataclass(frozen=True)
class BrokerEnvelope:
    payload: Any
    headers: dict[str, Any]


class InMemoryBroker:
    def __init__(self) -> None:
        self._messages: dict[str, list[BrokerEnvelope]] = defaultdict(list)

    def route_via_exchange(
        self,
        *,
        exchange_name: str,
        routing_key: str,
        payload: Any,
        headers: dict[str, Any] | None = None,
    ) -> None:
        envelope = BrokerEnvelope(payload=payload, headers=dict(headers or {}))
        for definition in QUEUE_DEFINITIONS:
            if definition.exchange == exchange_name and definition.routing_key == routing_key:
                self._messages[definition.name].append(envelope)

    def pop(self, queue_name: str) -> BrokerEnvelope | None:
        messages = self._messages[queue_name]
        if not messages:
            return None
        return messages.pop(0)

    def requeue(self, queue_name: str, envelope: BrokerEnvelope) -> None:
        self._messages[queue_name].append(envelope)

    def expire_retry_queue(self, queue_name: str) -> None:
        definition = next(item for item in QUEUE_DEFINITIONS if item.name == queue_name)
        staged = list(self._messages[queue_name])
        self._messages[queue_name].clear()
        for envelope in staged:
            self.route_via_exchange(
                exchange_name=definition.dead_letter_exchange or "",
                routing_key=definition.dead_letter_routing_key or "",
                payload=envelope.payload,
                headers=envelope.headers,
            )

    def depth(self, queue_name: str) -> int:
        return len(self._messages[queue_name])


class BrokerConnection:
    def __init__(self, broker: InMemoryBroker) -> None:
        self._broker = broker
        self.channels: list[BrokerChannel] = []

    def channel(self) -> BrokerChannel:
        channel = BrokerChannel(self._broker)
        self.channels.append(channel)
        return channel


class BrokerChannel:
    def __init__(self, broker: InMemoryBroker) -> None:
        self.broker = broker
        self.prefetch_count: int | None = None

    def __enter__(self) -> BrokerChannel:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def basic_qos(self, *, prefetch_count: int) -> None:
        self.prefetch_count = prefetch_count


class BrokerExchange:
    def __init__(self, **kwargs) -> None:
        self.name = kwargs["name"]
        self.exchange_type = kwargs["type"]
        self.kwargs = kwargs

    def maybe_bind(self, connection) -> None:
        return None

    def declare(self) -> None:
        return None


class BrokerQueue:
    def __init__(self, **kwargs) -> None:
        self.name = kwargs["name"]
        self.exchange = kwargs["exchange"]
        self.routing_key = kwargs["routing_key"]
        self.kwargs = kwargs

    def maybe_bind(self, connection) -> None:
        return None

    def declare(self) -> None:
        return None


class BrokerProducer:
    def __init__(self, channel: BrokerChannel, **kwargs) -> None:
        self._channel = channel
        self._exchange = kwargs["exchange"]
        self._routing_key = kwargs["routing_key"]

    def publish(self, payload, **kwargs) -> None:
        self._channel.broker.route_via_exchange(
            exchange_name=self._exchange.name,
            routing_key=self._routing_key,
            payload=payload,
            headers=kwargs.get("headers"),
        )


class BrokerConsumerClass:
    def __init__(self, channel: BrokerChannel, **kwargs) -> None:
        self.channel = channel
        self.kwargs = kwargs


class BrokerMessage:
    def __init__(
        self,
        *,
        broker: InMemoryBroker,
        queue_name: str,
        envelope: BrokerEnvelope,
    ) -> None:
        self._broker = broker
        self._queue_name = queue_name
        self._envelope = envelope
        self.acked = False
        self.rejected_with: list[bool] = []

    @property
    def headers(self) -> dict[str, Any]:
        return dict(self._envelope.headers)

    def ack(self) -> None:
        self.acked = True

    def reject(self, *, requeue: bool) -> None:
        self.rejected_with.append(requeue)
        if requeue:
            self._broker.requeue(self._queue_name, self._envelope)
            return
        definition = next(item for item in QUEUE_DEFINITIONS if item.name == self._queue_name)
        self._broker.route_via_exchange(
            exchange_name=definition.dead_letter_exchange or "",
            routing_key=definition.dead_letter_routing_key or "",
            payload=self._envelope.payload,
            headers=self._envelope.headers,
        )


def build_runtime() -> SimpleNamespace:
    return SimpleNamespace(
        exchange_class=BrokerExchange,
        queue_class=BrokerQueue,
        producer_class=BrokerProducer,
        consumer_class=BrokerConsumerClass,
    )


def deliver_one(consumer: BrokerConsumerClass, broker: InMemoryBroker) -> BrokerMessage:
    queue = consumer.kwargs["queues"][0]
    callback: Callable[[Any, Any], None] = consumer.kwargs["callbacks"][0]
    envelope = broker.pop(queue.name)
    if envelope is None:
        raise AssertionError(f"Queue {queue.name} is empty.")
    message = BrokerMessage(
        broker=broker,
        queue_name=queue.name,
        envelope=envelope,
    )
    callback(envelope.payload, message)
    return message


@pytest.mark.parametrize(
    "queue_name",
    [
        "parser.high",
        "normalize.bulk",
        "card.updated",
    ],
)
def test_retry_lane_redrives_message_after_controlled_transient_worker_failure(
    queue_name: str,
) -> None:
    settings = PlatformSettings(service_name="scheduler").rabbitmq
    broker = InMemoryBroker()
    connection = BrokerConnection(broker)
    runtime = build_runtime()
    publisher = RabbitMQPublisher(connection, settings, runtime=runtime)
    consumer_helper = RabbitMQConsumer(connection, settings, runtime=runtime)
    retry_queue_name = retry_queue_for(queue_name)
    dead_queue_name = dead_queue_for(queue_name)
    attempts = 0
    handled: list[dict[str, Any]] = []

    def handler(body: Any, message: Any) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            publisher.publish(
                body,
                queue_name=retry_queue_name,
                headers=message.headers,
            )
            return
        handled.append(body)

    consumer = consumer_helper.build_consumer(
        queue_name=queue_name,
        handler=handler,
        requeue_on_error=False,
    )

    publisher.publish(
        {"event_name": "worker.test.v1", "queue_name": queue_name},
        queue_name=queue_name,
        headers={"event_name": "worker.test.v1", "queue_name": queue_name},
    )

    first_delivery = deliver_one(consumer, broker)
    assert first_delivery.acked is True
    assert first_delivery.rejected_with == []
    assert broker.depth(queue_name) == 0
    assert broker.depth(retry_queue_name) == 1

    broker.expire_retry_queue(retry_queue_name)

    assert broker.depth(retry_queue_name) == 0
    assert broker.depth(queue_name) == 1
    assert broker.depth(dead_queue_name) == 0

    second_delivery = deliver_one(consumer, broker)

    assert second_delivery.acked is True
    assert second_delivery.rejected_with == []
    assert handled == [{"event_name": "worker.test.v1", "queue_name": queue_name}]
    assert broker.depth(queue_name) == 0
    assert broker.depth(dead_queue_name) == 0


@pytest.mark.parametrize(
    "queue_name",
    [
        "parser.high",
        "normalize.bulk",
        "card.updated",
    ],
)
def test_dead_letter_lane_captures_message_after_terminal_worker_failure(
    queue_name: str,
) -> None:
    settings = PlatformSettings(service_name="scheduler").rabbitmq
    broker = InMemoryBroker()
    connection = BrokerConnection(broker)
    runtime = build_runtime()
    publisher = RabbitMQPublisher(connection, settings, runtime=runtime)
    consumer_helper = RabbitMQConsumer(connection, settings, runtime=runtime)
    dead_queue_name = dead_queue_for(queue_name)

    def failing_handler(body: Any, message: Any) -> None:
        raise RuntimeError(f"controlled failure for {queue_name}")

    consumer = consumer_helper.build_consumer(
        queue_name=queue_name,
        handler=failing_handler,
        requeue_on_error=False,
    )

    publisher.publish(
        {"event_name": "worker.test.v1", "queue_name": queue_name},
        queue_name=queue_name,
        headers={"event_name": "worker.test.v1", "queue_name": queue_name},
    )

    with pytest.raises(RuntimeError, match="controlled failure"):
        deliver_one(consumer, broker)

    assert broker.depth(queue_name) == 0
    assert broker.depth(dead_queue_name) == 1
    dead_letter = broker.pop(dead_queue_name)
    assert dead_letter is not None
    assert dead_letter.payload == {
        "event_name": "worker.test.v1",
        "queue_name": queue_name,
    }
    assert dead_letter.headers["event_name"] == "worker.test.v1"
