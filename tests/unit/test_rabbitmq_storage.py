import json
from pathlib import Path

from libs.storage.rabbitmq.topology import EXCHANGE_DEFINITIONS, QUEUE_DEFINITIONS
from libs.storage.rabbitmq.transport import (
    RabbitMQConsumer,
    RabbitMQPublisher,
    build_rabbitmq_connection_options,
    build_rabbitmq_publish_retry_policy,
    create_rabbitmq_connection,
    declare_rabbitmq_topology,
)
from libs.storage.settings import PlatformSettings


class FakeConnectionClass:
    def __init__(self, url: str, **kwargs) -> None:
        self.url = url
        self.kwargs = kwargs


class FakeExchange:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.bound_to = None
        self.declared = False

    def maybe_bind(self, connection) -> None:
        self.bound_to = connection

    def declare(self) -> None:
        self.declared = True


class FakeQueue:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.bound_to = None
        self.declared = False

    def maybe_bind(self, connection) -> None:
        self.bound_to = connection

    def declare(self) -> None:
        self.declared = True


class FakeProducer:
    def __init__(self, channel, **kwargs) -> None:
        self.channel = channel
        self.kwargs = kwargs
        self.published: list[tuple[object, dict]] = []
        channel.created_producers.append(self)

    def publish(self, payload, **kwargs) -> None:
        self.published.append((payload, kwargs))


class FakeConsumerClass:
    def __init__(self, channel, **kwargs) -> None:
        self.channel = channel
        self.kwargs = kwargs


class FakeChannel:
    def __init__(self) -> None:
        self.prefetch_count = None
        self.created_producers: list[FakeProducer] = []

    def __enter__(self) -> "FakeChannel":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def basic_qos(self, *, prefetch_count: int) -> None:
        self.prefetch_count = prefetch_count


class FakeConnection:
    def __init__(self) -> None:
        self.channels: list[FakeChannel] = []

    def channel(self) -> FakeChannel:
        channel = FakeChannel()
        self.channels.append(channel)
        return channel


class FakeMessage:
    def __init__(self) -> None:
        self.acked = False
        self.rejected_with: list[bool] = []

    def ack(self) -> None:
        self.acked = True

    def reject(self, *, requeue: bool) -> None:
        self.rejected_with.append(requeue)


class FakeRuntime:
    connection_class = FakeConnectionClass
    exchange_class = FakeExchange
    queue_class = FakeQueue
    producer_class = FakeProducer
    consumer_class = FakeConsumerClass


def test_topology_matches_rabbitmq_definitions_file() -> None:
    definitions = json.loads(Path("infra/rabbitmq/definitions.json").read_text(encoding="utf-8"))

    exchanges_by_name = {item["name"]: item for item in definitions["exchanges"]}
    queues_by_name = {item["name"]: item for item in definitions["queues"]}

    assert {item.name for item in EXCHANGE_DEFINITIONS} == set(exchanges_by_name)
    assert {item.name for item in QUEUE_DEFINITIONS} == set(queues_by_name)

    for definition in EXCHANGE_DEFINITIONS:
        expected = exchanges_by_name[definition.name]
        assert definition.exchange_type == expected["type"]
        assert definition.durable is expected["durable"]
        assert definition.auto_delete is expected["auto_delete"]
        assert definition.arguments == expected["arguments"]

    for definition in QUEUE_DEFINITIONS:
        expected = queues_by_name[definition.name]
        assert definition.durable is expected["durable"]
        assert definition.auto_delete is expected["auto_delete"]
        assert definition.queue_arguments() == expected["arguments"]


def test_build_rabbitmq_connection_options_and_retry_policy() -> None:
    settings = PlatformSettings(service_name="scheduler").rabbitmq

    assert build_rabbitmq_connection_options(settings) == {
        "heartbeat": 30,
        "connect_timeout": 5,
    }
    assert build_rabbitmq_publish_retry_policy(settings) == {
        "max_retries": 3,
        "interval_start": 0,
        "interval_step": 1,
        "interval_max": 3,
    }


def test_create_rabbitmq_connection_uses_settings_url(monkeypatch) -> None:
    monkeypatch.setattr(
        "libs.storage.rabbitmq.transport._load_kombu_runtime",
        lambda: FakeRuntime(),
    )

    settings = PlatformSettings(service_name="scheduler").rabbitmq
    connection = create_rabbitmq_connection(settings)

    assert isinstance(connection, FakeConnectionClass)
    assert connection.url == "amqp://aggregator:aggregator@rabbitmq:5672/%2F"
    assert connection.kwargs == {
        "heartbeat": 30,
        "connect_timeout": 5,
    }


def test_declare_rabbitmq_topology_binds_and_declares_all_entities(monkeypatch) -> None:
    monkeypatch.setattr(
        "libs.storage.rabbitmq.transport._load_kombu_runtime",
        lambda: FakeRuntime(),
    )

    connection = FakeConnection()
    exchanges, queues = declare_rabbitmq_topology(connection)

    assert set(exchanges) == {definition.name for definition in EXCHANGE_DEFINITIONS}
    assert set(queues) == {definition.name for definition in QUEUE_DEFINITIONS}
    assert all(
        exchange.bound_to is connection and exchange.declared
        for exchange in exchanges.values()
    )
    assert all(queue.bound_to is connection and queue.declared for queue in queues.values())


def test_rabbitmq_publisher_routes_messages_via_declared_queue_topology(monkeypatch) -> None:
    monkeypatch.setattr(
        "libs.storage.rabbitmq.transport._load_kombu_runtime",
        lambda: FakeRuntime(),
    )

    settings = PlatformSettings(service_name="scheduler").rabbitmq
    connection = FakeConnection()
    publisher = RabbitMQPublisher(connection, settings)

    result = publisher.publish(
        {"event_name": "crawl.request.v1"},
        queue_name="parser.high",
        headers={"x-trace-id": "abc"},
    )

    assert result.queue_name == "parser.high"
    assert result.exchange_name == "parser.jobs"
    assert result.routing_key == "high"

    channel = connection.channels[0]
    producer = channel.created_producers[0]
    assert producer.kwargs["routing_key"] == "high"
    payload, publish_kwargs = producer.published[0]
    assert payload == {"event_name": "crawl.request.v1"}
    assert publish_kwargs["headers"] == {"x-trace-id": "abc"}
    assert publish_kwargs["retry"] is True
    assert publish_kwargs["delivery_mode"] == 2
    assert publish_kwargs["retry_policy"]["max_retries"] == 3
    assert len(publish_kwargs["declare"]) == 2


def test_rabbitmq_consumer_sets_qos_and_acks_success(monkeypatch) -> None:
    monkeypatch.setattr(
        "libs.storage.rabbitmq.transport._load_kombu_runtime",
        lambda: FakeRuntime(),
    )

    settings = PlatformSettings(service_name="parser").rabbitmq
    connection = FakeConnection()
    consumer_helper = RabbitMQConsumer(connection, settings)
    handled: list[dict] = []

    consumer = consumer_helper.build_consumer(
        queue_name="parser.bulk",
        handler=lambda body, message: handled.append(body),
    )

    assert isinstance(consumer, FakeConsumerClass)
    assert connection.channels[0].prefetch_count == 16

    callback = consumer.kwargs["callbacks"][0]
    message = FakeMessage()
    callback({"payload": 1}, message)

    assert handled == [{"payload": 1}]
    assert message.acked is True
    assert message.rejected_with == []


def test_rabbitmq_consumer_rejects_failed_messages_with_requeue_flag(monkeypatch) -> None:
    monkeypatch.setattr(
        "libs.storage.rabbitmq.transport._load_kombu_runtime",
        lambda: FakeRuntime(),
    )

    settings = PlatformSettings(service_name="parser").rabbitmq
    connection = FakeConnection()
    consumer_helper = RabbitMQConsumer(connection, settings)

    consumer = consumer_helper.build_consumer(
        queue_name="parser.bulk",
        handler=lambda body, message: (_ for _ in ()).throw(
            ValueError("broken handler")
        ),
        requeue_on_error=True,
    )

    callback = consumer.kwargs["callbacks"][0]
    message = FakeMessage()

    try:
        callback({"payload": 1}, message)
    except ValueError as exc:
        assert str(exc) == "broken handler"
    else:
        raise AssertionError("Expected consumer callback to re-raise handler failure")

    assert message.acked is False
    assert message.rejected_with == [True]
