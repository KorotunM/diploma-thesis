from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from libs.storage.settings import RabbitMQSettings, get_platform_settings

_log = logging.getLogger(__name__)

from .topology import (
    EXCHANGE_DEFINITIONS,
    QUEUE_DEFINITIONS,
    get_queue_definition,
)


@dataclass(frozen=True)
class KombuRuntime:
    connection_class: type[Any]
    exchange_class: type[Any]
    queue_class: type[Any]
    producer_class: type[Any]
    consumer_class: type[Any]


@dataclass(frozen=True)
class RabbitMQPublishResult:
    queue_name: str
    exchange_name: str
    routing_key: str


def _load_kombu_runtime() -> KombuRuntime:
    try:
        from kombu import Connection, Consumer, Exchange, Producer, Queue
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Kombu RabbitMQ dependencies are not installed. "
            "Install worker/runtime dependencies before using rabbitmq storage helpers."
        ) from exc

    return KombuRuntime(
        connection_class=Connection,
        exchange_class=Exchange,
        queue_class=Queue,
        producer_class=Producer,
        consumer_class=Consumer,
    )


def build_rabbitmq_connection_options(settings: RabbitMQSettings) -> dict[str, Any]:
    return {
        "heartbeat": settings.heartbeat_seconds,
        "connect_timeout": settings.connection_timeout_seconds,
    }


def build_rabbitmq_publish_retry_policy(settings: RabbitMQSettings) -> dict[str, Any]:
    return {
        "max_retries": settings.publish_retry_max_retries,
        "interval_start": 0,
        "interval_step": 1,
        "interval_max": 3,
    }


def create_rabbitmq_connection(settings: RabbitMQSettings) -> Any:
    runtime = _load_kombu_runtime()
    return runtime.connection_class(
        settings.url,
        **build_rabbitmq_connection_options(settings),
    )


def create_exchange_entities(runtime: KombuRuntime | None = None) -> dict[str, Any]:
    resolved_runtime = runtime or _load_kombu_runtime()
    return {
        definition.name: resolved_runtime.exchange_class(
            name=definition.name,
            type=definition.exchange_type,
            durable=definition.durable,
            auto_delete=definition.auto_delete,
            arguments=dict(definition.arguments),
        )
        for definition in EXCHANGE_DEFINITIONS
    }


def create_queue_entities(
    exchanges: dict[str, Any] | None = None,
    runtime: KombuRuntime | None = None,
) -> dict[str, Any]:
    resolved_runtime = runtime or _load_kombu_runtime()
    resolved_exchanges = exchanges or create_exchange_entities(runtime=resolved_runtime)
    return {
        definition.name: resolved_runtime.queue_class(
            name=definition.name,
            exchange=resolved_exchanges[definition.exchange],
            routing_key=definition.routing_key,
            durable=definition.durable,
            auto_delete=definition.auto_delete,
            queue_arguments=definition.queue_arguments(),
        )
        for definition in QUEUE_DEFINITIONS
    }


def declare_rabbitmq_topology(
    connection: Any,
    *,
    runtime: KombuRuntime | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved_runtime = runtime or _load_kombu_runtime()
    exchanges = create_exchange_entities(runtime=resolved_runtime)
    queues = create_queue_entities(exchanges=exchanges, runtime=resolved_runtime)

    for exchange in exchanges.values():
        exchange.maybe_bind(connection)
        exchange.declare()

    for queue in queues.values():
        queue.maybe_bind(connection)
        queue.declare()

    return exchanges, queues


class RabbitMQPublisher:
    def __init__(
        self,
        connection: Any,
        settings: RabbitMQSettings,
        *,
        runtime: KombuRuntime | None = None,
        exchanges: dict[str, Any] | None = None,
        queues: dict[str, Any] | None = None,
    ) -> None:
        self._connection = connection
        self._settings = settings
        self._runtime = runtime or _load_kombu_runtime()
        self._exchanges = exchanges or create_exchange_entities(runtime=self._runtime)
        self._queues = queues or create_queue_entities(
            exchanges=self._exchanges,
            runtime=self._runtime,
        )

    def publish(
        self,
        payload: Any,
        *,
        queue_name: str,
        headers: dict[str, Any] | None = None,
        serializer: str = "json",
        content_type: str | None = None,
        content_encoding: str | None = None,
        retry: bool | None = None,
        extra_publish_kwargs: dict[str, Any] | None = None,
    ) -> RabbitMQPublishResult:
        queue_definition = get_queue_definition(queue_name)
        exchange = self._exchanges[queue_definition.exchange]
        queue = self._queues[queue_name]

        publish_kwargs = {
            "headers": headers or {},
            "declare": [exchange, queue],
            "retry": self._settings.publish_retry if retry is None else retry,
            "retry_policy": build_rabbitmq_publish_retry_policy(self._settings),
            "serializer": serializer,
            "delivery_mode": 2,
        }
        if content_type is not None:
            publish_kwargs["content_type"] = content_type
        if content_encoding is not None:
            publish_kwargs["content_encoding"] = content_encoding
        if extra_publish_kwargs:
            publish_kwargs.update(extra_publish_kwargs)

        with self._connection.channel() as channel:
            producer = self._runtime.producer_class(
                channel,
                exchange=exchange,
                routing_key=queue_definition.routing_key,
                serializer=serializer,
            )
            producer.publish(payload, **publish_kwargs)

        return RabbitMQPublishResult(
            queue_name=queue_definition.name,
            exchange_name=queue_definition.exchange,
            routing_key=queue_definition.routing_key,
        )


class RabbitMQConsumer:
    def __init__(
        self,
        connection: Any,
        settings: RabbitMQSettings,
        *,
        runtime: KombuRuntime | None = None,
        exchanges: dict[str, Any] | None = None,
        queues: dict[str, Any] | None = None,
    ) -> None:
        self._connection = connection
        self._settings = settings
        self._runtime = runtime or _load_kombu_runtime()
        self._exchanges = exchanges or create_exchange_entities(runtime=self._runtime)
        self._queues = queues or create_queue_entities(
            exchanges=self._exchanges,
            runtime=self._runtime,
        )

    def build_consumer(
        self,
        *,
        queue_name: str,
        handler: Callable[[Any, Any], None],
        accept: tuple[str, ...] = ("json",),
        prefetch_count: int | None = None,
        requeue_on_error: bool = False,
    ) -> Any:
        queue = self._queues[queue_name]
        channel = self._connection.channel()
        channel.basic_qos(
            prefetch_size=0,
            prefetch_count=prefetch_count or self._settings.prefetch_count,
            a_global=False,
        )

        def callback(body: Any, message: Any) -> None:
            try:
                handler(body, message)
            except Exception as exc:
                _log.error(
                    "Message processing failed — rejecting (requeue=%s): %s",
                    requeue_on_error,
                    exc,
                    exc_info=True,
                )
                message.reject(requeue=requeue_on_error)
            else:
                message.ack()

        return self._runtime.consumer_class(
            channel,
            queues=[queue],
            callbacks=[callback],
            accept=list(accept),
        )


def get_rabbitmq_connection(
    service_name: str | None = None,
    app_env: str | None = None,
) -> Any:
    settings = get_platform_settings(service_name=service_name, app_env=app_env)
    return create_rabbitmq_connection(settings.rabbitmq)
