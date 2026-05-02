from __future__ import annotations

from typing import Any

from libs.contracts.events import ParseCompletedEvent
from libs.storage import RabbitMQConsumer

from .models import ParseCompletedProcessingResult
from .service import ParseCompletedProcessingService

NORMALIZE_HIGH_QUEUE = "normalize.high"
NORMALIZE_BULK_QUEUE = "normalize.bulk"
NORMALIZER_PARSE_COMPLETED_QUEUES = (NORMALIZE_HIGH_QUEUE, NORMALIZE_BULK_QUEUE)


class ParseCompletedConsumer:
    def __init__(
        self,
        *,
        service: ParseCompletedProcessingService,
    ) -> None:
        self._service = service

    def handle_message(
        self,
        body: Any,
        _message: Any | None = None,
    ) -> ParseCompletedProcessingResult:
        event = ParseCompletedEvent.model_validate(body)
        return self._service.process(event)


def build_parse_completed_consumer(
    *,
    rabbitmq_consumer: RabbitMQConsumer,
    service: ParseCompletedProcessingService,
    queue_name: str = NORMALIZE_BULK_QUEUE,
    prefetch_count: int | None = None,
    requeue_on_error: bool = False,
) -> Any:
    consumer = ParseCompletedConsumer(service=service)
    return rabbitmq_consumer.build_consumer(
        queue_name=queue_name,
        handler=consumer.handle_message,
        accept=("json",),
        prefetch_count=prefetch_count,
        requeue_on_error=requeue_on_error,
    )


def build_parse_completed_consumers(
    *,
    rabbitmq_consumer: RabbitMQConsumer,
    service: ParseCompletedProcessingService,
    queue_names: tuple[str, ...] = NORMALIZER_PARSE_COMPLETED_QUEUES,
    prefetch_count: int | None = None,
    requeue_on_error: bool = False,
) -> dict[str, Any]:
    return {
        queue_name: build_parse_completed_consumer(
            rabbitmq_consumer=rabbitmq_consumer,
            service=service,
            queue_name=queue_name,
            prefetch_count=prefetch_count,
            requeue_on_error=requeue_on_error,
        )
        for queue_name in queue_names
    }
