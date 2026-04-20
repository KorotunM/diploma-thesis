from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from libs.contracts.events import CrawlRequestEvent
from libs.storage import RabbitMQConsumer

from .models import CrawlRequestProcessingResult
from .service import CrawlRequestProcessingService

PARSER_HIGH_QUEUE = "parser.high"
PARSER_BULK_QUEUE = "parser.bulk"
PARSER_CRAWL_REQUEST_QUEUES = (PARSER_HIGH_QUEUE, PARSER_BULK_QUEUE)


class CrawlRequestConsumer:
    def __init__(
        self,
        *,
        service: CrawlRequestProcessingService,
        async_runner: Callable[[Any], CrawlRequestProcessingResult] | None = None,
    ) -> None:
        self._service = service
        self._async_runner = async_runner or asyncio.run

    def handle_message(
        self,
        body: Any,
        _message: Any | None = None,
    ) -> CrawlRequestProcessingResult:
        event = CrawlRequestEvent.model_validate(body)
        return self._async_runner(self._service.process(event))


def build_crawl_request_consumer(
    *,
    rabbitmq_consumer: RabbitMQConsumer,
    service: CrawlRequestProcessingService,
    queue_name: str = PARSER_BULK_QUEUE,
    prefetch_count: int | None = None,
    requeue_on_error: bool = False,
) -> Any:
    crawl_request_consumer = CrawlRequestConsumer(service=service)
    return rabbitmq_consumer.build_consumer(
        queue_name=queue_name,
        handler=crawl_request_consumer.handle_message,
        accept=("json",),
        prefetch_count=prefetch_count,
        requeue_on_error=requeue_on_error,
    )


def build_crawl_request_consumers(
    *,
    rabbitmq_consumer: RabbitMQConsumer,
    service: CrawlRequestProcessingService,
    queue_names: tuple[str, ...] = PARSER_CRAWL_REQUEST_QUEUES,
    prefetch_count: int | None = None,
    requeue_on_error: bool = False,
) -> dict[str, Any]:
    return {
        queue_name: build_crawl_request_consumer(
            rabbitmq_consumer=rabbitmq_consumer,
            service=service,
            queue_name=queue_name,
            prefetch_count=prefetch_count,
            requeue_on_error=requeue_on_error,
        )
        for queue_name in queue_names
    }
