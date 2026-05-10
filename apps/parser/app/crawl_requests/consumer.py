from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from libs.contracts.events import CrawlRequestEvent
from libs.source_sdk import TransientFetchError
from libs.storage import RabbitMQConsumer
from libs.storage.rabbitmq.topology import retry_queue_for

from .models import CrawlRequestProcessingResult
from .service import CrawlRequestProcessingService

PARSER_HIGH_QUEUE = "parser.high"
PARSER_BULK_QUEUE = "parser.bulk"
PARSER_CRAWL_REQUEST_QUEUES = (PARSER_HIGH_QUEUE, PARSER_BULK_QUEUE)
TRANSIENT_RETRY_HEADER = "x-parser-transient-retry-count"
TRANSIENT_RETRY_ERROR_HEADER = "x-parser-last-transient-error"
DEFAULT_MAX_TRANSIENT_RETRIES = 3

LOGGER = logging.getLogger(__name__)


class CrawlRequestConsumer:
    def __init__(
        self,
        *,
        service: CrawlRequestProcessingService,
        async_runner: Callable[[Any], CrawlRequestProcessingResult] | None = None,
        retry_publisher: Any | None = None,
        queue_name: str = PARSER_BULK_QUEUE,
        max_transient_retries: int = DEFAULT_MAX_TRANSIENT_RETRIES,
    ) -> None:
        self._service = service
        self._async_runner = async_runner or asyncio.run
        self._retry_publisher = retry_publisher
        self._queue_name = queue_name
        self._max_transient_retries = max(0, max_transient_retries)

    def handle_message(
        self,
        body: Any,
        message: Any | None = None,
    ) -> CrawlRequestProcessingResult | None:
        event = CrawlRequestEvent.model_validate(body)
        try:
            return self._async_runner(self._service.process(event))
        except TransientFetchError as exc:
            if self._publish_retry(body=body, message=message, exc=exc):
                return None
            raise

    def _publish_retry(
        self,
        *,
        body: Any,
        message: Any | None,
        exc: TransientFetchError,
    ) -> bool:
        if self._retry_publisher is None or message is None:
            return False
        headers = dict(getattr(message, "headers", {}) or {})
        retry_count = _read_retry_count(headers) + 1
        if retry_count > self._max_transient_retries:
            LOGGER.warning(
                "Fetch for %s timed out after %s transient retries. Sending to dead-letter.",
                exc.endpoint_url,
                self._max_transient_retries,
            )
            return False

        headers[TRANSIENT_RETRY_HEADER] = retry_count
        headers[TRANSIENT_RETRY_ERROR_HEADER] = type(exc.__cause__ or exc).__name__
        retry_queue_name = retry_queue_for(self._queue_name)
        self._retry_publisher.publish(
            body,
            queue_name=retry_queue_name,
            headers=headers,
        )
        LOGGER.warning(
            "Transient fetch failure for %s. Scheduled retry %s/%s via %s.",
            exc.endpoint_url,
            retry_count,
            self._max_transient_retries,
            retry_queue_name,
        )
        return True


def build_crawl_request_consumer(
    *,
    rabbitmq_consumer: RabbitMQConsumer,
    service: CrawlRequestProcessingService,
    queue_name: str = PARSER_BULK_QUEUE,
    prefetch_count: int | None = None,
    requeue_on_error: bool = False,
    retry_publisher: Any | None = None,
    max_transient_retries: int = DEFAULT_MAX_TRANSIENT_RETRIES,
) -> Any:
    crawl_request_consumer = CrawlRequestConsumer(
        service=service,
        retry_publisher=retry_publisher,
        queue_name=queue_name,
        max_transient_retries=max_transient_retries,
    )
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
    retry_publisher: Any | None = None,
    max_transient_retries: int = DEFAULT_MAX_TRANSIENT_RETRIES,
) -> dict[str, Any]:
    return {
        queue_name: build_crawl_request_consumer(
            rabbitmq_consumer=rabbitmq_consumer,
            service=service,
            queue_name=queue_name,
            prefetch_count=prefetch_count,
            requeue_on_error=requeue_on_error,
            retry_publisher=retry_publisher,
            max_transient_retries=max_transient_retries,
        )
        for queue_name in queue_names
    }


def _read_retry_count(headers: dict[str, Any]) -> int:
    value = headers.get(TRANSIENT_RETRY_HEADER)
    if isinstance(value, int):
        return max(0, value)
    if isinstance(value, str):
        try:
            return max(0, int(value))
        except ValueError:
            return 0
    return 0
