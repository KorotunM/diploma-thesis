from __future__ import annotations

from contextlib import ExitStack
import logging
import os

from libs.storage import (
    get_postgres_session_factory,
    get_rabbitmq_connection,
    run_resilient_worker_loop,
)

from .crawl_requests import build_crawl_request_consumers
from .dependencies import (
    create_crawl_request_processing_service,
    create_parser_rabbitmq_consumer,
)


LOGGER = logging.getLogger(__name__)


class SessionScopedCrawlRequestProcessingService:
    def __init__(self, session_factory) -> None:
        self._session_factory = session_factory

    async def process(self, event):
        session = self._session_factory()
        try:
            service = create_crawl_request_processing_service(session)
            result = await service.process(event)
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _run_parser_consumer_session(
    *,
    prefetch_count: int | None,
    requeue_on_error: bool,
) -> None:
    connection = get_rabbitmq_connection(service_name="parser")
    rabbitmq_consumer = create_parser_rabbitmq_consumer(connection=connection)
    session_factory = get_postgres_session_factory(service_name="parser")
    service = SessionScopedCrawlRequestProcessingService(session_factory)
    consumers = build_crawl_request_consumers(
        rabbitmq_consumer=rabbitmq_consumer,
        service=service,
        prefetch_count=prefetch_count,
        requeue_on_error=requeue_on_error,
    )

    with connection:
        with ExitStack() as stack:
            for consumer in consumers.values():
                entered = stack.enter_context(consumer)
                entered.consume()
            while True:
                connection.drain_events()


def run_parser_worker(
    *,
    prefetch_count: int | None = None,
    requeue_on_error: bool = True,
) -> None:
    initial_retry_seconds = _env_float("WORKER_RETRY_INITIAL_SECONDS", 2.0)
    max_retry_seconds = _env_float("WORKER_RETRY_MAX_SECONDS", 30.0)

    run_resilient_worker_loop(
        worker_name="parser",
        operation=lambda: _run_parser_consumer_session(
            prefetch_count=prefetch_count,
            requeue_on_error=requeue_on_error,
        ),
        initial_retry_seconds=initial_retry_seconds,
        max_retry_seconds=max_retry_seconds,
        logger=LOGGER,
    )


def main() -> None:
    run_parser_worker()


if __name__ == "__main__":
    main()
