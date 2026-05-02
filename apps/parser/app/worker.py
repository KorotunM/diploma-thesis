from __future__ import annotations

from contextlib import ExitStack

from libs.storage import get_postgres_session_factory, get_rabbitmq_connection

from .crawl_requests import build_crawl_request_consumers
from .dependencies import (
    create_crawl_request_processing_service,
    create_parser_rabbitmq_consumer,
)


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


def run_parser_worker(
    *,
    prefetch_count: int | None = None,
    requeue_on_error: bool = True,
) -> None:
    rabbitmq_consumer = create_parser_rabbitmq_consumer()
    session_factory = get_postgres_session_factory(service_name="parser")
    service = SessionScopedCrawlRequestProcessingService(session_factory)
    consumers = build_crawl_request_consumers(
        rabbitmq_consumer=rabbitmq_consumer,
        service=service,
        prefetch_count=prefetch_count,
        requeue_on_error=requeue_on_error,
    )
    connection = get_rabbitmq_connection(service_name="parser")
    with connection:
        with ExitStack() as stack:
            for consumer in consumers.values():
                entered = stack.enter_context(consumer)
                entered.consume()
            while True:
                connection.drain_events()


def main() -> None:
    run_parser_worker()


if __name__ == "__main__":
    main()
