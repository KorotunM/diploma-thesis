from __future__ import annotations

import os
from time import sleep

from libs.storage import (
    RabbitMQPublisher,
    declare_rabbitmq_topology,
    get_platform_settings,
    get_postgres_session_factory,
    get_rabbitmq_connection,
)

from apps.scheduler.app.runs import PipelineRunRepository

from .scheduled import (
    ScheduledCrawlRepository,
    ScheduledCrawlService,
    ScheduledCrawlSweepRequest,
)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def run_scheduler_worker(
    *,
    poll_interval_seconds: int | None = None,
    batch_limit: int | None = None,
    priority: str | None = None,
) -> None:
    settings = get_platform_settings(service_name="scheduler")
    interval = poll_interval_seconds or _env_int("SCHEDULER_POLL_INTERVAL_SECONDS", 30)
    limit = batch_limit or _env_int("SCHEDULER_CRAWL_BATCH_LIMIT", 100)
    crawl_priority = priority or os.getenv("SCHEDULER_CRAWL_PRIORITY", "bulk")
    session_factory = get_postgres_session_factory(service_name="scheduler")
    connection = get_rabbitmq_connection(service_name="scheduler")
    declare_rabbitmq_topology(connection)

    with connection:
        publisher = RabbitMQPublisher(connection, settings.rabbitmq)
        while True:
            session = session_factory()
            try:
                ScheduledCrawlService(
                    repository=ScheduledCrawlRepository(session),
                    run_repository=PipelineRunRepository(session),
                    publisher=publisher,
                ).run(
                    ScheduledCrawlSweepRequest(
                        priority="high" if crawl_priority == "high" else "bulk",
                        limit=limit,
                    )
                )
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()
            sleep(interval)


def main() -> None:
    run_scheduler_worker()


if __name__ == "__main__":
    main()
