from __future__ import annotations

import logging
from collections.abc import Callable
from time import sleep


def is_transient_dependency_error(exc: BaseException) -> bool:
    if isinstance(exc, OSError):
        return True

    transient_error_types: list[type[BaseException]] = []
    try:
        from kombu.exceptions import OperationalError as KombuOperationalError
    except ModuleNotFoundError:
        pass
    else:
        transient_error_types.append(KombuOperationalError)

    try:
        from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError
    except ModuleNotFoundError:
        pass
    else:
        transient_error_types.append(SQLAlchemyOperationalError)

    return any(isinstance(exc, error_type) for error_type in transient_error_types)


def run_resilient_worker_loop(
    *,
    worker_name: str,
    operation: Callable[[], None],
    initial_retry_seconds: float = 2.0,
    max_retry_seconds: float = 30.0,
    sleep_fn: Callable[[float], None] = sleep,
    logger: logging.Logger | None = None,
) -> None:
    resolved_logger = logger or logging.getLogger(f"{worker_name}.worker")
    retry_seconds = initial_retry_seconds if initial_retry_seconds > 0 else 2.0
    retry_ceiling = max_retry_seconds if max_retry_seconds > 0 else retry_seconds

    while True:
        try:
            operation()
            retry_seconds = initial_retry_seconds if initial_retry_seconds > 0 else 2.0
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            if not is_transient_dependency_error(exc):
                raise

            resolved_logger.warning(
                "Transient dependency failure in %s worker: %s. Retrying in %.1fs.",
                worker_name,
                exc,
                retry_seconds,
            )
            sleep_fn(retry_seconds)
            retry_seconds = min(retry_seconds * 2, retry_ceiling)
