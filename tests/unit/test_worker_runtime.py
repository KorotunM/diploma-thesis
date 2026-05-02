from __future__ import annotations

import pytest

from libs.storage.worker import is_transient_dependency_error, run_resilient_worker_loop


class Done(Exception):
    pass


def test_transient_dependency_error_recognizes_socket_failures() -> None:
    assert is_transient_dependency_error(ConnectionRefusedError(111, "refused")) is True


def test_run_resilient_worker_loop_retries_transient_errors_before_retrying() -> None:
    attempts: list[str] = []
    sleep_calls: list[float] = []

    def operation() -> None:
        attempts.append("attempt")
        if len(attempts) == 1:
            raise ConnectionRefusedError(111, "refused")
        raise Done()

    with pytest.raises(Done):
        run_resilient_worker_loop(
            worker_name="parser",
            operation=operation,
            initial_retry_seconds=1.5,
            max_retry_seconds=5.0,
            sleep_fn=sleep_calls.append,
        )

    assert len(attempts) == 2
    assert sleep_calls == [1.5]
