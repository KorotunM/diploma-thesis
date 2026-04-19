import pytest

pytest.importorskip("fastapi")
pytest.importorskip("prometheus_client")

from apps.scheduler.app.main import app


def test_app_exists() -> None:
    assert app.title == "scheduler"
