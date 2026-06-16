from __future__ import annotations

import logging
import os

from fastapi import APIRouter, FastAPI
from prometheus_client import make_asgi_app, start_http_server

from libs.contracts.dto import HealthResponse
from libs.storage import get_platform_settings

LOGGER = logging.getLogger(__name__)

DEFAULT_WORKER_METRICS_PORT = 9100


def create_service_app(service_name: str, description: str) -> FastAPI:
    settings = get_platform_settings(service_name=service_name)
    service = settings.service
    app = FastAPI(title=service_name, description=description, version=service.version)
    app.state.platform_settings = settings
    router = APIRouter(tags=["platform"])

    dependencies = {
        "postgres": "configured" if settings.postgres.sqlalchemy_dsn else "missing",
        "rabbitmq": "configured" if settings.rabbitmq.url else "missing",
        "minio": "configured" if settings.minio.endpoint else "missing",
    }

    @router.get("/healthz", response_model=HealthResponse, summary="Liveness probe")
    def healthz() -> HealthResponse:
        return HealthResponse(
            service=service_name,
            environment=service.environment,
            version=service.version,
            dependencies=dependencies,
        )

    @router.get("/readyz", response_model=HealthResponse, summary="Readiness probe")
    def readyz() -> HealthResponse:
        return HealthResponse(
            service=service_name,
            environment=service.environment,
            version=service.version,
            dependencies=dependencies,
        )

    app.include_router(router)
    app.mount("/metrics", make_asgi_app())
    return app


def start_worker_metrics_server(*, default_port: int = DEFAULT_WORKER_METRICS_PORT) -> bool:
    """Expose the default Prometheus registry over HTTP for a worker process.

    FastAPI services publish ``/metrics`` through ``create_service_app``. Worker
    processes (parser-worker, normalizer-worker, scheduler-worker) do the actual
    pipeline work and therefore own the domain counters/histograms, but they run
    no HTTP server — so Prometheus had nothing to scrape. This starts a tiny
    standalone exporter so worker-owned metrics become visible.

    The port is read from ``WORKER_METRICS_PORT`` (falling back to ``default_port``).
    Set the env var to ``0`` to disable the exporter entirely.
    """
    raw_port = os.getenv("WORKER_METRICS_PORT")
    try:
        port = int(raw_port) if raw_port is not None else default_port
    except ValueError:
        port = default_port
    if port <= 0:
        LOGGER.info("worker_metrics_exporter_disabled")
        return False
    try:
        start_http_server(port)
    except OSError as exc:  # pragma: no cover - port already bound / unavailable
        LOGGER.warning("worker_metrics_exporter_failed", extra={"port": port, "error": str(exc)})
        return False
    LOGGER.info("worker_metrics_exporter_started", extra={"port": port})
    return True
