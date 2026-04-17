from fastapi import APIRouter, FastAPI
from prometheus_client import make_asgi_app

from libs.contracts.dto import HealthResponse
from libs.storage.settings import PlatformSettings


def create_service_app(service_name: str, description: str) -> FastAPI:
    settings = PlatformSettings()
    app = FastAPI(title=service_name, description=description, version=settings.app_version)
    router = APIRouter(tags=["platform"])

    dependencies = {
        "postgres": "configured" if settings.postgres_dsn else "missing",
        "rabbitmq": "configured" if settings.rabbitmq_url else "missing",
        "minio": "configured" if settings.minio_endpoint else "missing",
    }

    @router.get("/healthz", response_model=HealthResponse, summary="Liveness probe")
    def healthz() -> HealthResponse:
        return HealthResponse(
            service=service_name,
            environment=settings.app_env,
            version=settings.app_version,
            dependencies=dependencies,
        )

    @router.get("/readyz", response_model=HealthResponse, summary="Readiness probe")
    def readyz() -> HealthResponse:
        return HealthResponse(
            service=service_name,
            environment=settings.app_env,
            version=settings.app_version,
            dependencies=dependencies,
        )

    app.include_router(router)
    app.mount("/metrics", make_asgi_app())
    return app
