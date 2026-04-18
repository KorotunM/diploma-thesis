from fastapi import APIRouter, FastAPI
from prometheus_client import make_asgi_app

from libs.contracts.dto import HealthResponse
from libs.storage import get_platform_settings


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
