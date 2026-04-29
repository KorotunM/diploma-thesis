from typing import Annotated, Any

from fastapi import APIRouter, Depends, status

from apps.scheduler.app.dependencies import get_scheduler_session
from libs.storage import RabbitMQPublisher, get_platform_settings, get_rabbitmq_connection

from .emitter import StaleSourceReviewRequiredEmitter
from .models import (
    FreshnessOverviewResponse,
    StaleSourceMonitoringRunRequest,
    StaleSourceMonitoringRunResponse,
)
from .repository import SourceFreshnessRepository
from .service import SourceFreshnessService, StaleSourceMonitoringService

router = APIRouter(prefix="/admin/v1", tags=["scheduler:freshness"])

SchedulerSessionDependency = Annotated[Any, Depends(get_scheduler_session)]


def get_source_freshness_service(
    session: SchedulerSessionDependency,
) -> SourceFreshnessService:
    repository = SourceFreshnessRepository(session)
    return SourceFreshnessService(repository=repository)


def get_stale_source_monitoring_service(
    session: SchedulerSessionDependency,
) -> StaleSourceMonitoringService:
    settings = get_platform_settings(service_name="scheduler")
    connection = get_rabbitmq_connection(service_name="scheduler")
    repository = SourceFreshnessRepository(session)
    return StaleSourceMonitoringService(
        repository=repository,
        freshness_service=SourceFreshnessService(repository=repository),
        emitter=StaleSourceReviewRequiredEmitter(
            publisher=RabbitMQPublisher(connection, settings.rabbitmq),
        ),
    )


SourceFreshnessServiceDependency = Annotated[
    SourceFreshnessService,
    Depends(get_source_freshness_service),
]
StaleSourceMonitoringServiceDependency = Annotated[
    StaleSourceMonitoringService,
    Depends(get_stale_source_monitoring_service),
]


@router.get(
    "/freshness",
    response_model=FreshnessOverviewResponse,
    summary="Compute source freshness from observed crawl activity",
)
def get_source_freshness(
    service: SourceFreshnessServiceDependency,
    include_inactive: bool = True,
) -> FreshnessOverviewResponse:
    return service.build_overview(include_inactive=include_inactive)


@router.post(
    "/freshness/monitor-jobs",
    response_model=StaleSourceMonitoringRunResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Run stale-source monitoring and emit review.required for stale scheduled sources",
)
def run_stale_source_monitoring(
    request: StaleSourceMonitoringRunRequest,
    service: StaleSourceMonitoringServiceDependency,
) -> StaleSourceMonitoringRunResponse:
    return service.run(request)
