from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, status

from apps.scheduler.app.dependencies import get_scheduler_session
from apps.scheduler.app.sources.endpoint_repository import SourceEndpointRepository
from apps.scheduler.app.sources.repository import SourceRepository
from libs.storage import RabbitMQPublisher, get_platform_settings, get_rabbitmq_connection

from .models import (
    CrawlJobAcceptedResponse,
    ManualCrawlTriggerRequest,
    PipelineRerunRequest,
    PipelineRerunResponse,
    PipelineRunListResponse,
    PipelineRunResponse,
)
from .repository import PipelineRunRepository
from .service import (
    CrawlRequestPublishError,
    ManualCrawlEndpointNotFoundError,
    ManualCrawlTriggerService,
    PipelineRerunService,
)

router = APIRouter(prefix="/admin/v1", tags=["scheduler:runs"])

SchedulerSessionDependency = Annotated[Any, Depends(get_scheduler_session)]


def get_manual_crawl_trigger_service(
    session: SchedulerSessionDependency,
) -> ManualCrawlTriggerService:
    settings = get_platform_settings(service_name="scheduler")
    connection = get_rabbitmq_connection(service_name="scheduler")
    return ManualCrawlTriggerService(
        endpoint_repository=SourceEndpointRepository(session),
        run_repository=PipelineRunRepository(session),
        publisher=RabbitMQPublisher(connection, settings.rabbitmq),
    )


ManualCrawlTriggerServiceDependency = Annotated[
    ManualCrawlTriggerService,
    Depends(get_manual_crawl_trigger_service),
]


def get_pipeline_rerun_service(
    session: SchedulerSessionDependency,
    trigger_service: ManualCrawlTriggerServiceDependency,
) -> PipelineRerunService:
    return PipelineRerunService(
        source_repository=SourceRepository(session),
        endpoint_repository=SourceEndpointRepository(session),
        trigger_service=trigger_service,
    )


PipelineRerunServiceDependency = Annotated[
    PipelineRerunService,
    Depends(get_pipeline_rerun_service),
]


def get_pipeline_run_repository(
    session: SchedulerSessionDependency,
) -> PipelineRunRepository:
    return PipelineRunRepository(session)


PipelineRunRepositoryDependency = Annotated[
    PipelineRunRepository,
    Depends(get_pipeline_run_repository),
]


@router.post(
    "/crawl-jobs",
    response_model=CrawlJobAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create a manual crawl run",
)
def create_crawl_job(
    request: ManualCrawlTriggerRequest,
    service: ManualCrawlTriggerServiceDependency,
) -> CrawlJobAcceptedResponse:
    try:
        return service.trigger_manual_crawl(request)
    except ManualCrawlEndpointNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Endpoint '{exc.endpoint_id}' was not found for source '{exc.source_key}'.",
        ) from exc
    except CrawlRequestPublishError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(exc),
        ) from exc


@router.post(
    "/pipeline/rerun",
    response_model=PipelineRerunResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Re-run the whole pipeline or a single source",
)
def rerun_pipeline(
    request: PipelineRerunRequest,
    service: PipelineRerunServiceDependency,
) -> PipelineRerunResponse:
    return service.rerun(request)


@router.get(
    "/pipeline/runs",
    response_model=PipelineRunListResponse,
    summary="List recent pipeline runs",
)
def list_pipeline_runs(
    repository: PipelineRunRepositoryDependency,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> PipelineRunListResponse:
    runs = repository.list_recent(limit=limit, offset=offset)
    return PipelineRunListResponse(
        total=len(runs),
        items=[PipelineRunResponse.model_validate(run.model_dump(mode="python")) for run in runs],
    )
