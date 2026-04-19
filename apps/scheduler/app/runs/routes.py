from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, status

from apps.scheduler.app.dependencies import get_scheduler_session
from apps.scheduler.app.sources.endpoint_repository import SourceEndpointRepository
from libs.storage import RabbitMQPublisher, get_platform_settings, get_rabbitmq_connection

from .models import CrawlJobAcceptedResponse, ManualCrawlTriggerRequest
from .repository import PipelineRunRepository
from .service import (
    CrawlRequestPublishError,
    ManualCrawlEndpointNotFoundError,
    ManualCrawlTriggerService,
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
