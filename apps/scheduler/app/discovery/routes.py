from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, status

from apps.scheduler.app.dependencies import get_scheduler_session
from apps.scheduler.app.sources.endpoint_repository import (
    SourceEndpointRepository,
    SourceNotFoundError,
)
from apps.scheduler.app.sources.repository import SourceRepository

from .models import DiscoveryMaterializationRequest, DiscoveryMaterializationResponse
from .service import (
    HttpDiscoveryFetcher,
    SourceEndpointDiscoveryService,
    SourceEndpointDiscoveryWorkflowError,
)

router = APIRouter(prefix="/admin/v1/discovery", tags=["scheduler:discovery"])

SchedulerSessionDependency = Annotated[Any, Depends(get_scheduler_session)]


def get_source_endpoint_discovery_service(
    session: SchedulerSessionDependency,
) -> SourceEndpointDiscoveryService:
    return SourceEndpointDiscoveryService(
        source_repository=SourceRepository(session),
        endpoint_repository=SourceEndpointRepository(session),
        fetcher=HttpDiscoveryFetcher(),
    )


SourceEndpointDiscoveryServiceDependency = Annotated[
    SourceEndpointDiscoveryService,
    Depends(get_source_endpoint_discovery_service),
]


@router.post(
    "/materialize-jobs",
    response_model=DiscoveryMaterializationResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Fetch discovery sources and materialize child crawl endpoints",
)
def materialize_source_endpoints(
    request: DiscoveryMaterializationRequest,
    service: SourceEndpointDiscoveryServiceDependency,
) -> DiscoveryMaterializationResponse:
    try:
        return service.materialize_discovered_endpoints(request)
    except SourceNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source with key '{exc.source_key}' was not found.",
        ) from exc
    except SourceEndpointDiscoveryWorkflowError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
