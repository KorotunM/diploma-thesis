from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status

from libs.storage import get_postgres_session_factory

from .endpoint_repository import (
    SourceEndpointAlreadyExistsError,
    SourceEndpointRepository,
    SourceNotFoundError,
)
from .models import (
    CreateSourceEndpointRequest,
    CreateSourceRequest,
    SourceEndpointListResponse,
    SourceEndpointResponse,
    SourceListResponse,
    SourceResponse,
    UpdateSourceEndpointRequest,
    UpdateSourceRequest,
)
from .repository import SourceAlreadyExistsError, SourceRepository

router = APIRouter(prefix="/admin/v1/sources", tags=["scheduler:sources"])


def get_scheduler_session():
    session_factory = get_postgres_session_factory(service_name="scheduler")
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


SchedulerSessionDependency = Annotated[Any, Depends(get_scheduler_session)]


def get_source_repository(session: SchedulerSessionDependency) -> SourceRepository:
    return SourceRepository(session)


def get_source_endpoint_repository(
    session: SchedulerSessionDependency,
) -> SourceEndpointRepository:
    return SourceEndpointRepository(session)


SourceRepositoryDependency = Annotated[SourceRepository, Depends(get_source_repository)]
SourceEndpointRepositoryDependency = Annotated[
    SourceEndpointRepository,
    Depends(get_source_endpoint_repository),
]


@router.post(
    "",
    response_model=SourceResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new data source",
)
def create_source(
    request: CreateSourceRequest,
    repository: SourceRepositoryDependency,
) -> SourceResponse:
    try:
        return SourceResponse.model_validate(repository.create(request))
    except SourceAlreadyExistsError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Source with key '{exc.source_key}' already exists.",
        ) from exc


@router.get(
    "",
    response_model=SourceListResponse,
    summary="List registered data sources",
)
def list_sources(
    repository: SourceRepositoryDependency,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    include_inactive: bool = False,
) -> SourceListResponse:
    items, total = repository.list(
        limit=limit,
        offset=offset,
        include_inactive=include_inactive,
    )
    return SourceListResponse(
        total=total,
        limit=limit,
        offset=offset,
        items=[SourceResponse.model_validate(item) for item in items],
    )


@router.get(
    "/{source_key}",
    response_model=SourceResponse,
    summary="Read a registered data source",
)
def get_source(
    source_key: str,
    repository: SourceRepositoryDependency,
) -> SourceResponse:
    source = repository.get_by_key(source_key)
    if source is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source with key '{source_key}' was not found.",
        )
    return SourceResponse.model_validate(source)


@router.patch(
    "/{source_key}",
    response_model=SourceResponse,
    summary="Update a registered data source",
)
def update_source(
    source_key: str,
    request: UpdateSourceRequest,
    repository: SourceRepositoryDependency,
) -> SourceResponse:
    source = repository.update(source_key, request)
    if source is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source with key '{source_key}' was not found.",
        )
    return SourceResponse.model_validate(source)


@router.post(
    "/{source_key}/endpoints",
    response_model=SourceEndpointResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a crawl endpoint for a data source",
)
def create_source_endpoint(
    source_key: str,
    request: CreateSourceEndpointRequest,
    repository: SourceEndpointRepositoryDependency,
) -> SourceEndpointResponse:
    try:
        endpoint = repository.create(source_key, request)
    except SourceNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source with key '{exc.source_key}' was not found.",
        ) from exc
    except SourceEndpointAlreadyExistsError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Endpoint '{exc.endpoint_url}' already exists for source '{exc.source_key}'.",
        ) from exc
    return SourceEndpointResponse.model_validate(endpoint)


@router.get(
    "/{source_key}/endpoints",
    response_model=SourceEndpointListResponse,
    summary="List crawl endpoints for a data source",
)
def list_source_endpoints(
    source_key: str,
    repository: SourceEndpointRepositoryDependency,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> SourceEndpointListResponse:
    try:
        items, total = repository.list(source_key, limit=limit, offset=offset)
    except SourceNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source with key '{exc.source_key}' was not found.",
        ) from exc
    return SourceEndpointListResponse(
        total=total,
        limit=limit,
        offset=offset,
        items=[SourceEndpointResponse.model_validate(item) for item in items],
    )


@router.get(
    "/{source_key}/endpoints/{endpoint_id}",
    response_model=SourceEndpointResponse,
    summary="Read a crawl endpoint for a data source",
)
def get_source_endpoint(
    source_key: str,
    endpoint_id: UUID,
    repository: SourceEndpointRepositoryDependency,
) -> SourceEndpointResponse:
    endpoint = repository.get(source_key, endpoint_id)
    if endpoint is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Endpoint '{endpoint_id}' was not found for source '{source_key}'.",
        )
    return SourceEndpointResponse.model_validate(endpoint)


@router.patch(
    "/{source_key}/endpoints/{endpoint_id}",
    response_model=SourceEndpointResponse,
    summary="Update a crawl endpoint for a data source",
)
def update_source_endpoint(
    source_key: str,
    endpoint_id: UUID,
    request: UpdateSourceEndpointRequest,
    repository: SourceEndpointRepositoryDependency,
) -> SourceEndpointResponse:
    try:
        endpoint = repository.update(source_key, endpoint_id, request)
    except SourceEndpointAlreadyExistsError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Endpoint '{exc.endpoint_url}' already exists for source '{exc.source_key}'.",
        ) from exc
    if endpoint is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Endpoint '{endpoint_id}' was not found for source '{source_key}'.",
        )
    return SourceEndpointResponse.model_validate(endpoint)
