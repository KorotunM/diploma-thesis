from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, status

from libs.storage import get_postgres_session_factory

from .models import CreateSourceRequest, SourceListResponse, SourceResponse, UpdateSourceRequest
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


SourceRepositoryDependency = Annotated[SourceRepository, Depends(get_source_repository)]


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
