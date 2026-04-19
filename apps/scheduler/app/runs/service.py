from __future__ import annotations

from apps.scheduler.app.sources.endpoint_repository import SourceEndpointRepository
from libs.contracts.events import CrawlRequestEvent, CrawlRequestPayload, EventHeader

from .models import (
    CrawlJobAcceptedResponse,
    ManualCrawlTriggerRequest,
    PipelineRunResponse,
    PipelineRunStatus,
    PipelineRunType,
    PipelineTriggerType,
)
from .repository import PipelineRunRepository


class ManualCrawlEndpointNotFoundError(ValueError):
    def __init__(self, source_key: str, endpoint_id: object) -> None:
        super().__init__(f"Endpoint {endpoint_id} was not found for source {source_key}")
        self.source_key = source_key
        self.endpoint_id = endpoint_id


class ManualCrawlTriggerService:
    def __init__(
        self,
        *,
        endpoint_repository: SourceEndpointRepository,
        run_repository: PipelineRunRepository,
    ) -> None:
        self._endpoint_repository = endpoint_repository
        self._run_repository = run_repository

    def trigger_manual_crawl(
        self,
        request: ManualCrawlTriggerRequest,
    ) -> CrawlJobAcceptedResponse:
        endpoint = self._endpoint_repository.get(request.source_key, request.endpoint_id)
        if endpoint is None:
            raise ManualCrawlEndpointNotFoundError(request.source_key, request.endpoint_id)

        crawl_policy = endpoint.crawl_policy.model_dump(mode="json")
        payload = CrawlRequestPayload(
            crawl_run_id=request.crawl_run_id,
            source_key=endpoint.source_key,
            endpoint_url=endpoint.endpoint_url,
            priority=request.priority,
            trigger="manual",
            parser_profile=endpoint.parser_profile,
            requested_at=request.requested_at,
            metadata={
                **request.metadata,
                "endpoint_id": str(endpoint.endpoint_id),
                "crawl_policy": crawl_policy,
            },
        )
        pipeline_run = self._run_repository.create(
            run_id=payload.crawl_run_id,
            run_type=PipelineRunType.CRAWL,
            status=PipelineRunStatus.QUEUED,
            trigger_type=PipelineTriggerType.MANUAL,
            source_key=payload.source_key,
            metadata={
                "endpoint_id": str(endpoint.endpoint_id),
                "endpoint_url": endpoint.endpoint_url,
                "parser_profile": endpoint.parser_profile,
                "priority": payload.priority,
                "requested_at": payload.requested_at.isoformat(),
                "request_metadata": request.metadata,
            },
        )

        return CrawlJobAcceptedResponse(
            pipeline_run=PipelineRunResponse.model_validate(pipeline_run),
            event=CrawlRequestEvent(
                header=EventHeader(producer="scheduler"),
                payload=payload,
            ),
        )
