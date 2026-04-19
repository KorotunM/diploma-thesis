from __future__ import annotations

from typing import Any, Protocol

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

PARSER_HIGH_QUEUE = "parser.high"
PARSER_BULK_QUEUE = "parser.bulk"


class CrawlRequestPublisher(Protocol):
    def publish(
        self,
        payload: Any,
        *,
        queue_name: str,
        headers: dict[str, Any] | None = None,
    ) -> Any:
        """Publish a crawl request event to the parser topology."""


class ManualCrawlEndpointNotFoundError(ValueError):
    def __init__(self, source_key: str, endpoint_id: object) -> None:
        super().__init__(f"Endpoint {endpoint_id} was not found for source {source_key}")
        self.source_key = source_key
        self.endpoint_id = endpoint_id


class CrawlRequestPublishError(RuntimeError):
    def __init__(self, crawl_run_id: object, queue_name: str, reason: str) -> None:
        super().__init__(
            f"Failed to publish crawl request for run {crawl_run_id} to {queue_name}: {reason}"
        )
        self.crawl_run_id = crawl_run_id
        self.queue_name = queue_name
        self.reason = reason


def parser_queue_for_priority(priority: str) -> str:
    if priority == "high":
        return PARSER_HIGH_QUEUE
    return PARSER_BULK_QUEUE


class ManualCrawlTriggerService:
    def __init__(
        self,
        *,
        endpoint_repository: SourceEndpointRepository,
        run_repository: PipelineRunRepository,
        publisher: CrawlRequestPublisher,
    ) -> None:
        self._endpoint_repository = endpoint_repository
        self._run_repository = run_repository
        self._publisher = publisher

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
        self._run_repository.create(
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
        self._run_repository.commit()
        event = CrawlRequestEvent(
            header=EventHeader(producer="scheduler"),
            payload=payload,
        )
        queue_name = parser_queue_for_priority(payload.priority)
        try:
            publish_result = self._publisher.publish(
                event.model_dump(mode="json"),
                queue_name=queue_name,
                headers={
                    "event_name": event.event_name,
                    "event_id": str(event.header.event_id),
                    "schema_version": str(event.header.schema_version),
                    "crawl_run_id": str(payload.crawl_run_id),
                    "source_key": payload.source_key,
                    "priority": payload.priority,
                },
            )
        except Exception as exc:
            self._run_repository.transition(
                run_id=payload.crawl_run_id,
                status=PipelineRunStatus.FAILED,
                metadata_patch={
                    "publish_stage": "rabbitmq",
                    "publish_queue": queue_name,
                    "publish_error": str(exc),
                },
                finish=True,
            )
            self._run_repository.commit()
            raise CrawlRequestPublishError(payload.crawl_run_id, queue_name, str(exc)) from exc

        published_run = self._run_repository.transition(
            run_id=payload.crawl_run_id,
            status=PipelineRunStatus.PUBLISHED,
            metadata_patch={
                "published_event_id": str(event.header.event_id),
                "published_event_name": event.event_name,
                "published_queue": publish_result.queue_name,
                "published_exchange": publish_result.exchange_name,
                "published_routing_key": publish_result.routing_key,
            },
        )
        if published_run is None:
            raise RuntimeError(
                f"Pipeline run {payload.crawl_run_id} was not found after crawl request publish"
            )
        self._run_repository.commit()

        return CrawlJobAcceptedResponse(
            pipeline_run=PipelineRunResponse.model_validate(published_run),
            event=event,
        )
