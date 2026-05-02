from __future__ import annotations

from libs.contracts.events import CrawlRequestEvent, CrawlRequestPayload, EventHeader
from libs.observability import DomainMetricsCollector, get_domain_metrics

from apps.scheduler.app.runs.models import (
    PipelineRunResponse,
    PipelineRunStatus,
    PipelineRunType,
    PipelineTriggerType,
)
from apps.scheduler.app.runs.repository import PipelineRunRepository
from apps.scheduler.app.runs.service import (
    CrawlRequestPublishError,
    CrawlRequestPublisher,
    parser_queue_for_priority,
)

from .models import (
    ScheduledCrawlJobResult,
    ScheduledCrawlSweepRequest,
    ScheduledCrawlSweepResponse,
    ScheduledEndpointRecord,
)
from .repository import ScheduledCrawlRepository


class ScheduledCrawlService:
    def __init__(
        self,
        *,
        repository: ScheduledCrawlRepository,
        run_repository: PipelineRunRepository,
        publisher: CrawlRequestPublisher,
        metrics_collector: DomainMetricsCollector | None = None,
    ) -> None:
        self._repository = repository
        self._run_repository = run_repository
        self._publisher = publisher
        self._metrics = metrics_collector or get_domain_metrics()

    def run(self, request: ScheduledCrawlSweepRequest) -> ScheduledCrawlSweepResponse:
        endpoints = self._repository.list_scheduled_endpoints(limit=request.limit)
        items: list[ScheduledCrawlJobResult] = []
        due_endpoint_count = 0
        scheduled_count = 0

        for endpoint in endpoints:
            due, reason = self._due_state(endpoint=endpoint, requested_at=request.requested_at)
            if not due:
                items.append(
                    ScheduledCrawlJobResult(
                        source_key=endpoint.source_key,
                        endpoint_id=endpoint.endpoint_id,
                        endpoint_url=endpoint.endpoint_url,
                        scheduled=False,
                        due=False,
                        reason=reason,
                    )
                )
                continue

            due_endpoint_count += 1
            if request.dry_run:
                items.append(
                    ScheduledCrawlJobResult(
                        source_key=endpoint.source_key,
                        endpoint_id=endpoint.endpoint_id,
                        endpoint_url=endpoint.endpoint_url,
                        scheduled=False,
                        due=True,
                        reason="dry_run",
                    )
                )
                continue

            accepted = self._trigger_scheduled_crawl(
                endpoint=endpoint,
                request=request,
            )
            scheduled_count += 1
            items.append(
                ScheduledCrawlJobResult(
                    source_key=endpoint.source_key,
                    endpoint_id=endpoint.endpoint_id,
                    endpoint_url=endpoint.endpoint_url,
                    crawl_run_id=accepted.run_id,
                    queue_name=accepted.metadata.get("published_queue"),
                    scheduled=True,
                    due=True,
                    reason="scheduled",
                )
            )

        return ScheduledCrawlSweepResponse(
            requested_at=request.requested_at,
            scanned_endpoint_count=len(endpoints),
            due_endpoint_count=due_endpoint_count,
            scheduled_count=scheduled_count,
            items=items,
        )

    @staticmethod
    def _due_state(
        *,
        endpoint: ScheduledEndpointRecord,
        requested_at,
    ) -> tuple[bool, str]:
        interval_seconds = endpoint.crawl_policy.interval_seconds
        if interval_seconds is None:
            return False, "missing_interval"

        reference_time = endpoint.last_observed_at or endpoint.last_attempted_at
        if reference_time is None:
            return True, "never_crawled"

        age_seconds = max(0.0, (requested_at - reference_time).total_seconds())
        if age_seconds >= interval_seconds:
            return True, "interval_elapsed"
        remaining_seconds = max(0, interval_seconds - int(age_seconds))
        return False, f"not_due:{remaining_seconds}s_remaining"

    def _trigger_scheduled_crawl(
        self,
        *,
        endpoint: ScheduledEndpointRecord,
        request: ScheduledCrawlSweepRequest,
    ) -> PipelineRunResponse:
        crawl_policy = endpoint.crawl_policy.model_dump(mode="json")
        payload = CrawlRequestPayload(
            source_key=endpoint.source_key,
            endpoint_url=endpoint.endpoint_url,
            priority=request.priority,
            trigger="schedule",
            parser_profile=endpoint.parser_profile,
            requested_at=request.requested_at,
            metadata={
                **request.metadata,
                "endpoint_id": str(endpoint.endpoint_id),
                "crawl_policy": crawl_policy,
                "schedule_enabled": endpoint.crawl_policy.schedule_enabled,
                "interval_seconds": endpoint.crawl_policy.interval_seconds,
            },
        )
        self._run_repository.create(
            run_id=payload.crawl_run_id,
            run_type=PipelineRunType.CRAWL,
            status=PipelineRunStatus.QUEUED,
            trigger_type=PipelineTriggerType.SCHEDULE,
            source_key=payload.source_key,
            metadata={
                "endpoint_id": str(endpoint.endpoint_id),
                "endpoint_url": endpoint.endpoint_url,
                "parser_profile": endpoint.parser_profile,
                "priority": payload.priority,
                "requested_at": payload.requested_at.isoformat(),
                "schedule_enabled": endpoint.crawl_policy.schedule_enabled,
                "interval_seconds": endpoint.crawl_policy.interval_seconds,
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
            self._metrics.record_crawl_job(
                status="failed",
                trigger_type=payload.trigger,
                priority=payload.priority,
                parser_profile=payload.parser_profile,
            )
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
        self._metrics.record_crawl_job(
            status="published",
            trigger_type=payload.trigger,
            priority=payload.priority,
            parser_profile=payload.parser_profile,
        )
        return PipelineRunResponse.model_validate(published_run)
