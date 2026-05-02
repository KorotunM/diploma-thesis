from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

from apps.scheduler.app.runs.models import PipelineRunStatus, PipelineTriggerType
from apps.scheduler.app.scheduled import (
    ScheduledCrawlJobResult,
    ScheduledCrawlService,
    ScheduledCrawlSweepRequest,
    ScheduledEndpointRecord,
)
from apps.scheduler.app.sources.models import CrawlPolicy


class FakeScheduledRepository:
    def __init__(self, endpoints: list[ScheduledEndpointRecord]) -> None:
        self.endpoints = endpoints
        self.calls: list[int] = []

    def list_scheduled_endpoints(self, *, limit: int) -> list[ScheduledEndpointRecord]:
        self.calls.append(limit)
        return self.endpoints[:limit]


class FakeRunRepository:
    def __init__(self) -> None:
        self.created: list[dict] = []
        self.transitions: list[dict] = []
        self.commit_count = 0
        self.record = None

    def create(self, **kwargs):
        self.created.append(kwargs)
        self.record = {
            "run_id": kwargs["run_id"],
            "run_type": kwargs["run_type"],
            "status": kwargs["status"],
            "trigger_type": kwargs["trigger_type"],
            "source_key": kwargs["source_key"],
            "started_at": datetime(2026, 5, 2, 10, 0, tzinfo=UTC),
            "finished_at": None,
            "metadata": kwargs["metadata"],
        }
        return self.record

    def transition(self, **kwargs):
        self.transitions.append(kwargs)
        if self.record is None:
            return None
        self.record = {
            **self.record,
            "status": kwargs["status"],
            "metadata": {
                **self.record["metadata"],
                **(kwargs.get("metadata_patch") or {}),
            },
        }
        return self.record

    def commit(self) -> None:
        self.commit_count += 1


class FakePublisher:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def publish(self, payload, *, queue_name: str, headers: dict | None = None):
        self.calls.append(
            {
                "payload": payload,
                "queue_name": queue_name,
                "headers": headers or {},
            }
        )
        return SimpleNamespace(
            queue_name=queue_name,
            exchange_name="parser.jobs",
            routing_key="high" if queue_name == "parser.high" else "bulk",
        )


def scheduled_endpoint(
    *,
    last_observed_at: datetime | None,
    last_attempted_at: datetime | None = None,
    interval_seconds: int = 3600,
) -> ScheduledEndpointRecord:
    return ScheduledEndpointRecord(
        endpoint_id=uuid4(),
        source_id=uuid4(),
        source_key="kubsu-official",
        endpoint_url="https://example.edu/programs",
        parser_profile="official_site.kubsu.programs_html",
        crawl_policy=CrawlPolicy(
            schedule_enabled=True,
            interval_seconds=interval_seconds,
        ),
        last_observed_at=last_observed_at,
        last_attempted_at=last_attempted_at,
    )


def build_service(endpoints: list[ScheduledEndpointRecord]):
    return (
        ScheduledCrawlService(
            repository=FakeScheduledRepository(endpoints),
            run_repository=FakeRunRepository(),
            publisher=FakePublisher(),
        ),
    )


def test_scheduled_crawl_service_publishes_due_endpoint_with_schedule_trigger() -> None:
    endpoint = scheduled_endpoint(
        last_observed_at=datetime(2026, 5, 2, 7, 0, tzinfo=UTC),
    )
    repository = FakeScheduledRepository([endpoint])
    run_repository = FakeRunRepository()
    publisher = FakePublisher()
    service = ScheduledCrawlService(
        repository=repository,
        run_repository=run_repository,
        publisher=publisher,
    )

    response = service.run(
        ScheduledCrawlSweepRequest(
            requested_at=datetime(2026, 5, 2, 10, 0, tzinfo=UTC),
            priority="bulk",
            metadata={"worker": "scheduler-loop"},
        )
    )

    assert response.scanned_endpoint_count == 1
    assert response.due_endpoint_count == 1
    assert response.scheduled_count == 1
    assert isinstance(response.items[0], ScheduledCrawlJobResult)
    assert response.items[0].scheduled is True
    assert response.items[0].reason == "scheduled"
    assert run_repository.created[0]["trigger_type"] == PipelineTriggerType.SCHEDULE
    assert run_repository.transitions[0]["status"] == PipelineRunStatus.PUBLISHED
    assert run_repository.commit_count == 2
    assert publisher.calls[0]["queue_name"] == "parser.bulk"
    assert publisher.calls[0]["payload"]["payload"]["trigger"] == "schedule"
    assert publisher.calls[0]["payload"]["payload"]["metadata"]["worker"] == "scheduler-loop"


def test_scheduled_crawl_service_skips_recent_endpoint_and_supports_dry_run() -> None:
    requested_at = datetime(2026, 5, 2, 10, 0, tzinfo=UTC)
    fresh_endpoint = scheduled_endpoint(
        last_observed_at=requested_at - timedelta(minutes=10),
        interval_seconds=3600,
    )
    never_crawled_endpoint = scheduled_endpoint(
        last_observed_at=None,
        last_attempted_at=None,
        interval_seconds=7200,
    ).model_copy(
        update={"endpoint_url": "https://example.edu/places.pdf"}
    )
    repository = FakeScheduledRepository([fresh_endpoint, never_crawled_endpoint])
    run_repository = FakeRunRepository()
    publisher = FakePublisher()
    service = ScheduledCrawlService(
        repository=repository,
        run_repository=run_repository,
        publisher=publisher,
    )

    response = service.run(
        ScheduledCrawlSweepRequest(
            requested_at=requested_at,
            dry_run=True,
            limit=10,
        )
    )

    assert response.scanned_endpoint_count == 2
    assert response.due_endpoint_count == 1
    assert response.scheduled_count == 0
    assert response.items[0].scheduled is False
    assert response.items[0].due is False
    assert response.items[0].reason.startswith("not_due:")
    assert response.items[1].scheduled is False
    assert response.items[1].due is True
    assert response.items[1].reason == "dry_run"
    assert run_repository.created == []
    assert publisher.calls == []
