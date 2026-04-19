import json
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from apps.scheduler.app.runs.models import (
    ManualCrawlTriggerRequest,
    PipelineRunStatus,
    PipelineRunType,
    PipelineTriggerType,
)
from apps.scheduler.app.runs.repository import PipelineRunRepository
from apps.scheduler.app.runs.service import (
    CrawlRequestPublishError,
    ManualCrawlEndpointNotFoundError,
    ManualCrawlTriggerService,
)
from apps.scheduler.app.sources.models import CrawlPolicy, SourceEndpointRecord


class FakeMappingResult:
    def __init__(self, *, row=None) -> None:
        self._row = row

    def mappings(self) -> "FakeMappingResult":
        return self

    def one(self):
        return self._row

    def one_or_none(self):
        return self._row


class FakePipelineRunSession:
    def __init__(self) -> None:
        self.rows: dict[object, dict] = {}
        self.executed: list[tuple[str, dict]] = []
        self.commit_count = 0
        self.now = datetime(2026, 4, 19, 10, 0, tzinfo=UTC)
        self.finished_at = datetime(2026, 4, 19, 10, 5, tzinfo=UTC)

    def commit(self) -> None:
        self.commit_count += 1

    def execute(self, statement: str, params: dict):
        normalized_statement = " ".join(statement.split()).lower()
        self.executed.append((normalized_statement, params))

        if normalized_statement.startswith("insert"):
            row = {
                "run_id": params["run_id"],
                "run_type": params["run_type"],
                "status": params["status"],
                "trigger_type": params["trigger_type"],
                "source_key": params["source_key"],
                "started_at": self.now,
                "finished_at": None,
                "metadata": params["metadata"],
            }
            self.rows[params["run_id"]] = row
            return FakeMappingResult(row=row)

        if normalized_statement.startswith("select"):
            return FakeMappingResult(row=self.rows.get(params["run_id"]))

        if normalized_statement.startswith("update"):
            row = self.rows.get(params["run_id"])
            if row is None:
                return FakeMappingResult(row=None)
            row["status"] = params["status"]
            if params["finish"]:
                row["finished_at"] = self.finished_at
            row_metadata = json.loads(row["metadata"])
            row_metadata.update(json.loads(params["metadata_patch"]))
            row["metadata"] = json.dumps(row_metadata)
            return FakeMappingResult(row=row)

        raise AssertionError(f"Unexpected statement: {statement}")


class FakeEndpointRepository:
    def __init__(self, endpoint: SourceEndpointRecord | None) -> None:
        self.endpoint = endpoint
        self.calls: list[tuple[str, object]] = []

    def get(self, source_key: str, endpoint_id):
        self.calls.append((source_key, endpoint_id))
        return self.endpoint


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
            "started_at": datetime(2026, 4, 19, 10, 0, tzinfo=UTC),
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
        if kwargs.get("finish"):
            self.record["finished_at"] = datetime(2026, 4, 19, 10, 5, tzinfo=UTC)
        return self.record

    def commit(self) -> None:
        self.commit_count += 1


class FakeCrawlRequestPublisher:
    def __init__(self, *, fail_with: Exception | None = None) -> None:
        self.fail_with = fail_with
        self.calls: list[dict] = []

    def publish(self, payload, *, queue_name: str, headers: dict | None = None):
        self.calls.append(
            {
                "payload": payload,
                "queue_name": queue_name,
                "headers": headers or {},
            }
        )
        if self.fail_with is not None:
            raise self.fail_with
        routing_key = "high" if queue_name == "parser.high" else "bulk"
        return SimpleNamespace(
            queue_name=queue_name,
            exchange_name="parser.jobs",
            routing_key=routing_key,
        )


def build_endpoint(endpoint_id=None) -> SourceEndpointRecord:
    return SourceEndpointRecord(
        endpoint_id=endpoint_id or uuid4(),
        source_id=uuid4(),
        source_key="msu-official",
        endpoint_url="https://example.edu",
        parser_profile="official_site.default",
        crawl_policy=CrawlPolicy(timeout_seconds=45, max_retries=2),
    )


def test_pipeline_run_repository_creates_queued_manual_crawl_run() -> None:
    session = FakePipelineRunSession()
    repository = PipelineRunRepository(session, sql_text=lambda statement: statement)
    run_id = uuid4()

    created = repository.create(
        run_id=run_id,
        run_type=PipelineRunType.CRAWL,
        status=PipelineRunStatus.QUEUED,
        trigger_type=PipelineTriggerType.MANUAL,
        source_key="msu-official",
        metadata={"endpoint_url": "https://example.edu"},
    )

    assert created.run_id == run_id
    assert created.run_type == PipelineRunType.CRAWL
    assert created.status == PipelineRunStatus.QUEUED
    assert created.trigger_type == PipelineTriggerType.MANUAL
    assert created.source_key == "msu-official"
    assert created.metadata == {"endpoint_url": "https://example.edu"}


def test_pipeline_run_repository_transitions_status_and_merges_metadata() -> None:
    session = FakePipelineRunSession()
    repository = PipelineRunRepository(session, sql_text=lambda statement: statement)
    run_id = uuid4()
    repository.create(
        run_id=run_id,
        run_type=PipelineRunType.CRAWL,
        status=PipelineRunStatus.QUEUED,
        trigger_type=PipelineTriggerType.MANUAL,
        source_key="msu-official",
        metadata={"attempt": 1},
    )

    transitioned = repository.transition(
        run_id=run_id,
        status=PipelineRunStatus.PUBLISHED,
        metadata_patch={"published_queue": "parser.high"},
    )

    assert transitioned is not None
    assert transitioned.status == PipelineRunStatus.PUBLISHED
    assert transitioned.finished_at is None
    assert transitioned.metadata == {
        "attempt": 1,
        "published_queue": "parser.high",
    }


def test_manual_crawl_trigger_service_persists_run_and_builds_event() -> None:
    endpoint_id = uuid4()
    endpoint = build_endpoint(endpoint_id)
    endpoint_repository = FakeEndpointRepository(endpoint)
    run_repository = FakeRunRepository()
    publisher = FakeCrawlRequestPublisher()
    service = ManualCrawlTriggerService(
        endpoint_repository=endpoint_repository,
        run_repository=run_repository,
        publisher=publisher,
    )
    crawl_run_id = uuid4()
    requested_at = datetime(2026, 4, 19, 12, 0, tzinfo=UTC)

    response = service.trigger_manual_crawl(
        ManualCrawlTriggerRequest(
            crawl_run_id=crawl_run_id,
            source_key="msu-official",
            endpoint_id=endpoint_id,
            priority="high",
            requested_at=requested_at,
            metadata={"requested_by": "operator"},
        )
    )

    assert response.pipeline_run.run_id == crawl_run_id
    assert response.pipeline_run.status == PipelineRunStatus.PUBLISHED
    assert response.event.event_name == "crawl.request.v1"
    assert response.event.payload.crawl_run_id == crawl_run_id
    assert response.event.payload.trigger == "manual"
    assert response.event.payload.source_key == "msu-official"
    assert response.event.payload.endpoint_url == "https://example.edu"
    assert response.event.payload.priority == "high"
    assert response.event.payload.parser_profile == "official_site.default"
    assert response.event.payload.metadata["endpoint_id"] == str(endpoint_id)
    assert response.event.payload.metadata["requested_by"] == "operator"
    assert response.event.payload.metadata["crawl_policy"]["timeout_seconds"] == 45
    assert run_repository.created[0]["metadata"]["priority"] == "high"
    assert run_repository.transitions[0]["status"] == PipelineRunStatus.PUBLISHED
    assert run_repository.transitions[0]["metadata_patch"]["published_queue"] == "parser.high"
    assert run_repository.transitions[0]["metadata_patch"]["published_exchange"] == "parser.jobs"
    assert run_repository.transitions[0]["metadata_patch"]["published_routing_key"] == "high"
    assert run_repository.commit_count == 2
    assert publisher.calls == [
        {
            "payload": response.event.model_dump(mode="json"),
            "queue_name": "parser.high",
            "headers": {
                "event_name": "crawl.request.v1",
                "event_id": str(response.event.header.event_id),
                "schema_version": "1",
                "crawl_run_id": str(crawl_run_id),
                "source_key": "msu-official",
                "priority": "high",
            },
        }
    ]


def test_manual_crawl_trigger_service_routes_bulk_priority_to_bulk_queue() -> None:
    endpoint_id = uuid4()
    run_repository = FakeRunRepository()
    publisher = FakeCrawlRequestPublisher()
    service = ManualCrawlTriggerService(
        endpoint_repository=FakeEndpointRepository(build_endpoint(endpoint_id)),
        run_repository=run_repository,
        publisher=publisher,
    )

    response = service.trigger_manual_crawl(
        ManualCrawlTriggerRequest(
            source_key="msu-official",
            endpoint_id=endpoint_id,
            priority="bulk",
        )
    )

    assert response.pipeline_run.status == PipelineRunStatus.PUBLISHED
    assert publisher.calls[0]["queue_name"] == "parser.bulk"
    assert run_repository.transitions[0]["metadata_patch"]["published_routing_key"] == "bulk"
    assert run_repository.commit_count == 2


def test_manual_crawl_trigger_service_marks_run_failed_when_publish_fails() -> None:
    endpoint_id = uuid4()
    run_repository = FakeRunRepository()
    service = ManualCrawlTriggerService(
        endpoint_repository=FakeEndpointRepository(build_endpoint(endpoint_id)),
        run_repository=run_repository,
        publisher=FakeCrawlRequestPublisher(fail_with=RuntimeError("broker unavailable")),
    )

    with pytest.raises(CrawlRequestPublishError) as exc_info:
        service.trigger_manual_crawl(
            ManualCrawlTriggerRequest(
                source_key="msu-official",
                endpoint_id=endpoint_id,
                priority="high",
            )
        )

    assert "broker unavailable" in str(exc_info.value)
    assert run_repository.transitions[0]["status"] == PipelineRunStatus.FAILED
    assert run_repository.transitions[0]["finish"] is True
    assert run_repository.transitions[0]["metadata_patch"] == {
        "publish_stage": "rabbitmq",
        "publish_queue": "parser.high",
        "publish_error": "broker unavailable",
    }
    assert run_repository.commit_count == 2


def test_manual_crawl_trigger_service_rejects_unknown_endpoint() -> None:
    service = ManualCrawlTriggerService(
        endpoint_repository=FakeEndpointRepository(None),
        run_repository=FakeRunRepository(),
        publisher=FakeCrawlRequestPublisher(),
    )

    with pytest.raises(ManualCrawlEndpointNotFoundError):
        service.trigger_manual_crawl(
            ManualCrawlTriggerRequest(
                source_key="msu-official",
                endpoint_id=uuid4(),
            )
        )
