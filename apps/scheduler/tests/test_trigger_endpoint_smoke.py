from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
pytest.importorskip("prometheus_client")

from fastapi.testclient import TestClient

from apps.scheduler.app.main import app
from apps.scheduler.app.runs.models import (
    PipelineRunStatus,
    PipelineRunType,
    PipelineTriggerType,
)
from apps.scheduler.app.runs.routes import get_manual_crawl_trigger_service
from apps.scheduler.app.runs.service import ManualCrawlTriggerService
from apps.scheduler.app.sources.models import CrawlPolicy, SourceEndpointRecord
from libs.contracts.events import CrawlRequestEvent


class FakeEndpointRepository:
    def __init__(self, endpoint: SourceEndpointRecord) -> None:
        self.endpoint = endpoint

    def get(self, source_key: str, endpoint_id: UUID) -> SourceEndpointRecord | None:
        if source_key != self.endpoint.source_key:
            return None
        if endpoint_id != self.endpoint.endpoint_id:
            return None
        return self.endpoint


class FakeRunRepository:
    def __init__(self) -> None:
        self.record: dict | None = None
        self.commit_count = 0

    def create(self, **kwargs):
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


def test_trigger_endpoint_publishes_crawl_request_event_contract() -> None:
    endpoint_id = uuid4()
    crawl_run_id = uuid4()
    publisher = FakePublisher()
    run_repository = FakeRunRepository()
    endpoint = SourceEndpointRecord(
        endpoint_id=endpoint_id,
        source_id=uuid4(),
        source_key="msu-official",
        endpoint_url="https://example.edu",
        parser_profile="official_site.default",
        crawl_policy=CrawlPolicy(
            timeout_seconds=45,
            max_retries=2,
            render_mode="http",
        ),
    )

    def override_service() -> ManualCrawlTriggerService:
        return ManualCrawlTriggerService(
            endpoint_repository=FakeEndpointRepository(endpoint),
            run_repository=run_repository,
            publisher=publisher,
        )

    app.dependency_overrides[get_manual_crawl_trigger_service] = override_service
    try:
        response = TestClient(app).post(
            "/admin/v1/crawl-jobs",
            json={
                "crawl_run_id": str(crawl_run_id),
                "source_key": "msu-official",
                "endpoint_id": str(endpoint_id),
                "priority": "high",
                "requested_at": "2026-04-19T12:00:00Z",
                "metadata": {"requested_by": "operator"},
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 202
    body = response.json()
    event = CrawlRequestEvent.model_validate(body["event"])
    published_event = CrawlRequestEvent.model_validate(publisher.calls[0]["payload"])

    assert body["pipeline_run"]["run_id"] == str(crawl_run_id)
    assert body["pipeline_run"]["run_type"] == PipelineRunType.CRAWL
    assert body["pipeline_run"]["trigger_type"] == PipelineTriggerType.MANUAL
    assert body["pipeline_run"]["status"] == PipelineRunStatus.PUBLISHED
    assert body["pipeline_run"]["metadata"]["published_queue"] == "parser.high"
    assert body["pipeline_run"]["metadata"]["published_exchange"] == "parser.jobs"
    assert body["pipeline_run"]["metadata"]["published_routing_key"] == "high"

    assert event.event_name == "crawl.request.v1"
    assert event.header.producer == "scheduler"
    assert event.payload.crawl_run_id == crawl_run_id
    assert event.payload.source_key == "msu-official"
    assert event.payload.endpoint_url == "https://example.edu"
    assert event.payload.priority == "high"
    assert event.payload.trigger == "manual"
    assert event.payload.parser_profile == "official_site.default"
    assert event.payload.metadata["endpoint_id"] == str(endpoint_id)
    assert event.payload.metadata["requested_by"] == "operator"
    assert event.payload.metadata["crawl_policy"]["timeout_seconds"] == 45
    assert event.payload.metadata["crawl_policy"]["render_mode"] == "http"

    assert published_event == event
    assert publisher.calls[0]["queue_name"] == "parser.high"
    assert publisher.calls[0]["headers"] == {
        "event_name": "crawl.request.v1",
        "event_id": str(event.header.event_id),
        "schema_version": "1",
        "crawl_run_id": str(crawl_run_id),
        "source_key": "msu-official",
        "priority": "high",
    }
    assert run_repository.commit_count == 2
