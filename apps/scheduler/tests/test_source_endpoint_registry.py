from uuid import uuid4

import pytest
from pydantic import ValidationError

from apps.scheduler.app.sources.endpoint_repository import (
    SourceEndpointAlreadyExistsError,
    SourceEndpointRepository,
    SourceNotFoundError,
)
from apps.scheduler.app.sources.models import (
    CrawlPolicy,
    CreateSourceEndpointRequest,
    UpdateSourceEndpointRequest,
)


class FakeMappingResult:
    def __init__(self, *, row=None, rows=None, scalar=None) -> None:
        self._row = row
        self._rows = rows or []
        self._scalar = scalar

    def mappings(self) -> "FakeMappingResult":
        return self

    def one(self):
        return self._row

    def one_or_none(self):
        return self._row

    def all(self):
        return self._rows

    def scalar_one(self):
        return self._scalar

    def scalar_one_or_none(self):
        return self._scalar


class FakeEndpointSession:
    def __init__(self) -> None:
        self.source_ids: dict[str, object] = {}
        self.endpoints: dict[object, dict] = {}
        self.executed: list[tuple[str, dict]] = []

    def add_source(self, source_key: str, source_id=None) -> object:
        resolved_source_id = source_id or uuid4()
        self.source_ids[source_key] = resolved_source_id
        return resolved_source_id

    def execute(self, statement: str, params: dict):
        normalized_statement = " ".join(statement.split()).lower()
        self.executed.append((normalized_statement, params))

        if normalized_statement.startswith("select source_id"):
            return FakeMappingResult(scalar=self.source_ids.get(params["source_key"]))

        if normalized_statement.startswith("select") and "count(*)" in normalized_statement:
            rows = self._rows_for_source(params["source_key"])
            return FakeMappingResult(scalar=len(rows))

        if normalized_statement.startswith("select"):
            if "limit" not in params:
                row = self._select_endpoint_row(params)
                return FakeMappingResult(row=row)
            rows = self._rows_for_source(params["source_key"])
            return FakeMappingResult(
                rows=rows[params["offset"] : params["offset"] + params["limit"]]
            )

        if normalized_statement.startswith("insert"):
            row = {
                "endpoint_id": params["endpoint_id"],
                "source_id": params["source_id"],
                "source_key": params["source_key"],
                "endpoint_url": params["endpoint_url"],
                "parser_profile": params["parser_profile"],
                "crawl_policy": params["crawl_policy"],
            }
            self.endpoints[params["endpoint_id"]] = row
            return FakeMappingResult(row=row)

        if normalized_statement.startswith("update"):
            row = self.endpoints.get(params["endpoint_id"])
            if row is None or row["source_key"] != params["source_key"]:
                return FakeMappingResult(row=None)
            if params["endpoint_url"] is not None:
                row["endpoint_url"] = params["endpoint_url"]
            if params["parser_profile"] is not None:
                row["parser_profile"] = params["parser_profile"]
            if params["crawl_policy_is_set"]:
                row["crawl_policy"] = params["crawl_policy"]
            return FakeMappingResult(row=row)

        raise AssertionError(f"Unexpected statement: {statement}")

    def _select_endpoint_row(self, params: dict) -> dict | None:
        for row in self.endpoints.values():
            if row["source_key"] != params["source_key"]:
                continue
            if "endpoint_url" in params and row["endpoint_url"] == params["endpoint_url"]:
                return row
            if "endpoint_id" in params and row["endpoint_id"] == params["endpoint_id"]:
                return row
        return None

    def _rows_for_source(self, source_key: str) -> list[dict]:
        return sorted(
            [row for row in self.endpoints.values() if row["source_key"] == source_key],
            key=lambda row: row["endpoint_url"],
        )


def test_crawl_policy_requires_interval_when_schedule_enabled() -> None:
    with pytest.raises(ValidationError):
        CrawlPolicy(schedule_enabled=True)


def test_source_endpoint_request_normalizes_url_and_validates_parser_profile() -> None:
    request = CreateSourceEndpointRequest(
        endpoint_url="https://example.edu/",
        parser_profile="official_site.default",
    )

    assert request.normalized_endpoint_url == "https://example.edu"

    with pytest.raises(ValidationError):
        CreateSourceEndpointRequest(
            endpoint_url="https://example.edu",
            parser_profile="Bad Profile",
        )


def test_source_endpoint_repository_rejects_missing_source() -> None:
    session = FakeEndpointSession()
    repository = SourceEndpointRepository(session, sql_text=lambda statement: statement)

    with pytest.raises(SourceNotFoundError):
        repository.create(
            "missing-source",
            CreateSourceEndpointRequest(endpoint_url="https://example.edu"),
        )


def test_source_endpoint_repository_creates_and_reads_endpoint() -> None:
    session = FakeEndpointSession()
    source_id = session.add_source("msu-official")
    repository = SourceEndpointRepository(session, sql_text=lambda statement: statement)
    endpoint_id = uuid4()

    created = repository.create(
        "msu-official",
        CreateSourceEndpointRequest(
            endpoint_id=endpoint_id,
            endpoint_url="https://example.edu/",
            parser_profile="official_site.default",
            crawl_policy=CrawlPolicy(
                schedule_enabled=True,
                interval_seconds=86400,
                render_mode="http",
            ),
        ),
    )
    loaded = repository.get("msu-official", endpoint_id)

    assert created.endpoint_id == endpoint_id
    assert created.source_id == source_id
    assert created.source_key == "msu-official"
    assert created.endpoint_url == "https://example.edu"
    assert created.crawl_policy.schedule_enabled is True
    assert created.crawl_policy.interval_seconds == 86400
    assert loaded == created


def test_source_endpoint_repository_rejects_duplicate_endpoint_url_per_source() -> None:
    session = FakeEndpointSession()
    session.add_source("msu-official")
    repository = SourceEndpointRepository(session, sql_text=lambda statement: statement)
    request = CreateSourceEndpointRequest(endpoint_url="https://example.edu")

    repository.create("msu-official", request)

    with pytest.raises(SourceEndpointAlreadyExistsError):
        repository.create("msu-official", request)


def test_source_endpoint_repository_lists_endpoints_for_source() -> None:
    session = FakeEndpointSession()
    session.add_source("msu-official")
    session.add_source("other-source")
    repository = SourceEndpointRepository(session, sql_text=lambda statement: statement)

    repository.create("msu-official", CreateSourceEndpointRequest(endpoint_url="https://b.edu"))
    repository.create("msu-official", CreateSourceEndpointRequest(endpoint_url="https://a.edu"))
    repository.create("other-source", CreateSourceEndpointRequest(endpoint_url="https://z.edu"))

    items, total = repository.list("msu-official", limit=50, offset=0)

    assert total == 2
    assert [item.endpoint_url for item in items] == ["https://a.edu", "https://b.edu"]


def test_source_endpoint_repository_updates_endpoint_policy_without_all_fields() -> None:
    session = FakeEndpointSession()
    session.add_source("msu-official")
    repository = SourceEndpointRepository(session, sql_text=lambda statement: statement)
    created = repository.create(
        "msu-official",
        CreateSourceEndpointRequest(endpoint_url="https://example.edu"),
    )

    updated = repository.update(
        "msu-official",
        created.endpoint_id,
        UpdateSourceEndpointRequest(
            parser_profile="official_site.priority",
            crawl_policy=CrawlPolicy(timeout_seconds=60, max_retries=5),
        ),
    )

    assert updated is not None
    assert updated.endpoint_url == "https://example.edu"
    assert updated.parser_profile == "official_site.priority"
    assert updated.crawl_policy.timeout_seconds == 60
    assert updated.crawl_policy.max_retries == 5


def test_source_endpoint_repository_rejects_duplicate_endpoint_url_on_update() -> None:
    session = FakeEndpointSession()
    session.add_source("msu-official")
    repository = SourceEndpointRepository(session, sql_text=lambda statement: statement)
    first = repository.create(
        "msu-official",
        CreateSourceEndpointRequest(endpoint_url="https://first.edu"),
    )
    repository.create(
        "msu-official",
        CreateSourceEndpointRequest(endpoint_url="https://second.edu"),
    )

    with pytest.raises(SourceEndpointAlreadyExistsError):
        repository.update(
            "msu-official",
            first.endpoint_id,
            UpdateSourceEndpointRequest(endpoint_url="https://second.edu"),
        )
