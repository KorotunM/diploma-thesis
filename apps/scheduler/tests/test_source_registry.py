from uuid import uuid4

import pytest
from pydantic import ValidationError

from apps.scheduler.app.sources.models import (
    CreateSourceRequest,
    SourceTrustTier,
    SourceType,
    UpdateSourceRequest,
)
from apps.scheduler.app.sources.repository import SourceAlreadyExistsError, SourceRepository


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


class FakeSourceSession:
    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}
        self.executed: list[tuple[str, dict]] = []

    def execute(self, statement: str, params: dict):
        normalized_statement = " ".join(statement.split()).lower()
        self.executed.append((normalized_statement, params))

        if normalized_statement.startswith("select") and "count(*)" in normalized_statement:
            rows = self._filtered_rows(params["include_inactive"])
            return FakeMappingResult(scalar=len(rows))

        if normalized_statement.startswith("select") and "where source_key" in normalized_statement:
            return FakeMappingResult(row=self.rows.get(params["source_key"]))

        if normalized_statement.startswith("select"):
            rows = self._filtered_rows(params["include_inactive"])
            return FakeMappingResult(
                rows=rows[params["offset"] : params["offset"] + params["limit"]]
            )

        if normalized_statement.startswith("insert"):
            row = {
                "source_id": params["source_id"],
                "source_key": params["source_key"],
                "source_type": params["source_type"],
                "trust_tier": params["trust_tier"],
                "is_active": params["is_active"],
                "metadata": params["metadata"],
            }
            self.rows[params["source_key"]] = row
            return FakeMappingResult(row=row)

        if normalized_statement.startswith("update"):
            row = self.rows.get(params["source_key"])
            if row is None:
                return FakeMappingResult(row=None)
            if params["source_type"] is not None:
                row["source_type"] = params["source_type"]
            if params["trust_tier"] is not None:
                row["trust_tier"] = params["trust_tier"]
            if params["is_active"] is not None:
                row["is_active"] = params["is_active"]
            if params["metadata_is_set"]:
                row["metadata"] = params["metadata"]
            return FakeMappingResult(row=row)

        raise AssertionError(f"Unexpected statement: {statement}")

    def _filtered_rows(self, include_inactive: bool) -> list[dict]:
        rows = sorted(self.rows.values(), key=lambda row: row["source_key"])
        if include_inactive:
            return rows
        return [row for row in rows if row["is_active"]]


def test_create_source_request_validates_source_key_shape() -> None:
    with pytest.raises(ValidationError):
        CreateSourceRequest(
            source_key="Bad Key",
            source_type=SourceType.OFFICIAL_SITE,
            trust_tier=SourceTrustTier.AUTHORITATIVE,
        )


def test_update_source_request_requires_at_least_one_field() -> None:
    with pytest.raises(ValidationError):
        UpdateSourceRequest()


def test_source_repository_creates_and_reads_source() -> None:
    session = FakeSourceSession()
    repository = SourceRepository(session, sql_text=lambda statement: statement)
    source_id = uuid4()

    created = repository.create(
        CreateSourceRequest(
            source_id=source_id,
            source_key="msu-official",
            source_type=SourceType.OFFICIAL_SITE,
            trust_tier=SourceTrustTier.AUTHORITATIVE,
            metadata={"owner": "scheduler"},
        )
    )
    loaded = repository.get_by_key("msu-official")

    assert created.source_id == source_id
    assert created.source_key == "msu-official"
    assert created.metadata == {"owner": "scheduler"}
    assert loaded == created


def test_source_repository_rejects_duplicate_source_key() -> None:
    session = FakeSourceSession()
    repository = SourceRepository(session, sql_text=lambda statement: statement)
    request = CreateSourceRequest(
        source_key="ranking-provider",
        source_type=SourceType.RANKING,
        trust_tier=SourceTrustTier.TRUSTED,
    )

    repository.create(request)

    with pytest.raises(SourceAlreadyExistsError):
        repository.create(request)


def test_source_repository_lists_only_active_sources_by_default() -> None:
    session = FakeSourceSession()
    repository = SourceRepository(session, sql_text=lambda statement: statement)

    repository.create(
        CreateSourceRequest(
            source_key="inactive-aggregator",
            source_type=SourceType.AGGREGATOR,
            trust_tier=SourceTrustTier.AUXILIARY,
            is_active=False,
        )
    )
    repository.create(
        CreateSourceRequest(
            source_key="active-official",
            source_type=SourceType.OFFICIAL_SITE,
            trust_tier=SourceTrustTier.AUTHORITATIVE,
        )
    )

    items, total = repository.list(limit=50, offset=0, include_inactive=False)

    assert total == 1
    assert [item.source_key for item in items] == ["active-official"]


def test_source_repository_updates_source_without_requiring_all_fields() -> None:
    session = FakeSourceSession()
    repository = SourceRepository(session, sql_text=lambda statement: statement)
    repository.create(
        CreateSourceRequest(
            source_key="exp-source",
            source_type=SourceType.AGGREGATOR,
            trust_tier=SourceTrustTier.EXPERIMENTAL,
            metadata={"stage": "sandbox"},
        )
    )

    updated = repository.update(
        "exp-source",
        UpdateSourceRequest(
            trust_tier=SourceTrustTier.TRUSTED,
            is_active=False,
            metadata={"stage": "approved"},
        ),
    )

    assert updated is not None
    assert updated.source_type == SourceType.AGGREGATOR
    assert updated.trust_tier == SourceTrustTier.TRUSTED
    assert updated.is_active is False
    assert updated.metadata == {"stage": "approved"}
