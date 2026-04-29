import hashlib
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from apps.parser.app.raw_artifacts import (
    RawArtifactPersistenceError,
    RawArtifactPersistenceService,
    RawArtifactRepository,
)
from libs.source_sdk import FetchContext, FetchedArtifact


class FakeMappingResult:
    def __init__(self, *, row=None) -> None:
        self._row = row

    def mappings(self) -> "FakeMappingResult":
        return self

    def one(self):
        return self._row

    def one_or_none(self):
        return self._row


class FakeRawArtifactSession:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], dict] = {}
        self.executed: list[tuple[str, dict]] = []
        self.commit_count = 0

    def commit(self) -> None:
        self.commit_count += 1

    def execute(self, statement: str, params: dict):
        normalized_statement = " ".join(statement.split()).lower()
        self.executed.append((normalized_statement, params))

        if normalized_statement.startswith("insert"):
            key = (params["source_key"], params["sha256"])
            existing = self.rows.get(key)
            if existing is None:
                row = dict(params)
            else:
                existing_metadata = json.loads(existing["metadata"])
                existing_metadata.update(json.loads(params["metadata"]))
                row = {
                    **existing,
                    **params,
                    "raw_artifact_id": existing["raw_artifact_id"],
                    "metadata": json.dumps(existing_metadata),
                }
            self.rows[key] = row
            return FakeMappingResult(row=row)
        if "from ingestion.raw_artifact" in normalized_statement:
            row = next(
                (
                    candidate
                    for candidate in self.rows.values()
                    if candidate["raw_artifact_id"] == params["raw_artifact_id"]
                ),
                None,
            )
            return FakeMappingResult(row=row)

        raise AssertionError(f"Unexpected statement: {statement}")


class FakeRawStore:
    def __init__(self, stored_artifact: FetchedArtifact) -> None:
        self.stored_artifact = stored_artifact
        self.calls: list[tuple[FetchContext, FetchedArtifact]] = []

    async def store_raw(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> FetchedArtifact:
        self.calls.append((context, artifact))
        return self.stored_artifact


def build_context() -> FetchContext:
    return FetchContext(
        crawl_run_id=uuid4(),
        source_key="msu-official",
        endpoint_url="https://example.edu/start",
        parser_profile="official_site.default",
        priority="high",
        trigger="manual",
        requested_at=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
    )


def build_stored_artifact(payload: bytes = b"<html>ok</html>") -> FetchedArtifact:
    sha256 = hashlib.sha256(payload).hexdigest()
    return FetchedArtifact(
        raw_artifact_id=uuid4(),
        crawl_run_id=uuid4(),
        source_key="msu-official",
        source_url="https://example.edu/start",
        final_url="https://example.edu/final",
        http_status=200,
        content_type="text/html; charset=utf-8",
        response_headers={"content-type": "text/html; charset=utf-8"},
        content_length=len(payload),
        sha256=sha256,
        fetched_at=datetime(2026, 4, 20, 12, 1, tzinfo=UTC),
        render_mode="http",
        etag='"abc123"',
        last_modified="Mon, 20 Apr 2026 10:00:00 GMT",
        storage_bucket="raw-html",
        storage_object_key=f"msu-official/{sha256[0:2]}/{sha256[2:4]}/{sha256}.html",
        content=payload,
        metadata={"minio_etag": "etag-1", "minio_version_id": "v1"},
    )


def test_raw_artifact_repository_upserts_stored_artifact_metadata() -> None:
    context = build_context()
    artifact = build_stored_artifact()
    session = FakeRawArtifactSession()
    repository = RawArtifactRepository(session, sql_text=lambda statement: statement)

    record = repository.upsert_from_artifact(context=context, artifact=artifact)

    assert record.raw_artifact_id == artifact.raw_artifact_id
    assert record.crawl_run_id == context.crawl_run_id
    assert record.source_key == "msu-official"
    assert record.source_url == "https://example.edu/start"
    assert record.final_url == "https://example.edu/final"
    assert record.http_status == 200
    assert record.content_type == "text/html; charset=utf-8"
    assert record.content_length == len(b"<html>ok</html>")
    assert record.sha256 == artifact.sha256
    assert record.storage_bucket == "raw-html"
    assert record.storage_object_key == artifact.storage_object_key
    assert record.etag == '"abc123"'
    assert record.last_modified == "Mon, 20 Apr 2026 10:00:00 GMT"
    assert record.fetched_at == datetime(2026, 4, 20, 12, 1, tzinfo=UTC)
    assert record.metadata["minio_etag"] == "etag-1"
    assert record.metadata["parser_profile"] == "official_site.default"
    assert record.metadata["priority"] == "high"
    assert record.metadata["trigger"] == "manual"
    assert record.metadata["requested_at"] == "2026-04-20T12:00:00+00:00"
    assert record.metadata["response_headers"] == {"content-type": "text/html; charset=utf-8"}
    assert session.executed[0][1]["metadata"] == json.dumps(
        record.metadata,
        ensure_ascii=False,
        sort_keys=True,
    )


def test_raw_artifact_repository_merges_metadata_on_same_source_sha256() -> None:
    context = build_context()
    artifact = build_stored_artifact()
    session = FakeRawArtifactSession()
    repository = RawArtifactRepository(session, sql_text=lambda statement: statement)
    first = repository.upsert_from_artifact(context=context, artifact=artifact)
    updated_artifact = artifact.model_copy(
        update={
            "raw_artifact_id": uuid4(),
            "final_url": "https://example.edu/redirected",
            "metadata": {"minio_etag": "etag-2", "retry": 1},
        }
    )

    second = repository.upsert_from_artifact(context=context, artifact=updated_artifact)

    assert second.raw_artifact_id == first.raw_artifact_id
    assert second.final_url == "https://example.edu/redirected"
    assert second.metadata["minio_etag"] == "etag-2"
    assert second.metadata["retry"] == 1
    assert second.metadata["parser_profile"] == "official_site.default"


def test_raw_artifact_repository_get_by_id_returns_persisted_record() -> None:
    context = build_context()
    artifact = build_stored_artifact()
    session = FakeRawArtifactSession()
    repository = RawArtifactRepository(session, sql_text=lambda statement: statement)
    persisted = repository.upsert_from_artifact(context=context, artifact=artifact)

    record = repository.get_by_id(persisted.raw_artifact_id)

    assert record == persisted


def test_raw_artifact_repository_requires_minio_storage_pointer() -> None:
    context = build_context()
    repository = RawArtifactRepository(
        FakeRawArtifactSession(),
        sql_text=lambda statement: statement,
    )

    with pytest.raises(RawArtifactPersistenceError, match="storage_bucket"):
        repository.upsert_from_artifact(
            context=context,
            artifact=build_stored_artifact().model_copy(update={"storage_bucket": None}),
        )

    with pytest.raises(RawArtifactPersistenceError, match="storage_object_key"):
        repository.upsert_from_artifact(
            context=context,
            artifact=build_stored_artifact().model_copy(update={"storage_object_key": None}),
        )


@pytest.mark.asyncio
async def test_raw_artifact_persistence_service_stores_raw_then_persists_metadata() -> None:
    context = build_context()
    fetched_artifact = build_stored_artifact().model_copy(
        update={"storage_bucket": None, "storage_object_key": None}
    )
    stored_artifact = build_stored_artifact()
    raw_store = FakeRawStore(stored_artifact)
    session = FakeRawArtifactSession()
    repository = RawArtifactRepository(session, sql_text=lambda statement: statement)
    service = RawArtifactPersistenceService(raw_store=raw_store, repository=repository)

    returned_artifact, record = await service.persist_after_successful_fetch(
        context=context,
        artifact=fetched_artifact,
    )

    assert raw_store.calls == [(context, fetched_artifact)]
    assert returned_artifact == stored_artifact
    assert record.storage_bucket == "raw-html"
    assert session.commit_count == 1
