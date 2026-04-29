from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from apps.parser.adapters.official_sites import OfficialSiteAdapter
from apps.parser.app.parsed_documents import (
    ParsedDocumentPersistenceError,
    ParsedDocumentPersistenceService,
    ParsedDocumentRepository,
)
from apps.parser.app.persistence import json_from_db, json_to_db
from libs.source_sdk import FetchContext, FetchedArtifact

FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "parser_ingestion"


class FakeFetcher:
    def __init__(self, artifact: FetchedArtifact) -> None:
        self.artifact = artifact

    async def fetch(self, context: FetchContext) -> FetchedArtifact:
        return self.artifact.model_copy(
            update={
                "crawl_run_id": context.crawl_run_id,
                "source_key": context.source_key,
            }
        )


class MappingResult:
    def __init__(
        self,
        row: dict[str, Any] | None = None,
        rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self._row = row
        self._rows = rows or []

    def mappings(self) -> MappingResult:
        return self

    def one(self) -> dict[str, Any]:
        return self._row

    def one_or_none(self) -> dict[str, Any] | None:
        return self._row

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class FakeParsedDocumentSession:
    def __init__(self) -> None:
        self.documents: dict[tuple[Any, str], dict[str, Any]] = {}
        self.fragments: dict[Any, dict[str, Any]] = {}
        self.commit_count = 0

    def execute(self, statement: Any, params: dict[str, Any]) -> MappingResult:
        sql = str(statement).lower()
        if "insert into parsing.parsed_document" in sql:
            row = self._upsert_document(params)
            return MappingResult(row)
        if "insert into parsing.extracted_fragment" in sql:
            row = self._upsert_fragment(params)
            return MappingResult(row)
        if "from parsing.parsed_document" in sql:
            row = self.documents.get((params["raw_artifact_id"], params["parser_version"]))
            return MappingResult(row)
        if "from parsing.extracted_fragment" in sql:
            rows = [
                fragment
                for fragment in self.fragments.values()
                if fragment["parsed_document_id"] == params["parsed_document_id"]
            ]
            rows.sort(key=lambda row: (row["field_name"], str(row["fragment_id"])))
            return MappingResult(rows=rows)
        raise AssertionError(f"Unexpected SQL statement: {statement}")

    def commit(self) -> None:
        self.commit_count += 1

    def _upsert_document(self, params: dict[str, Any]) -> dict[str, Any]:
        key = (params["raw_artifact_id"], params["parser_version"])
        existing = self.documents.get(key)
        row = dict(params)
        if existing is not None:
            row["parsed_document_id"] = existing["parsed_document_id"]
            row["metadata"] = json_to_db(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                }
            )
        self.documents[key] = row
        return row

    def _upsert_fragment(self, params: dict[str, Any]) -> dict[str, Any]:
        key = params["fragment_id"]
        existing = self.fragments.get(key)
        row = dict(params)
        if existing is not None:
            row["metadata"] = json_to_db(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                }
            )
        self.fragments[key] = row
        return row


def build_context() -> FetchContext:
    return FetchContext(
        crawl_run_id=uuid4(),
        source_key="msu-official",
        endpoint_url="https://example.edu/admissions",
        parser_profile="official_site.default",
    )


def build_artifact() -> FetchedArtifact:
    content = (FIXTURE_ROOT / "official_site_admissions.html").read_bytes()
    return FetchedArtifact(
        raw_artifact_id=uuid4(),
        source_url="https://example.edu/admissions",
        final_url="https://example.edu/admissions",
        http_status=200,
        content_type="text/html; charset=utf-8",
        content_length=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        fetched_at=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
        storage_bucket="raw-html",
        storage_object_key="msu-official/official-site.html",
        content=content,
    )


def build_official_execution_result():
    context = build_context()
    adapter = OfficialSiteAdapter(fetcher=FakeFetcher(build_artifact()))
    return asyncio.run(adapter.execute(context))


def test_parsed_document_repository_persists_official_execution_result() -> None:
    execution_result = build_official_execution_result()
    session = FakeParsedDocumentSession()
    repository = ParsedDocumentRepository(session=session, sql_text=lambda value: value)

    document = repository.upsert_document(execution_result=execution_result)
    fragments = repository.upsert_fragments(
        parsed_document=document,
        fragments=execution_result.fragments,
    )

    assert document.crawl_run_id == execution_result.crawl_run_id
    assert document.raw_artifact_id == execution_result.artifact.raw_artifact_id
    assert document.source_key == "msu-official"
    assert document.parser_profile == "official_site.default"
    assert document.parser_version == "0.1.0"
    assert document.entity_type == "university"
    assert document.entity_hint == "Example University"
    assert document.extracted_fragment_count == len(execution_result.fragments)
    assert document.metadata["adapter_key"] == "official_sites:0.1.0"
    assert document.metadata["claim_count"] == len(execution_result.fragments)
    assert len(document.metadata["fragment_ids"]) == len(execution_result.fragments)

    by_field = {fragment.field_name: fragment for fragment in fragments}
    assert by_field["canonical_name"].value == "Example University"
    assert by_field["contacts.emails"].value == ["admissions@example.edu"]
    assert by_field["contacts.emails"].value_type == "list"
    assert by_field["location.city"].raw_artifact_id == document.raw_artifact_id
    assert all(
        fragment.metadata["parsed_document_id"] == str(document.parsed_document_id)
        for fragment in fragments
    )


def test_parsed_document_repository_is_idempotent_by_raw_artifact_and_parser_version() -> None:
    execution_result = build_official_execution_result()
    session = FakeParsedDocumentSession()
    repository = ParsedDocumentRepository(session=session, sql_text=lambda value: value)

    first = repository.upsert_document(execution_result=execution_result)
    second = repository.upsert_document(execution_result=execution_result)

    assert second.parsed_document_id == first.parsed_document_id
    assert len(session.documents) == 1


def test_parsed_document_repository_can_load_document_and_fragments_for_replay() -> None:
    execution_result = build_official_execution_result()
    session = FakeParsedDocumentSession()
    repository = ParsedDocumentRepository(session=session, sql_text=lambda value: value)

    persisted_document = repository.upsert_document(execution_result=execution_result)
    persisted_fragments = repository.upsert_fragments(
        parsed_document=persisted_document,
        fragments=execution_result.fragments,
    )

    loaded_document = repository.get_document_by_raw_artifact_and_parser_version(
        raw_artifact_id=persisted_document.raw_artifact_id,
        parser_version=persisted_document.parser_version,
    )
    loaded_fragments = repository.list_fragments_for_document(
        persisted_document.parsed_document_id
    )

    assert loaded_document == persisted_document
    assert loaded_fragments == sorted(
        persisted_fragments,
        key=lambda fragment: (fragment.field_name, str(fragment.fragment_id)),
    )


def test_parsed_document_repository_requires_completed_execution_artifact_and_records() -> None:
    execution_result = build_official_execution_result()
    session = FakeParsedDocumentSession()
    repository = ParsedDocumentRepository(session=session, sql_text=lambda value: value)

    with pytest.raises(ParsedDocumentPersistenceError):
        repository.upsert_document(
            execution_result=execution_result.model_copy(update={"artifact": None})
        )
    with pytest.raises(ParsedDocumentPersistenceError):
        repository.upsert_document(
            execution_result=execution_result.model_copy(update={"completed_at": None})
        )
    with pytest.raises(ParsedDocumentPersistenceError):
        repository.upsert_document(
            execution_result=execution_result.model_copy(update={"intermediate_records": []})
        )


def test_parsed_document_persistence_service_commits_document_and_fragments() -> None:
    execution_result = build_official_execution_result()
    session = FakeParsedDocumentSession()
    repository = ParsedDocumentRepository(session=session, sql_text=lambda value: value)
    service = ParsedDocumentPersistenceService(repository)

    document, fragments = service.persist_successful_execution(
        execution_result=execution_result
    )

    assert document.raw_artifact_id == execution_result.artifact.raw_artifact_id
    assert len(fragments) == len(execution_result.fragments)
    assert len(session.documents) == 1
    assert len(session.fragments) == len(execution_result.fragments)
    assert session.commit_count == 1
