from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from apps.backend.app.dependencies import get_university_provenance_read_service
from apps.backend.app.main import app
from apps.backend.app.provenance import (
    UniversityProvenanceNotFoundError,
    UniversityProvenanceReadService,
    UniversityProvenanceRepository,
)
from libs.domain.university import UniversityCard


class MappingResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> MappingResult:
        return self

    def one_or_none(self) -> dict[str, Any] | None:
        if not self._rows:
            return None
        return self._rows[0]

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class FakeProvenanceSession:
    def __init__(self, datasets: dict[str, list[dict[str, Any]]]) -> None:
        self._datasets = datasets
        self.calls: list[dict[str, Any]] = []

    def execute(self, statement: str, params: dict[str, Any]) -> MappingResult:
        sql = " ".join(statement.split()).lower()
        self.calls.append({"statement": statement, "params": params})
        if "from delivery.university_card as projection" in sql:
            return MappingResult(self._datasets.get("projection", []))
        if "select resolved_fact_id" in sql and "from core.resolved_fact" in sql:
            return MappingResult(self._datasets.get("facts", []))
        if "with selected_claim_documents as" in sql:
            return MappingResult(self._datasets.get("raw_artifacts", []))
        if (
            "from core.resolved_fact as fact" in sql
            and "join parsing.parsed_document as document" in sql
        ):
            return MappingResult(self._datasets.get("parsed_documents", []))
        if "from core.resolved_fact as fact" in sql and "join normalize.claim as claim" in sql:
            return MappingResult(self._datasets.get("claims", []))
        if (
            "from core.resolved_fact as fact" in sql
            and "join normalize.claim_evidence as evidence" in sql
        ):
            return MappingResult(self._datasets.get("evidence", []))
        raise AssertionError(f"Unexpected SQL: {statement}")


class FakeUniversityProvenanceReadService:
    def __init__(self, payload: dict[str, Any] | None) -> None:
        self.payload = payload
        self.calls: list[UUID] = []

    def get_latest_trace(self, university_id: UUID):
        self.calls.append(university_id)
        if self.payload is None:
            raise UniversityProvenanceNotFoundError(university_id)
        return self.payload


def build_card(university_id: UUID) -> UniversityCard:
    return UniversityCard.sample().model_copy(update={"university_id": university_id})


def build_repository_datasets(university_id: UUID) -> dict[str, list[dict[str, Any]]]:
    resolved_fact_id = uuid4()
    claim_id = uuid4()
    evidence_id = uuid4()
    parsed_document_id = uuid4()
    raw_artifact_id = uuid4()
    now = datetime(2026, 4, 23, 15, 0, tzinfo=UTC)
    card = build_card(university_id)
    return {
        "projection": [
            {
                "university_id": university_id,
                "card_version": 1,
                "card_json": json.dumps(card.model_dump(mode="json"), sort_keys=True),
                "projection_generated_at": now,
                "card_generated_at": now,
                "normalizer_version": "normalizer.0.1.0",
            }
        ],
        "facts": [
            {
                "resolved_fact_id": resolved_fact_id,
                "university_id": university_id,
                "field_name": "canonical_name",
                "value_json": json.dumps(
                    {"value": "Example State University", "value_type": "string"},
                    sort_keys=True,
                ),
                "fact_score": 0.99,
                "resolution_policy": "single_source_authoritative_highest_confidence",
                "card_version": 1,
                "resolved_at": now,
                "metadata": json.dumps(
                    {
                        "source_key": "official-site",
                        "source_urls": ["https://example.edu"],
                        "selected_claim_ids": [str(claim_id)],
                        "selected_evidence_ids": [str(evidence_id)],
                    },
                    sort_keys=True,
                ),
            }
        ],
        "claims": [
            {
                "claim_id": claim_id,
                "parsed_document_id": parsed_document_id,
                "source_key": "official-site",
                "field_name": "canonical_name",
                "value_json": json.dumps(
                    {"value": "Example State University", "value_type": "string"},
                    sort_keys=True,
                ),
                "entity_hint": "Example State University",
                "parser_version": "official-site.0.1.0",
                "normalizer_version": "normalizer.0.1.0",
                "parser_confidence": 0.99,
                "created_at": now,
                "metadata": json.dumps({"fragment_id": str(uuid4())}, sort_keys=True),
            }
        ],
        "evidence": [
            {
                "evidence_id": evidence_id,
                "claim_id": claim_id,
                "raw_artifact_id": raw_artifact_id,
                "fragment_id": uuid4(),
                "source_key": "official-site",
                "source_url": "https://example.edu/about",
                "captured_at": now,
                "metadata": json.dumps({"locator": "#contacts"}, sort_keys=True),
            }
        ],
        "parsed_documents": [
            {
                "parsed_document_id": parsed_document_id,
                "crawl_run_id": uuid4(),
                "raw_artifact_id": raw_artifact_id,
                "source_key": "official-site",
                "parser_profile": "official",
                "parser_version": "official-site.0.1.0",
                "entity_type": "university",
                "entity_hint": "Example State University",
                "extracted_fragment_count": 4,
                "parsed_at": now,
                "metadata": json.dumps({"adapter": "official-site"}, sort_keys=True),
            }
        ],
        "raw_artifacts": [
            {
                "raw_artifact_id": raw_artifact_id,
                "crawl_run_id": uuid4(),
                "source_key": "official-site",
                "source_url": "https://example.edu/about",
                "final_url": "https://example.edu/about",
                "http_status": 200,
                "content_type": "text/html",
                "content_length": 4096,
                "sha256": "abc123",
                "storage_bucket": "raw-artifacts",
                "storage_object_key": "sha256/ab/c123.html",
                "etag": "\"etag\"",
                "last_modified": "Thu, 23 Apr 2026 12:00:00 GMT",
                "fetched_at": now,
                "metadata": json.dumps(
                    {"headers": {"cache-control": "max-age=60"}},
                    sort_keys=True,
                ),
            }
        ],
    }


def test_university_provenance_repository_reads_stitched_trace_layers() -> None:
    university_id = uuid4()
    session = FakeProvenanceSession(build_repository_datasets(university_id))
    repository = UniversityProvenanceRepository(session=session, sql_text=lambda value: value)

    projection = repository.get_latest_projection_context(university_id)
    assert projection is not None
    assert projection.university_id == university_id
    assert projection.card.university_id == university_id
    assert projection.normalizer_version == "normalizer.0.1.0"

    facts = repository.list_resolved_facts(university_id=university_id, card_version=1)
    claims = repository.list_claims_for_card(university_id=university_id, card_version=1)
    evidence = repository.list_claim_evidence_for_card(university_id=university_id, card_version=1)
    parsed_documents = repository.list_parsed_documents_for_card(
        university_id=university_id,
        card_version=1,
    )
    raw_artifacts = repository.list_raw_artifacts_for_card(
        university_id=university_id,
        card_version=1,
    )

    assert len(facts) == 1
    assert facts[0].selected_claim_ids == [claims[0].claim_id]
    assert facts[0].selected_evidence_ids == [evidence[0].evidence_id]
    assert claims[0].parsed_document_id == parsed_documents[0].parsed_document_id
    assert evidence[0].raw_artifact_id == raw_artifacts[0].raw_artifact_id
    assert session.calls[0]["params"] == {"university_id": university_id}


def test_university_provenance_read_service_raises_when_projection_missing() -> None:
    repository = UniversityProvenanceRepository(
        session=FakeProvenanceSession({}),
        sql_text=lambda value: value,
    )
    service = UniversityProvenanceReadService(repository)

    with pytest.raises(UniversityProvenanceNotFoundError):
        service.get_latest_trace(uuid4())


def test_university_provenance_endpoint_serves_stitched_trace() -> None:
    university_id = uuid4()
    payload = {
        "university_id": str(university_id),
        "chain": [
            "raw",
            "parsed",
            "claims",
            "resolved_facts",
            "card_version",
            "delivery_projection",
        ],
        "delivery_projection": {
            "university_id": str(university_id),
            "card_version": 1,
            "card": build_card(university_id).model_dump(mode="json"),
            "projection_generated_at": "2026-04-23T15:00:00Z",
            "card_generated_at": "2026-04-23T15:00:00Z",
            "normalizer_version": "normalizer.0.1.0",
        },
        "resolved_facts": [],
        "claims": [],
        "claim_evidence": [],
        "parsed_documents": [],
        "raw_artifacts": [],
    }
    service = FakeUniversityProvenanceReadService(payload)
    app.dependency_overrides[get_university_provenance_read_service] = lambda: service
    try:
        response = TestClient(app).get(f"/api/v1/universities/{university_id}/provenance")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["university_id"] == str(university_id)
    assert body["delivery_projection"]["card_version"] == 1
    assert body["chain"][-1] == "delivery_projection"
    assert service.calls == [university_id]


def test_university_provenance_endpoint_returns_404_when_trace_missing() -> None:
    university_id = uuid4()
    service = FakeUniversityProvenanceReadService(None)
    app.dependency_overrides[get_university_provenance_read_service] = lambda: service
    try:
        response = TestClient(app).get(f"/api/v1/universities/{university_id}/provenance")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json()["detail"] == f"University provenance {university_id} was not found."
