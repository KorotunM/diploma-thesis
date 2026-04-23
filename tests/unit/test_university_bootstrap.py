from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from apps.normalizer.app.claims import (
    ClaimBuildResult,
    ClaimEvidenceRecord,
    ClaimRecord,
    ParsedDocumentSnapshot,
)
from apps.normalizer.app.persistence import json_from_db, json_to_db
from apps.normalizer.app.universities import (
    UniversityBootstrapError,
    UniversityBootstrapRepository,
    UniversityBootstrapService,
    deterministic_university_id,
)


class MappingResult:
    def __init__(self, row: dict[str, Any] | None = None) -> None:
        self._row = row

    def mappings(self) -> MappingResult:
        return self

    def one(self) -> dict[str, Any]:
        assert self._row is not None
        return self._row

    def one_or_none(self) -> dict[str, Any] | None:
        return self._row


class FakeUniversityBootstrapSession:
    def __init__(self) -> None:
        self.source_id = uuid4()
        self.created_at = datetime(2026, 4, 23, 9, 0, tzinfo=UTC)
        self.sources: dict[str, dict[str, Any]] = {
            "msu-official": {
                "source_id": self.source_id,
                "source_key": "msu-official",
                "source_type": "official_site",
                "trust_tier": "authoritative",
                "is_active": True,
                "metadata": json_to_db({"owner": "admissions"}),
            }
        }
        self.universities: dict[Any, dict[str, Any]] = {}
        self.commit_count = 0

    def execute(self, statement: str, params: dict[str, Any]) -> MappingResult:
        sql = " ".join(statement.split()).lower()
        if "from ingestion.source" in sql:
            return MappingResult(self.sources.get(params["source_key"]))
        if "insert into core.university" in sql:
            return MappingResult(self._upsert_university(params))
        raise AssertionError(f"Unexpected SQL statement: {statement}")

    def commit(self) -> None:
        self.commit_count += 1

    def _upsert_university(self, params: dict[str, Any]) -> dict[str, Any]:
        existing = self.universities.get(params["university_id"])
        row = {
            **params,
            "created_at": existing["created_at"] if existing else self.created_at,
        }
        if existing is not None:
            row["metadata"] = json.dumps(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        self.universities[params["university_id"]] = row
        return row


def build_claim_result(source_key: str = "msu-official") -> ClaimBuildResult:
    parsed_document_id = uuid4()
    raw_artifact_id = uuid4()
    claim_ids = [uuid4(), uuid4(), uuid4()]
    evidence_ids = [uuid4(), uuid4(), uuid4()]
    parsed_document = ParsedDocumentSnapshot(
        parsed_document_id=parsed_document_id,
        crawl_run_id=uuid4(),
        raw_artifact_id=raw_artifact_id,
        source_key=source_key,
        parser_profile="official_site.default",
        parser_version="0.1.0",
        entity_type="university",
        entity_hint="Example University",
        parsed_at=datetime(2026, 4, 23, 8, 58, tzinfo=UTC),
        metadata={},
    )
    claims = [
        ClaimRecord(
            claim_id=claim_ids[0],
            parsed_document_id=parsed_document_id,
            source_key=source_key,
            field_name="canonical_name",
            value="Example University",
            value_type="str",
            entity_hint="Example University",
            parser_version="0.1.0",
            normalizer_version="normalizer.0.1.0",
            parser_confidence=0.98,
            created_at=datetime(2026, 4, 23, 9, 0, tzinfo=UTC),
            metadata={"fragment_id": str(uuid4())},
        ),
        ClaimRecord(
            claim_id=claim_ids[1],
            parsed_document_id=parsed_document_id,
            source_key=source_key,
            field_name="contacts.website",
            value="https://www.example.edu/admissions",
            value_type="str",
            entity_hint="Example University",
            parser_version="0.1.0",
            normalizer_version="normalizer.0.1.0",
            parser_confidence=0.9,
            created_at=datetime(2026, 4, 23, 9, 0, tzinfo=UTC),
            metadata={"fragment_id": str(uuid4())},
        ),
        ClaimRecord(
            claim_id=claim_ids[2],
            parsed_document_id=parsed_document_id,
            source_key=source_key,
            field_name="location.city",
            value="Moscow",
            value_type="str",
            entity_hint="Example University",
            parser_version="0.1.0",
            normalizer_version="normalizer.0.1.0",
            parser_confidence=0.88,
            created_at=datetime(2026, 4, 23, 9, 0, tzinfo=UTC),
            metadata={"fragment_id": str(uuid4())},
        ),
    ]
    evidence = [
        ClaimEvidenceRecord(
            evidence_id=evidence_ids[index],
            claim_id=claim.claim_id,
            source_key=source_key,
            source_url="https://example.edu/admissions",
            raw_artifact_id=raw_artifact_id,
            fragment_id=uuid4(),
            captured_at=datetime(2026, 4, 23, 8, 55, tzinfo=UTC),
            metadata={"field_name": claim.field_name},
        )
        for index, claim in enumerate(claims)
    ]
    return ClaimBuildResult(
        parsed_document=parsed_document,
        claims=claims,
        evidence=evidence,
    )


def build_service(session: FakeUniversityBootstrapSession) -> UniversityBootstrapService:
    return UniversityBootstrapService(
        UniversityBootstrapRepository(session=session, sql_text=lambda value: value)
    )


def test_university_bootstrap_creates_core_record_from_authoritative_claims() -> None:
    session = FakeUniversityBootstrapSession()
    service = build_service(session)
    claim_result = build_claim_result()

    result = service.bootstrap_single_source_authoritative(claim_result)

    assert result.source.source_key == "msu-official"
    assert result.university.university_id == deterministic_university_id("msu-official")
    assert result.university.canonical_name == "Example University"
    assert result.university.canonical_domain == "example.edu"
    assert result.university.city_name == "Moscow"
    assert result.university.metadata["bootstrap_policy"] == "single_source_authoritative"
    assert result.university.metadata["source_id"] == str(session.source_id)
    assert result.university.metadata["claim_ids"] == [
        str(claim.claim_id) for claim in claim_result.claims
    ]
    assert result.university.metadata["evidence_ids"] == [
        str(record.evidence_id) for record in claim_result.evidence
    ]
    assert result.university.metadata["source_urls"] == [
        "https://example.edu/admissions"
    ]
    assert len(session.universities) == 1
    assert session.commit_count == 1


def test_university_bootstrap_is_idempotent_for_same_source() -> None:
    session = FakeUniversityBootstrapSession()
    service = build_service(session)
    claim_result = build_claim_result()

    first = service.bootstrap_single_source_authoritative(claim_result)
    second = service.bootstrap_single_source_authoritative(claim_result)

    assert second.university.university_id == first.university.university_id
    assert len(session.universities) == 1
    assert session.commit_count == 2


def test_university_bootstrap_rejects_non_authoritative_or_inactive_source() -> None:
    session = FakeUniversityBootstrapSession()
    service = build_service(session)
    session.sources["msu-official"]["trust_tier"] = "trusted"

    with pytest.raises(UniversityBootstrapError):
        service.bootstrap_single_source_authoritative(build_claim_result())

    session.sources["msu-official"]["trust_tier"] = "authoritative"
    session.sources["msu-official"]["is_active"] = False

    with pytest.raises(UniversityBootstrapError):
        service.bootstrap_single_source_authoritative(build_claim_result())


def test_university_bootstrap_requires_canonical_name_and_single_source() -> None:
    session = FakeUniversityBootstrapSession()
    service = build_service(session)
    missing_name = build_claim_result().model_copy(update={"claims": []})

    with pytest.raises(UniversityBootstrapError):
        service.bootstrap_single_source_authoritative(missing_name)

    mixed = build_claim_result()
    mixed_claims = list(mixed.claims)
    mixed_claims[1] = mixed_claims[1].model_copy(update={"source_key": "aggregator"})

    with pytest.raises(UniversityBootstrapError):
        service.bootstrap_single_source_authoritative(
            mixed.model_copy(update={"claims": mixed_claims})
        )
