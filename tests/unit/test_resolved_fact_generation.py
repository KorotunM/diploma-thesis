from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from apps.normalizer.app.claims import ClaimEvidenceRecord, ClaimRecord
from apps.normalizer.app.facts import (
    CANONICAL_FACT_FIELDS,
    ResolvedFactGenerationService,
    ResolvedFactRepository,
    deterministic_resolved_fact_id,
)
from apps.normalizer.app.persistence import json_from_db
from apps.normalizer.app.universities import (
    SourceAuthorityRecord,
    UniversityBootstrapResult,
    UniversityRecord,
    deterministic_university_id,
)


class MappingResult:
    def __init__(self, row: dict[str, Any]) -> None:
        self._row = row

    def mappings(self) -> MappingResult:
        return self

    def one(self) -> dict[str, Any]:
        return self._row


class FakeResolvedFactSession:
    def __init__(self) -> None:
        self.resolved_at = datetime(2026, 4, 23, 10, 0, tzinfo=UTC)
        self.facts: dict[UUID, dict[str, Any]] = {}
        self.commit_count = 0

    def execute(self, statement: str, params: dict[str, Any]) -> MappingResult:
        sql = " ".join(statement.split()).lower()
        if "insert into core.resolved_fact" in sql:
            return MappingResult(self._upsert_fact(params))
        raise AssertionError(f"Unexpected SQL statement: {statement}")

    def commit(self) -> None:
        self.commit_count += 1

    def _upsert_fact(self, params: dict[str, Any]) -> dict[str, Any]:
        existing = self.facts.get(params["resolved_fact_id"])
        row = {
            **params,
            "resolved_at": self.resolved_at,
        }
        if existing is not None:
            metadata = {
                **json_from_db(existing["metadata"]),
                **json_from_db(params["metadata"]),
            }
            row["metadata"] = json.dumps(metadata, ensure_ascii=False, sort_keys=True)
        self.facts[params["resolved_fact_id"]] = row
        return row


def claim(
    *,
    field_name: str,
    value,
    confidence: float,
    claim_id: UUID | None = None,
) -> ClaimRecord:
    return ClaimRecord(
        claim_id=claim_id or uuid4(),
        parsed_document_id=uuid4(),
        source_key="msu-official",
        field_name=field_name,
        value=value,
        value_type="str" if isinstance(value, str) else "null",
        entity_hint="Example University",
        parser_version="0.1.0",
        normalizer_version="normalizer.0.1.0",
        parser_confidence=confidence,
        created_at=datetime(2026, 4, 23, 9, 0, tzinfo=UTC),
        metadata={"fragment_id": str(uuid4())},
    )


def evidence_for(claim_record: ClaimRecord) -> ClaimEvidenceRecord:
    return ClaimEvidenceRecord(
        evidence_id=uuid4(),
        claim_id=claim_record.claim_id,
        source_key=claim_record.source_key,
        source_url="https://example.edu/admissions",
        raw_artifact_id=uuid4(),
        fragment_id=uuid4(),
        captured_at=datetime(2026, 4, 23, 8, 55, tzinfo=UTC),
        metadata={"field_name": claim_record.field_name},
    )


def build_bootstrap_result() -> UniversityBootstrapResult:
    university_id = deterministic_university_id("msu-official")
    selected_name = claim(
        field_name="canonical_name",
        value="Example University",
        confidence=0.98,
    )
    lower_confidence_name = claim(
        field_name="canonical_name",
        value="Example University Old Name",
        confidence=0.5,
    )
    website = claim(
        field_name="contacts.website",
        value="https://example.edu",
        confidence=0.9,
    )
    city = claim(field_name="location.city", value="Moscow", confidence=0.88)
    country = claim(field_name="location.country_code", value="RU", confidence=0.8)
    email = claim(
        field_name="contacts.emails",
        value=["admissions@example.edu"],
        confidence=0.9,
    )
    null_field = claim(field_name="location.country_code", value=None, confidence=0.99)
    claims = [
        selected_name,
        lower_confidence_name,
        website,
        city,
        country,
        email,
        null_field,
    ]
    return UniversityBootstrapResult(
        source=SourceAuthorityRecord(
            source_id=uuid4(),
            source_key="msu-official",
            source_type="official_site",
            trust_tier="authoritative",
            is_active=True,
        ),
        university=UniversityRecord(
            university_id=university_id,
            canonical_name="Example University",
            canonical_domain="example.edu",
            country_code="RU",
            city_name="Moscow",
            created_at=datetime(2026, 4, 23, 9, 5, tzinfo=UTC),
            metadata={"bootstrap_policy": "single_source_authoritative"},
        ),
        claims_used=claims,
        evidence_used=[evidence_for(claim_record) for claim_record in claims],
    )


def build_service(session: FakeResolvedFactSession) -> ResolvedFactGenerationService:
    return ResolvedFactGenerationService(
        ResolvedFactRepository(session=session, sql_text=lambda value: value)
    )


def test_resolved_fact_generation_persists_canonical_fields() -> None:
    session = FakeResolvedFactSession()
    service = build_service(session)
    bootstrap_result = build_bootstrap_result()

    result = service.generate_for_bootstrap(bootstrap_result)

    assert result.university == bootstrap_result.university
    assert {fact.field_name for fact in result.facts} == set(CANONICAL_FACT_FIELDS)
    by_field = {fact.field_name: fact for fact in result.facts}
    assert by_field["canonical_name"].value == "Example University"
    assert by_field["canonical_name"].fact_score == 0.98
    assert by_field["contacts.website"].value == "https://example.edu"
    assert by_field["location.city"].value == "Moscow"
    assert by_field["location.country_code"].value == "RU"
    assert by_field["contacts.website"].resolution_policy == (
        "single_source_authoritative_highest_confidence"
    )
    assert by_field["canonical_name"].selected_claim_ids == [
        bootstrap_result.claims_used[0].claim_id
    ]
    assert by_field["canonical_name"].selected_evidence_ids == [
        bootstrap_result.evidence_used[0].evidence_id
    ]
    assert by_field["canonical_name"].metadata["source_key"] == "msu-official"
    assert by_field["canonical_name"].metadata["source_urls"] == [
        "https://example.edu/admissions"
    ]
    assert len(session.facts) == 4
    assert session.commit_count == 1


def test_resolved_fact_generation_is_idempotent_by_university_field_and_card_version() -> None:
    session = FakeResolvedFactSession()
    service = build_service(session)
    bootstrap_result = build_bootstrap_result()

    first = service.generate_for_bootstrap(bootstrap_result)
    second = service.generate_for_bootstrap(bootstrap_result)

    assert [fact.resolved_fact_id for fact in second.facts] == [
        fact.resolved_fact_id for fact in first.facts
    ]
    assert len(session.facts) == 4
    assert session.commit_count == 2
    assert set(session.facts) == {
        deterministic_resolved_fact_id(
            university_id=bootstrap_result.university.university_id,
            field_name=field_name,
            card_version=1,
        )
        for field_name in CANONICAL_FACT_FIELDS
    }
