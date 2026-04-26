from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from apps.normalizer.app.cards import (
    UniversityCardProjectionRepository,
    UniversityCardProjectionService,
)
from apps.normalizer.app.facts import ResolvedFactBuildResult, ResolvedFactRecord
from apps.normalizer.app.persistence import json_from_db
from apps.normalizer.app.resolution import (
    CANONICAL_FIELD_POLICY,
    SINGLE_SOURCE_AUTHORITATIVE_POLICY,
)
from apps.normalizer.app.universities import UniversityRecord, deterministic_university_id
from libs.domain.university import UniversityCard


class MappingResult:
    def __init__(self, row: dict[str, Any]) -> None:
        self._row = row

    def mappings(self) -> MappingResult:
        return self

    def one(self) -> dict[str, Any]:
        return self._row


class FakeCardProjectionSession:
    def __init__(self) -> None:
        self.generated_at = datetime(2026, 4, 23, 11, 0, tzinfo=UTC)
        self.card_versions: dict[tuple[UUID, int], dict[str, Any]] = {}
        self.projections: dict[tuple[UUID, int], dict[str, Any]] = {}
        self.commit_count = 0

    def execute(self, statement: str, params: dict[str, Any]) -> MappingResult:
        sql = " ".join(statement.split()).lower()
        if "insert into core.card_version" in sql:
            return MappingResult(self._upsert_card_version(params))
        if "insert into delivery.university_card" in sql:
            return MappingResult(self._upsert_projection(params))
        raise AssertionError(f"Unexpected SQL statement: {statement}")

    def commit(self) -> None:
        self.commit_count += 1

    def _upsert_card_version(self, params: dict[str, Any]) -> dict[str, Any]:
        key = (params["university_id"], params["card_version"])
        existing = self.card_versions.get(key)
        row = {
            **params,
            "generated_at": existing["generated_at"] if existing else self.generated_at,
        }
        self.card_versions[key] = row
        return row

    def _upsert_projection(self, params: dict[str, Any]) -> dict[str, Any]:
        key = (params["university_id"], params["card_version"])
        row = dict(params)
        self.projections[key] = row
        return row


def fact(
    *,
    university_id: UUID,
    field_name: str,
    value,
    score: float,
    card_version: int = 1,
    evidence_id: UUID | None = None,
) -> ResolvedFactRecord:
    selected_evidence_id = evidence_id or uuid4()
    return ResolvedFactRecord(
        resolved_fact_id=uuid4(),
        university_id=university_id,
        field_name=field_name,
        value=value,
        value_type="str",
        fact_score=score,
        resolution_policy=CANONICAL_FIELD_POLICY,
        selected_claim_ids=[uuid4()],
        selected_evidence_ids=[selected_evidence_id],
        card_version=card_version,
        resolved_at=datetime(2026, 4, 23, 10, 0, tzinfo=UTC),
        metadata={
            "source_key": "msu-official",
            "source_urls": ["https://example.edu/admissions"],
        },
    )


def build_fact_result() -> ResolvedFactBuildResult:
    university_id = deterministic_university_id("msu-official")
    university = UniversityRecord(
        university_id=university_id,
        canonical_name="Example University",
        canonical_domain="example.edu",
        country_code="RU",
        city_name="Moscow",
        created_at=datetime(2026, 4, 23, 9, 5, tzinfo=UTC),
        metadata={"bootstrap_policy": SINGLE_SOURCE_AUTHORITATIVE_POLICY},
    )
    return ResolvedFactBuildResult(
        university=university,
        facts=[
            fact(
                university_id=university_id,
                field_name="canonical_name",
                value="Example University",
                score=0.98,
            ),
            fact(
                university_id=university_id,
                field_name="contacts.website",
                value="https://example.edu",
                score=0.9,
            ),
            fact(
                university_id=university_id,
                field_name="location.city",
                value="Moscow",
                score=0.88,
            ),
            fact(
                university_id=university_id,
                field_name="location.country_code",
                value="RU",
                score=0.8,
            ),
        ],
    )


def build_service(session: FakeCardProjectionSession) -> UniversityCardProjectionService:
    return UniversityCardProjectionService(
        UniversityCardProjectionRepository(session=session, sql_text=lambda value: value)
    )


def test_university_card_projection_persists_card_version_and_delivery_card() -> None:
    session = FakeCardProjectionSession()
    service = build_service(session)
    fact_result = build_fact_result()

    result = service.create_projection(fact_result)

    assert result.card_version.university_id == fact_result.university.university_id
    assert result.card_version.card_version == 1
    assert result.card_version.normalizer_version == "normalizer.0.1.0"
    assert result.projection.university_id == fact_result.university.university_id
    assert result.projection.card_version == 1
    card = result.projection.card
    assert isinstance(card, UniversityCard)
    assert card.university_id == fact_result.university.university_id
    assert card.canonical_name.value == "Example University"
    assert card.canonical_name.confidence == 0.98
    assert card.contacts.website == "https://example.edu"
    assert card.location.city == "Moscow"
    assert card.location.country == "RU"
    assert card.version.card_version == 1
    assert card.version.generated_at == session.generated_at
    assert card.sources[0].source_key == "msu-official"
    assert card.sources[0].source_url == "https://example.edu/admissions"
    assert len(card.sources[0].evidence_ids) == 4
    stored_card = json_from_db(
        session.projections[(fact_result.university.university_id, 1)]["card_json"]
    )
    assert stored_card["canonical_name"]["value"] == "Example University"
    assert session.commit_count == 1


def test_university_card_projection_is_idempotent_by_university_and_card_version() -> None:
    session = FakeCardProjectionSession()
    service = build_service(session)
    fact_result = build_fact_result()

    first = service.create_projection(fact_result)
    second = service.create_projection(fact_result)

    assert second.card_version == first.card_version
    assert second.projection.card == first.projection.card
    assert len(session.card_versions) == 1
    assert len(session.projections) == 1
    assert session.commit_count == 2
