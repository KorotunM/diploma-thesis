from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from apps.normalizer.app.cards import (
    UniversityCardProjectionRepository,
    UniversityCardProjectionService,
)
from apps.normalizer.app.facts import (
    PROGRAM_FIELD_PREFIX,
    RATING_FIELD_PREFIX,
    ResolvedFactBuildResult,
    ResolvedFactRecord,
)
from apps.normalizer.app.persistence import json_from_db
from apps.normalizer.app.resolution import (
    CANONICAL_FIELD_POLICY,
    RATING_FIELD_POLICY,
    SINGLE_SOURCE_AUTHORITATIVE_POLICY,
)
from apps.normalizer.app.search_docs import (
    UniversitySearchDocProjectionRepository,
    UniversitySearchDocProjectionService,
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
        self.search_docs: dict[tuple[UUID, int], dict[str, Any]] = {}
        self.commit_count = 0

    def execute(self, statement: str, params: dict[str, Any]) -> MappingResult:
        sql = " ".join(statement.split()).lower()
        if "insert into core.card_version" in sql:
            return MappingResult(self._upsert_card_version(params))
        if "insert into delivery.university_card" in sql:
            return MappingResult(self._upsert_projection(params))
        if "insert into delivery.university_search_doc" in sql:
            return MappingResult(self._upsert_search_doc(params))
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

    def _upsert_search_doc(self, params: dict[str, Any]) -> dict[str, Any]:
        key = (params["university_id"], params["card_version"])
        row = dict(params)
        self.search_docs[key] = row
        return row


def fact(
    *,
    university_id: UUID,
    field_name: str,
    value,
    score: float,
    card_version: int = 1,
    evidence_id: UUID | None = None,
    value_type: str = "str",
    resolution_policy: str = CANONICAL_FIELD_POLICY,
    metadata: dict[str, Any] | None = None,
) -> ResolvedFactRecord:
    selected_evidence_id = evidence_id or uuid4()
    return ResolvedFactRecord(
        resolved_fact_id=uuid4(),
        university_id=university_id,
        field_name=field_name,
        value=value,
        value_type=value_type,
        fact_score=score,
        resolution_policy=resolution_policy,
        selected_claim_ids=[uuid4()],
        selected_evidence_ids=[selected_evidence_id],
        card_version=card_version,
        resolved_at=datetime(2026, 4, 23, 10, 0, tzinfo=UTC),
        metadata=metadata
        or {
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
                field_name="contacts.emails",
                value=["admissions@example.edu"],
                score=0.89,
                value_type="list",
            ),
            fact(
                university_id=university_id,
                field_name="contacts.phones",
                value=["+7 495 000-00-00"],
                score=0.88,
                value_type="list",
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
            fact(
                university_id=university_id,
                field_name=f"{RATING_FIELD_PREFIX}qs-world:2026:world_overall:example-university",
                value={
                    "provider": "QS World University Rankings",
                    "year": 2026,
                    "metric": "world_overall",
                    "value": "151",
                },
                value_type="rating_item",
                resolution_policy=RATING_FIELD_POLICY,
                score=0.95,
                metadata={
                    "source_key": "qs-world-ranking",
                    "source_urls": [
                        "https://rankings.example.com/universities/example-university"
                    ],
                },
            ),
            fact(
                university_id=university_id,
                field_name=f"{PROGRAM_FIELD_PREFIX}science-faculty:05.03.01:0",
                value={
                    "faculty": "Faculty of Science",
                    "code": "05.03.01",
                    "name": "Geology",
                    "budget_places": 25,
                    "passing_score": 182,
                    "year": 2025,
                },
                value_type="program_item",
                score=0.97,
                metadata={
                    "source_key": "msu-official",
                    "source_urls": ["https://example.edu/programs"],
                },
            ),
        ],
    )


def build_service(session: FakeCardProjectionSession) -> UniversityCardProjectionService:
    return UniversityCardProjectionService(
        UniversityCardProjectionRepository(session=session, sql_text=lambda value: value),
        search_doc_service=UniversitySearchDocProjectionService(
            UniversitySearchDocProjectionRepository(
                session=session,
                sql_text=lambda value: value,
            )
        ),
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
    assert card.contacts.emails == ["admissions@example.edu"]
    assert card.contacts.phones == ["+7 495 000-00-00"]
    assert card.location.city == "Moscow"
    assert card.location.country == "RU"
    assert card.programs[0]["faculty"] == "Faculty of Science"
    assert card.programs[0]["code"] == "05.03.01"
    assert card.programs[0]["name"] == "Geology"
    assert card.programs[0]["budget_places"] == 25
    assert card.programs[0]["passing_score"] == 182
    assert card.programs[0]["year"] == 2025
    assert card.programs[0]["confidence"] == 0.97
    assert card.ratings[0].provider == "QS World University Rankings"
    assert card.ratings[0].year == 2026
    assert card.ratings[0].metric == "world_overall"
    assert card.ratings[0].value == "151"
    assert card.version.card_version == 1
    assert card.version.generated_at == session.generated_at
    assert card.sources[0].source_key == "msu-official"
    assert card.sources[0].source_url == "https://example.edu/admissions"
    assert len(card.sources[0].evidence_ids) == 6
    assert {source.source_url for source in card.sources} == {
        "https://example.edu/admissions",
        "https://example.edu/programs",
        "https://rankings.example.com/universities/example-university",
    }
    assert result.search_doc.university_id == fact_result.university.university_id
    assert result.search_doc.card_version == 1
    assert result.search_doc.canonical_name == "Example University"
    assert result.search_doc.canonical_name_normalized == "example university"
    assert result.search_doc.website_domain == "example.edu"
    assert result.search_doc.city_name == "Moscow"
    assert result.search_doc.country_code == "RU"
    assert result.search_doc.search_document["ratings"][0]["provider"] == (
        "QS World University Rankings"
    )
    stored_card = json_from_db(
        session.projections[(fact_result.university.university_id, 1)]["card_json"]
    )
    assert stored_card["canonical_name"]["value"] == "Example University"
    assert stored_card["programs"][0]["name"] == "Geology"
    stored_search_doc = json_from_db(
        session.search_docs[(fact_result.university.university_id, 1)]["search_document"]
    )
    assert stored_search_doc["canonical_name"] == "Example University"
    assert (
        session.search_docs[(fact_result.university.university_id, 1)]["search_text_source"]
        == "Example University example university example.edu Moscow RU "
        "QS World University Rankings 2026 world_overall 151"
    )
    assert session.commit_count == 1


def test_university_card_projection_is_idempotent_by_university_and_card_version() -> None:
    session = FakeCardProjectionSession()
    service = build_service(session)
    fact_result = build_fact_result()

    first = service.create_projection(fact_result)
    second = service.create_projection(fact_result)

    assert second.card_version == first.card_version
    assert second.projection.card == first.projection.card
    assert second.search_doc == first.search_doc
    assert len(session.card_versions) == 1
    assert len(session.projections) == 1
    assert len(session.search_docs) == 1
    assert session.commit_count == 2
