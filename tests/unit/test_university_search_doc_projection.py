from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from apps.normalizer.app.persistence import json_from_db
from apps.normalizer.app.search_docs import (
    UniversitySearchDocProjectionRepository,
    UniversitySearchDocProjectionService,
)
from libs.domain.university.models import (
    CardVersionInfo,
    ConfidenceValue,
    ContactsInfo,
    FieldAttribution,
    InstitutionalInfo,
    LocationInfo,
    RatingItem,
    ReviewSummary,
    UniversityCard,
)


class MappingResult:
    def __init__(self, row: dict[str, Any]) -> None:
        self._row = row

    def mappings(self) -> MappingResult:
        return self

    def one(self) -> dict[str, Any]:
        return self._row


class FakeSearchDocSession:
    def __init__(self) -> None:
        self.rows: dict[tuple[UUID, int], dict[str, Any]] = {}

    def execute(self, statement: str, params: dict[str, Any]) -> MappingResult:
        sql = " ".join(statement.split()).lower()
        if "insert into delivery.university_search_doc" not in sql:
            raise AssertionError(f"Unexpected SQL statement: {statement}")
        key = (params["university_id"], params["card_version"])
        row = dict(params)
        self.rows[key] = row
        return MappingResult(row)


def build_card() -> UniversityCard:
    generated_at = datetime(2026, 4, 28, 10, 0, tzinfo=UTC)
    return UniversityCard(
        university_id=uuid4(),
        canonical_name=ConfidenceValue(
            value=" Example University ",
            confidence=0.98,
            sources=[
                FieldAttribution(
                    source_key="msu-official",
                    source_url="https://www.example.edu/admissions",
                )
            ],
        ),
        aliases=["ESU", " Example U ", "esu"],
        location=LocationInfo(country="RU", city="Moscow"),
        contacts=ContactsInfo(website="https://www.example.edu/admissions"),
        institutional=InstitutionalInfo.model_validate({"type": None, "founded_year": None}),
        programs=[],
        tuition=[],
        ratings=[
            RatingItem(
                provider="QS World University Rankings",
                year=2026,
                metric="world_overall",
                value="151",
            )
        ],
        dormitory={},
        reviews=ReviewSummary(),
        sources=[
            FieldAttribution(
                source_key="msu-official",
                source_url="https://www.example.edu/admissions",
                evidence_ids=[uuid4()],
            )
        ],
        version=CardVersionInfo(card_version=2, generated_at=generated_at),
    )


def test_search_doc_projection_normalizes_card_payload() -> None:
    session = FakeSearchDocSession()
    repository = UniversitySearchDocProjectionRepository(
        session=session,
        sql_text=lambda value: value,
    )
    service = UniversitySearchDocProjectionService(repository)
    card = build_card()

    result = service.refresh_for_card(card)

    assert result.university_id == card.university_id
    assert result.card_version == 2
    assert result.canonical_name == "Example University"
    assert result.canonical_name_normalized == "example university"
    assert result.website_url == "https://www.example.edu/admissions"
    assert result.website_domain == "example.edu"
    assert result.country_code == "RU"
    assert result.city_name == "Moscow"
    assert result.aliases == ["ESU", "Example U"]
    assert result.search_document["ratings"][0]["provider"] == "QS World University Rankings"
    assert result.metadata["projection_kind"] == "delivery.university_search_doc"
    stored = session.rows[(card.university_id, 2)]
    assert stored["search_text_source"] == (
        "Example University example university example.edu Moscow RU ESU Example U "
        "QS World University Rankings 2026 world_overall 151"
    )
    assert json_from_db(stored["search_document"])["website_domain"] == "example.edu"
