from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from apps.backend.app.cards import (
    UniversityCardNotFoundError,
    UniversityCardReadRepository,
    UniversityCardReadService,
    UniversityCardResponse,
)
from apps.backend.app.dependencies import get_university_card_read_service
from apps.backend.app.main import app
from apps.normalizer.app.resolution import CANONICAL_FIELD_POLICY
from libs.domain.university import UniversityCard


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

    def one_or_none(self) -> dict[str, Any] | None:
        return self._row

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class FakeDeliverySession:
    def __init__(
        self,
        row: dict[str, Any] | None,
        resolved_facts: list[dict[str, Any]] | None = None,
    ) -> None:
        self.row = row
        self.resolved_facts = resolved_facts or []
        self.calls: list[dict[str, Any]] = []

    def execute(self, statement: str, params: dict[str, Any]) -> MappingResult:
        sql = " ".join(statement.split()).lower()
        self.calls.append({"statement": statement, "params": params})
        if "from delivery.university_card" in sql:
            assert "order by card_version desc" in sql
            assert "limit 1" in sql
            return MappingResult(self.row)
        if "from core.resolved_fact" in sql:
            assert "card_version = :card_version" in sql
            return MappingResult(rows=self.resolved_facts)
        raise AssertionError(f"Unexpected SQL: {statement}")


class FakeUniversityCardReadService:
    def __init__(self, card: UniversityCardResponse | None) -> None:
        self.card = card
        self.calls: list[UUID] = []

    def get_latest_card(self, university_id: UUID) -> UniversityCardResponse:
        self.calls.append(university_id)
        if self.card is None:
            raise UniversityCardNotFoundError(university_id)
        return self.card


def build_card(university_id: UUID | None = None) -> UniversityCard:
    return UniversityCard.model_validate(
        {
            **UniversityCard.sample().model_dump(mode="python"),
            "university_id": university_id or uuid4(),
            "contacts": {
                "website": "https://example.edu",
                "emails": ["admissions@example.edu"],
                "phones": ["+7 495 000-00-00"],
            },
            "programs": [
                {
                    "faculty": "Faculty of Science",
                    "code": "05.03.01",
                    "name": "Geology",
                    "budget_places": 25,
                    "passing_score": 182,
                    "year": 2025,
                    "confidence": 0.96,
                    "sources": [
                        {
                            "source_key": "msu-official",
                            "source_url": "https://example.edu/programs",
                            "evidence_ids": [str(uuid4())],
                        }
                    ],
                }
            ],
        }
    )


def build_row(card: UniversityCard) -> dict[str, Any]:
    return {
        "university_id": card.university_id,
        "card_version": card.version.card_version,
        "card_json": json.dumps(card.model_dump(mode="json"), sort_keys=True),
        "generated_at": datetime(2026, 4, 23, 12, 0, tzinfo=UTC),
    }


def build_fact_rows() -> list[dict[str, Any]]:
    claim_id = uuid4()
    evidence_id = uuid4()
    return [
        {
            "field_name": "canonical_name",
            "resolution_policy": CANONICAL_FIELD_POLICY,
            "fact_score": 0.99,
            "metadata": json.dumps(
                {
                    "source_key": "msu-official",
                    "source_trust_tier": "authoritative",
                    "source_urls": ["https://example.edu/about"],
                    "selected_claim_ids": [str(claim_id)],
                    "selected_evidence_ids": [str(evidence_id)],
                    "resolution_strategy": "prefer_higher_trust_tier",
                    "preferred_trust_tiers": ["authoritative", "trusted"],
                    "source_keys": ["msu-official", "msu-aggregator"],
                },
                sort_keys=True,
            ),
        },
        {
            "field_name": "contacts.website",
            "resolution_policy": CANONICAL_FIELD_POLICY,
            "fact_score": 0.97,
            "metadata": json.dumps(
                {
                    "source_key": "msu-official",
                    "source_trust_tier": "authoritative",
                    "source_urls": ["https://example.edu"],
                    "selected_claim_ids": [str(uuid4())],
                    "selected_evidence_ids": [str(uuid4())],
                    "resolution_strategy": "prefer_higher_trust_tier",
                    "preferred_trust_tiers": ["authoritative", "trusted"],
                    "source_keys": ["msu-official", "msu-aggregator"],
                },
                sort_keys=True,
            ),
        },
        {
            "field_name": "contacts.emails",
            "resolution_policy": CANONICAL_FIELD_POLICY,
            "fact_score": 0.96,
            "metadata": json.dumps(
                {
                    "source_key": "msu-official",
                    "source_trust_tier": "authoritative",
                    "source_urls": ["https://example.edu/admissions"],
                    "selected_claim_ids": [str(uuid4())],
                    "selected_evidence_ids": [str(uuid4())],
                    "resolution_strategy": "prefer_higher_trust_tier",
                    "preferred_trust_tiers": ["authoritative", "trusted"],
                    "source_keys": ["msu-official"],
                },
                sort_keys=True,
            ),
        },
        {
            "field_name": "contacts.phones",
            "resolution_policy": CANONICAL_FIELD_POLICY,
            "fact_score": 0.95,
            "metadata": json.dumps(
                {
                    "source_key": "msu-official",
                    "source_trust_tier": "authoritative",
                    "source_urls": ["https://example.edu/admissions"],
                    "selected_claim_ids": [str(uuid4())],
                    "selected_evidence_ids": [str(uuid4())],
                    "resolution_strategy": "prefer_higher_trust_tier",
                    "preferred_trust_tiers": ["authoritative", "trusted"],
                    "source_keys": ["msu-official"],
                },
                sort_keys=True,
            ),
        },
        {
            "field_name": "programs.05.03.01:2025:geology",
            "resolution_policy": CANONICAL_FIELD_POLICY,
            "fact_score": 0.94,
            "metadata": json.dumps(
                {
                    "source_key": "msu-official",
                    "source_trust_tier": "authoritative",
                    "source_urls": ["https://example.edu/programs"],
                    "selected_claim_ids": [str(uuid4())],
                    "selected_evidence_ids": [str(uuid4())],
                    "resolution_strategy": "prefer_higher_trust_tier",
                    "preferred_trust_tiers": ["authoritative", "trusted"],
                    "source_keys": ["msu-official"],
                },
                sort_keys=True,
            ),
        },
    ]


def build_response(card: UniversityCard) -> UniversityCardResponse:
    return UniversityCardResponse.model_validate(
        {
            **card.model_dump(mode="python"),
            "field_attribution": {
                "canonical_name": {
                    "field_name": "canonical_name",
                    "source_key": "msu-official",
                    "source_trust_tier": "authoritative",
                    "source_urls": ["https://example.edu/about"],
                    "selected_claim_ids": [uuid4()],
                    "selected_evidence_ids": [uuid4()],
                    "resolution_policy": CANONICAL_FIELD_POLICY,
                    "resolution_strategy": "prefer_higher_trust_tier",
                    "rationale": (
                        "tiered_authority_highest_confidence; "
                        "strategy=prefer_higher_trust_tier; "
                        "winner=msu-official; tier=authoritative"
                    ),
                }
            },
            "admission": {
                "contacts": {
                    "website": "https://example.edu",
                    "emails": ["admissions@example.edu"],
                    "phones": ["+7 495 000-00-00"],
                    "field_attribution": {
                        "contacts.emails": {
                            "field_name": "contacts.emails",
                            "source_key": "msu-official",
                            "source_trust_tier": "authoritative",
                            "source_urls": ["https://example.edu/admissions"],
                            "selected_claim_ids": [uuid4()],
                            "selected_evidence_ids": [uuid4()],
                            "resolution_policy": CANONICAL_FIELD_POLICY,
                            "resolution_strategy": "prefer_higher_trust_tier",
                            "rationale": "tiered_authority_highest_confidence",
                        }
                    },
                },
                "programs": [
                    {
                        "field_name": "programs.05.03.01:2025:geology",
                        "faculty": "Faculty of Science",
                        "code": "05.03.01",
                        "name": "Geology",
                        "budget_places": 25,
                        "passing_score": 182,
                        "year": 2025,
                        "confidence": 0.94,
                        "sources": [],
                        "field_attribution": {
                            "field_name": "programs.05.03.01:2025:geology",
                            "source_key": "msu-official",
                            "source_trust_tier": "authoritative",
                            "source_urls": ["https://example.edu/programs"],
                            "selected_claim_ids": [uuid4()],
                            "selected_evidence_ids": [uuid4()],
                            "resolution_policy": CANONICAL_FIELD_POLICY,
                            "resolution_strategy": "prefer_higher_trust_tier",
                            "rationale": "tiered_authority_highest_confidence",
                        },
                    }
                ],
            },
            "source_rationale": [
                {
                    "source_key": "msu-official",
                    "trust_tier": "authoritative",
                    "selected_fields": ["canonical_name"],
                    "source_urls": ["https://example.edu/about"],
                    "rationale": "msu-official (authoritative) selected for fields canonical_name",
                }
            ],
        }
    )


def test_university_card_repository_reads_latest_delivery_projection() -> None:
    card = build_card()
    session = FakeDeliverySession(build_row(card), build_fact_rows())
    repository = UniversityCardReadRepository(session=session, sql_text=lambda value: value)

    record = repository.get_latest_by_university_id(card.university_id)
    facts = repository.list_resolved_facts(
        university_id=card.university_id,
        card_version=card.version.card_version,
    )

    assert record is not None
    assert record.university_id == card.university_id
    assert record.card_version == card.version.card_version
    assert record.card == card
    assert session.calls[0]["params"] == {"university_id": card.university_id}
    assert facts[0].field_name == "canonical_name"
    assert facts[0].selected_claim_ids
    assert facts[0].metadata["source_key"] == "msu-official"


def test_university_card_read_service_raises_when_projection_missing() -> None:
    repository = UniversityCardReadRepository(
        session=FakeDeliverySession(None),
        sql_text=lambda value: value,
    )
    service = UniversityCardReadService(repository)

    with pytest.raises(UniversityCardNotFoundError):
        service.get_latest_card(uuid4())


def test_university_card_read_service_adds_field_attribution_and_source_rationale() -> None:
    card = build_card()
    repository = UniversityCardReadRepository(
        session=FakeDeliverySession(build_row(card), build_fact_rows()),
        sql_text=lambda value: value,
    )
    service = UniversityCardReadService(repository)

    response = service.get_latest_card(card.university_id)

    assert response.university_id == card.university_id
    assert response.canonical_name.value == card.canonical_name.value
    assert response.field_attribution["canonical_name"].source_key == "msu-official"
    assert response.field_attribution["canonical_name"].source_trust_tier == (
        "authoritative"
    )
    assert "winner=msu-official" in response.field_attribution["canonical_name"].rationale
    assert response.admission.contacts.emails == ["admissions@example.edu"]
    assert response.admission.contacts.field_attribution["contacts.emails"].source_key == (
        "msu-official"
    )
    assert response.admission.programs[0].field_name == "programs.05.03.01:2025:geology"
    assert response.admission.programs[0].name == "Geology"
    assert response.admission.programs[0].field_attribution is not None
    assert response.admission.programs[0].field_attribution.source_urls == [
        "https://example.edu/programs"
    ]
    assert response.source_rationale[0].source_key == "msu-official"
    assert response.source_rationale[0].selected_fields == [
        "canonical_name",
        "contacts.emails",
        "contacts.phones",
        "contacts.website",
        "programs.05.03.01:2025:geology",
    ]


def test_university_card_endpoint_serves_delivery_projection() -> None:
    card = build_card()
    service = FakeUniversityCardReadService(build_response(card))
    app.dependency_overrides[get_university_card_read_service] = lambda: service
    try:
        response = TestClient(app).get(f"/api/v1/universities/{card.university_id}")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["university_id"] == str(card.university_id)
    assert body["canonical_name"]["value"] == card.canonical_name.value
    assert body["admission"]["contacts"]["emails"] == ["admissions@example.edu"]
    assert body["admission"]["programs"][0]["name"] == "Geology"
    assert body["field_attribution"]["canonical_name"]["source_key"] == "msu-official"
    assert body["source_rationale"][0]["source_key"] == "msu-official"
    assert service.calls == [card.university_id]


def test_university_card_endpoint_returns_404_when_projection_missing() -> None:
    university_id = uuid4()
    service = FakeUniversityCardReadService(None)
    app.dependency_overrides[get_university_card_read_service] = lambda: service
    try:
        response = TestClient(app).get(f"/api/v1/universities/{university_id}")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json()["detail"] == f"University card {university_id} was not found."
