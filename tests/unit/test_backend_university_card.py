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
)
from apps.backend.app.dependencies import get_university_card_read_service
from apps.backend.app.main import app
from libs.domain.university import UniversityCard


class MappingResult:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row

    def mappings(self) -> MappingResult:
        return self

    def one_or_none(self) -> dict[str, Any] | None:
        return self._row


class FakeDeliverySession:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self.row = row
        self.calls: list[dict[str, Any]] = []

    def execute(self, statement: str, params: dict[str, Any]) -> MappingResult:
        sql = " ".join(statement.split()).lower()
        assert "from delivery.university_card" in sql
        assert "order by card_version desc" in sql
        assert "limit 1" in sql
        self.calls.append({"statement": statement, "params": params})
        return MappingResult(self.row)


class FakeUniversityCardReadService:
    def __init__(self, card: UniversityCard | None) -> None:
        self.card = card
        self.calls: list[UUID] = []

    def get_latest_card(self, university_id: UUID) -> UniversityCard:
        self.calls.append(university_id)
        if self.card is None:
            raise UniversityCardNotFoundError(university_id)
        return self.card


def build_card(university_id: UUID | None = None) -> UniversityCard:
    return UniversityCard.sample().model_copy(
        update={
            "university_id": university_id or uuid4(),
        }
    )


def build_row(card: UniversityCard) -> dict[str, Any]:
    return {
        "university_id": card.university_id,
        "card_version": card.version.card_version,
        "card_json": json.dumps(card.model_dump(mode="json"), sort_keys=True),
        "generated_at": datetime(2026, 4, 23, 12, 0, tzinfo=UTC),
    }


def test_university_card_repository_reads_latest_delivery_projection() -> None:
    card = build_card()
    session = FakeDeliverySession(build_row(card))
    repository = UniversityCardReadRepository(session=session, sql_text=lambda value: value)

    record = repository.get_latest_by_university_id(card.university_id)

    assert record is not None
    assert record.university_id == card.university_id
    assert record.card_version == card.version.card_version
    assert record.card == card
    assert session.calls[0]["params"] == {"university_id": card.university_id}


def test_university_card_read_service_raises_when_projection_missing() -> None:
    repository = UniversityCardReadRepository(
        session=FakeDeliverySession(None),
        sql_text=lambda value: value,
    )
    service = UniversityCardReadService(repository)

    with pytest.raises(UniversityCardNotFoundError):
        service.get_latest_card(uuid4())


def test_university_card_endpoint_serves_delivery_projection() -> None:
    card = build_card()
    service = FakeUniversityCardReadService(card)
    app.dependency_overrides[get_university_card_read_service] = lambda: service
    try:
        response = TestClient(app).get(f"/api/v1/universities/{card.university_id}")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["university_id"] == str(card.university_id)
    assert body["canonical_name"]["value"] == card.canonical_name.value
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
