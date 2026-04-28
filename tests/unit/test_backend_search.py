from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fastapi.testclient import TestClient

from apps.backend.app.dependencies import get_university_search_service
from apps.backend.app.main import app
from apps.backend.app.search import (
    UniversitySearchRepository,
    UniversitySearchResponse,
    UniversitySearchService,
)


class MappingResult:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self._rows = rows or []

    def mappings(self) -> MappingResult:
        return self

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class FakeSearchSession:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows or []
        self.calls: list[dict[str, Any]] = []

    def execute(self, statement: str, params: dict[str, Any]) -> MappingResult:
        sql = " ".join(statement.split()).lower()
        self.calls.append({"statement": statement, "params": params})
        assert "from delivery.university_search_doc" in sql
        assert "ts_rank_cd" in sql
        assert "similarity(" in sql
        assert "order by" in sql
        return MappingResult(rows=self.rows)


class FakeUniversitySearchService:
    def __init__(self, response: UniversitySearchResponse) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def search(self, query: str, *, limit: int = 20) -> UniversitySearchResponse:
        self.calls.append({"query": query, "limit": limit})
        return self.response


def build_search_rows() -> list[dict[str, Any]]:
    return [
        {
            "university_id": uuid4(),
            "card_version": 2,
            "canonical_name": "Example University",
            "website_url": "https://example.edu",
            "website_domain": "example.edu",
            "country_code": "RU",
            "city_name": "Moscow",
            "aliases": ["ESU", "Example U"],
            "metadata": json.dumps({"projection_kind": "delivery.university_search_doc"}),
            "generated_at": datetime(2026, 4, 28, 11, 0, tzinfo=UTC),
            "text_rank": 0.92,
            "trigram_score": 0.81,
            "combined_score": 0.887,
        },
        {
            "university_id": uuid4(),
            "card_version": 1,
            "canonical_name": "Example Institute",
            "website_url": "https://example-institute.edu",
            "website_domain": "example-institute.edu",
            "country_code": "RU",
            "city_name": "Kazan",
            "aliases": [],
            "metadata": json.dumps({"projection_kind": "delivery.university_search_doc"}),
            "generated_at": datetime(2026, 4, 28, 11, 0, tzinfo=UTC),
            "text_rank": 0.0,
            "trigram_score": 0.79,
            "combined_score": 0.237,
        },
    ]


def test_university_search_repository_reads_ranked_hits_from_projection() -> None:
    session = FakeSearchSession(build_search_rows())
    repository = UniversitySearchRepository(session=session, sql_text=lambda value: value)

    hits = repository.search(
        query="example university",
        normalized_query="example university",
        limit=10,
    )

    assert len(hits) == 2
    assert hits[0].canonical_name == "Example University"
    assert hits[0].text_rank == 0.92
    assert hits[0].trigram_score == 0.81
    assert hits[0].combined_score == 0.887
    assert hits[0].aliases == ["ESU", "Example U"]
    assert session.calls[0]["params"] == {
        "query": "example university",
        "normalized_query": "example university",
        "limit": 10,
    }


def test_university_search_service_builds_api_response_and_caps_limit() -> None:
    repository = UniversitySearchRepository(
        session=FakeSearchSession(build_search_rows()),
        sql_text=lambda value: value,
    )
    service = UniversitySearchService(repository)

    response = service.search("  Example   University  ", limit=500)

    assert response.query == "Example University"
    assert response.total == 2
    assert response.items[0].canonical_name == "Example University"
    assert response.items[0].website == "https://example.edu"
    assert response.items[0].aliases == ["ESU", "Example U"]
    assert response.items[0].score == 0.887
    assert response.items[0].match_signals == ["full_text", "trigram"]
    assert response.items[1].match_signals == ["trigram"]


def test_university_search_service_returns_empty_response_for_blank_query() -> None:
    repository = UniversitySearchRepository(
        session=FakeSearchSession(build_search_rows()),
        sql_text=lambda value: value,
    )
    service = UniversitySearchService(repository)

    response = service.search("   ")

    assert response == UniversitySearchResponse(query="", total=0, items=[])


def test_search_endpoint_serves_live_search_response() -> None:
    row = build_search_rows()[0]
    response_model = UniversitySearchResponse.model_validate(
        {
            "query": "Example University",
            "total": 1,
            "items": [
                {
                    "university_id": row["university_id"],
                    "card_version": row["card_version"],
                    "canonical_name": row["canonical_name"],
                    "city": row["city_name"],
                    "country_code": row["country_code"],
                    "website": row["website_url"],
                    "aliases": row["aliases"],
                    "score": row["combined_score"],
                    "match_signals": ["full_text", "trigram"],
                }
            ],
        }
    )
    service = FakeUniversitySearchService(response_model)
    app.dependency_overrides[get_university_search_service] = lambda: service
    try:
        response = TestClient(app).get(
            "/api/v1/search",
            params={"query": "Example University", "limit": 5},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["query"] == "Example University"
    assert body["total"] == 1
    assert body["items"][0]["university_id"] == str(row["university_id"])
    assert body["items"][0]["canonical_name"] == "Example University"
    assert body["items"][0]["match_signals"] == ["full_text", "trigram"]
    assert service.calls == [{"query": "Example University", "limit": 5}]
