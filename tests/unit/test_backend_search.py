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
        if params["query"] is not None:
            assert "ts_rank_cd" in sql
            assert "similarity(" in sql
        assert "order by" in sql
        return MappingResult(rows=self.rows)


class FakeUniversitySearchService:
    def __init__(self, response: UniversitySearchResponse) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def search(
        self,
        query: str,
        *,
        city: str | None = None,
        country: str | None = None,
        source_type: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> UniversitySearchResponse:
        self.calls.append(
            {
                "query": query,
                "city": city,
                "country": country,
                "source_type": source_type,
                "page": page,
                "page_size": page_size,
            }
        )
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
            "total_count": 7,
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
            "total_count": 7,
        },
    ]


def test_university_search_repository_reads_ranked_hits_from_projection() -> None:
    session = FakeSearchSession(build_search_rows())
    repository = UniversitySearchRepository(session=session, sql_text=lambda value: value)

    hits = repository.search(
        query="example university",
        normalized_query="example university",
        city="Moscow",
        country_code="RU",
        source_type="official_site",
        limit=10,
        offset=20,
    )

    assert len(hits) == 2
    assert hits[0].canonical_name == "Example University"
    assert hits[0].text_rank == 0.92
    assert hits[0].trigram_score == 0.81
    assert hits[0].combined_score == 0.887
    assert hits[0].total_count == 7
    assert hits[0].aliases == ["ESU", "Example U"]
    assert session.calls[0]["params"] == {
        "query": "example university",
        "normalized_query": "example university",
        "city": "Moscow",
        "country_code": "RU",
        "source_type": "official_site",
        "limit": 10,
        "offset": 20,
    }


def test_university_search_service_builds_api_response_with_filters_and_paging() -> None:
    repository = UniversitySearchRepository(
        session=FakeSearchSession(build_search_rows()),
        sql_text=lambda value: value,
    )
    service = UniversitySearchService(repository)

    response = service.search(
        "  Example   University  ",
        city="  Moscow ",
        country="ru",
        source_type="Official_Site",
        page=3,
        page_size=500,
    )

    assert response.query == "Example University"
    assert response.total == 7
    assert response.page == 3
    assert response.page_size == 50
    assert response.has_more is False
    assert response.filters.city == "Moscow"
    assert response.filters.country == "RU"
    assert response.filters.source_type == "official_site"
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

    assert response == UniversitySearchResponse(
        query="",
        total=0,
        page=1,
        page_size=20,
        has_more=False,
        filters={},
        items=[],
    )


def test_university_search_service_supports_filter_only_browse() -> None:
    repository = UniversitySearchRepository(
        session=FakeSearchSession(build_search_rows()),
        sql_text=lambda value: value,
    )
    service = UniversitySearchService(repository)

    response = service.search("", country="ru", page=1, page_size=10)

    assert response.query == ""
    assert response.filters.country == "RU"
    assert response.total == 7
    assert response.page == 1
    assert response.page_size == 10


def test_search_endpoint_serves_live_search_response() -> None:
    row = build_search_rows()[0]
    response_model = UniversitySearchResponse.model_validate(
        {
            "query": "Example University",
            "total": 1,
            "page": 2,
            "page_size": 5,
            "has_more": True,
            "filters": {
                "city": "Moscow",
                "country": "RU",
                "source_type": "official_site",
            },
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
            params={
                "query": "Example University",
                "city": "Moscow",
                "country": "RU",
                "source_type": "official_site",
                "page": 2,
                "page_size": 5,
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["query"] == "Example University"
    assert body["total"] == 1
    assert body["page"] == 2
    assert body["page_size"] == 5
    assert body["has_more"] is True
    assert body["filters"] == {
        "city": "Moscow",
        "country": "RU",
        "source_type": "official_site",
    }
    assert body["items"][0]["university_id"] == str(row["university_id"])
    assert body["items"][0]["canonical_name"] == "Example University"
    assert body["items"][0]["match_signals"] == ["full_text", "trigram"]
    assert service.calls == [
        {
            "query": "Example University",
            "city": "Moscow",
            "country": "RU",
            "source_type": "official_site",
            "page": 2,
            "page_size": 5,
        }
    ]
