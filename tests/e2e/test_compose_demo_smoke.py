from __future__ import annotations

import httpx

from tests.e2e.compose_demo_smoke import (
    ComposeDemoSmokeConfig,
    ComposeDemoSmokeError,
    ComposeDemoSmokeRunner,
)

FRONTEND_HTML = """<!doctype html>
<html lang="ru">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>University Aggregator</title>
    <script src="/runtime-config.js"></script>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
"""


def test_compose_demo_smoke_runner_covers_frontend_to_provenance_flow() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == "http://localhost:5173/":
            return httpx.Response(
                200,
                text=FRONTEND_HTML,
                headers={"content-type": "text/html; charset=utf-8"},
            )
        if url == "http://localhost:5173/runtime-config.js":
            return httpx.Response(
                200,
                text="window.__APP_RUNTIME_CONFIG__ = window.__APP_RUNTIME_CONFIG__ ?? {};",
                headers={"content-type": "application/javascript"},
            )
        if url.startswith("http://localhost:5173/?query=Example%20University&university_id="):
            return httpx.Response(
                200,
                text=FRONTEND_HTML,
                headers={"content-type": "text/html; charset=utf-8"},
            )
        if url.endswith("/healthz"):
            service_name = url.split(":")[2].split("/")[0]
            service = {
                "8001": "scheduler",
                "8002": "parser",
                "8003": "normalizer",
                "8004": "backend",
            }[service_name]
            return httpx.Response(
                200,
                json={
                    "service": service,
                    "environment": "local",
                    "version": "0.1.0",
                    "dependencies": {
                        "postgres": "configured",
                        "rabbitmq": "configured",
                        "minio": "configured",
                    },
                },
            )
        if url == "http://localhost:8004/api/v1/search?query=Example%20University&page=1&page_size=10":
            return httpx.Response(
                200,
                json={
                    "query": "Example University",
                    "total": 1,
                    "page": 1,
                    "page_size": 10,
                    "has_more": False,
                    "filters": {"city": None, "country": None, "source_type": None},
                    "items": [
                        {
                            "university_id": "11111111-1111-4111-8111-111111111111",
                            "card_version": 1,
                            "canonical_name": "Example University",
                            "city": "Moscow",
                            "country_code": "RU",
                            "website": "https://www.example.edu/admissions",
                            "aliases": ["Example State University"],
                            "score": 0.887,
                            "match_signals": ["full_text", "trigram"],
                        }
                    ],
                },
            )
        if url == "http://localhost:8004/api/v1/universities/11111111-1111-4111-8111-111111111111":
            return httpx.Response(
                200,
                json={
                    "university_id": "11111111-1111-4111-8111-111111111111",
                    "canonical_name": {"value": "Example University", "confidence": 0.99},
                    "contacts": {"website": "https://www.example.edu/admissions"},
                    "location": {"city": "Moscow", "country": "RU"},
                    "institutional": {"type": None, "founded_year": None},
                    "version": {"card_version": 1, "generated_at": "2026-04-29T12:00:00Z"},
                    "sources": [],
                    "ratings": [],
                    "field_attribution": {
                        "canonical_name": {
                            "field_name": "canonical_name",
                            "source_key": "msu-official",
                            "source_trust_tier": "authoritative",
                            "source_urls": ["https://www.example.edu/admissions"],
                            "selected_claim_ids": [],
                            "selected_evidence_ids": [],
                            "resolution_policy": "tiered_authority_highest_confidence",
                            "resolution_strategy": "authoritative",
                            "rationale": "authoritative source selected",
                        }
                    },
                    "source_rationale": [
                        {
                            "source_key": "msu-official",
                            "trust_tier": "authoritative",
                            "selected_fields": ["canonical_name"],
                            "source_urls": ["https://www.example.edu/admissions"],
                            "rationale": "authoritative source selected",
                        }
                    ],
                },
            )
        if url == (
            "http://localhost:8004/api/v1/universities/"
            "11111111-1111-4111-8111-111111111111/provenance"
        ):
            return httpx.Response(
                200,
                json={
                    "university_id": "11111111-1111-4111-8111-111111111111",
                    "chain": [
                        "raw_artifact",
                        "parsed_document",
                        "claim",
                        "claim_evidence",
                        "resolved_fact",
                        "delivery_projection",
                    ],
                    "delivery_projection": {"card_version": 1},
                    "raw_artifacts": [
                        {"raw_artifact_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}
                    ],
                },
            )
        raise AssertionError(f"Unexpected request URL: {url}")

    runner = ComposeDemoSmokeRunner(
        client_factory=lambda **kwargs: httpx.Client(
            transport=httpx.MockTransport(handler),
            **kwargs,
        )
    )

    result = runner.run(ComposeDemoSmokeConfig())

    assert result.search_query == "Example University"
    assert result.university_id == "11111111-1111-4111-8111-111111111111"
    assert result.canonical_name == "Example University"
    assert result.frontend_deep_link_url.endswith(
        "?query=Example%20University&university_id=11111111-1111-4111-8111-111111111111"
    )
    assert [step.name for step in result.steps] == [
        "frontend shell",
        "frontend runtime config",
        "scheduler healthz",
        "parser healthz",
        "normalizer healthz",
        "backend healthz",
        "backend search",
        "backend card",
        "backend provenance",
        "frontend deep link",
    ]


def test_compose_demo_smoke_runner_fails_when_search_has_no_seeded_hits() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url in {
            "http://localhost:5173/",
            "http://localhost:5173/?query=Example%20University&university_id=missing",
        }:
            return httpx.Response(200, text=FRONTEND_HTML)
        if url == "http://localhost:5173/runtime-config.js":
            return httpx.Response(200, text="window.__APP_RUNTIME_CONFIG__ = {};")
        if url.endswith("/healthz"):
            return httpx.Response(
                200,
                json={
                    "service": {
                        "8001": "scheduler",
                        "8002": "parser",
                        "8003": "normalizer",
                        "8004": "backend",
                    }[url.split(":")[2].split("/")[0]],
                    "environment": "local",
                    "version": "0.1.0",
                    "dependencies": {"postgres": "configured"},
                },
            )
        if url == "http://localhost:8004/api/v1/search?query=Example%20University&page=1&page_size=10":
            return httpx.Response(
                200,
                json={
                    "query": "Example University",
                    "total": 0,
                    "page": 1,
                    "page_size": 10,
                    "has_more": False,
                    "filters": {"city": None, "country": None, "source_type": None},
                    "items": [],
                },
            )
        raise AssertionError(f"Unexpected request URL: {url}")

    runner = ComposeDemoSmokeRunner(
        client_factory=lambda **kwargs: httpx.Client(
            transport=httpx.MockTransport(handler),
            **kwargs,
        )
    )

    try:
        runner.run(ComposeDemoSmokeConfig())
    except ComposeDemoSmokeError as exc:
        assert "returned no items" in str(exc)
    else:
        raise AssertionError("ComposeDemoSmokeError was expected for empty search results.")
