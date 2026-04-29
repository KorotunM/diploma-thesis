from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from apps.normalizer.app.cards import (
    UniversityCardProjectionRepository,
    UniversityCardProjectionService,
)
from apps.normalizer.app.claims import ClaimBuildRepository, ClaimBuildService
from apps.normalizer.app.facts import (
    ResolvedFactGenerationService,
    ResolvedFactRepository,
)
from apps.normalizer.app.persistence import json_from_db, json_to_db
from apps.normalizer.app.resolution import FieldResolutionPolicyMatrix
from apps.normalizer.app.search_docs import (
    UniversitySearchDocProjectionRepository,
    UniversitySearchDocProjectionService,
)
from apps.normalizer.app.universities import (
    UniversityBootstrapRepository,
    UniversityBootstrapService,
    deterministic_university_id,
)
from apps.parser.adapters.aggregators import AggregatorAdapter
from apps.parser.adapters.official_sites import OfficialSiteAdapter
from apps.parser.adapters.rankings import RankingAdapter
from apps.parser.app.parsed_documents import (
    ParsedDocumentPersistenceService,
    ParsedDocumentRepository,
)
from libs.contracts.events import NormalizeRequestPayload
from libs.source_sdk import FetchContext, FetchedArtifact
from scripts.mvp_fixtures import FixtureBundleEntry, FixtureBundleManifest

MANIFEST_PATH = (
    Path(__file__).resolve().parents[1] / "fixtures" / "mvp_bundle" / "manifest.json"
)

EXPECTED_FIELDS_BY_FIXTURE_ID = {
    "official": {
        "count": 4,
        "canonical_name": "Example University",
        "contacts.website": "https://example.edu",
        "contacts.emails": ["admissions@example.edu"],
        "location.city": "Moscow",
    },
    "aggregator": {
        "count": 7,
        "canonical_name": "Example University",
        "aliases": ["EU", "Example U"],
        "contacts.website": "https://example.edu",
        "contacts.emails": ["admissions@example.edu"],
        "contacts.phones": ["+7 495 123-45-67"],
        "location.city": "Moscow",
        "location.country_code": "RU",
    },
    "ranking": {
        "count": 7,
        "canonical_name": "Example University",
        "contacts.website": "https://example.edu",
        "location.country_code": "RU",
        "ratings.provider": "QS World University Rankings",
        "ratings.year": 2026,
        "ratings.metric": "world_overall",
        "ratings.value": "151",
    },
}


class FakeFetcher:
    def __init__(self, artifact: FetchedArtifact) -> None:
        self._artifact = artifact

    async def fetch(self, context: FetchContext) -> FetchedArtifact:
        return self._artifact.model_copy(
            update={
                "crawl_run_id": context.crawl_run_id,
                "source_key": context.source_key,
            }
        )


class MappingResult:
    def __init__(
        self,
        *,
        row: dict[str, Any] | None = None,
        rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self._row = row
        self._rows = rows or []

    def mappings(self) -> MappingResult:
        return self

    def one(self) -> dict[str, Any]:
        assert self._row is not None
        return self._row

    def one_or_none(self) -> dict[str, Any] | None:
        return self._row

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class InMemoryRegressionSession:
    def __init__(self) -> None:
        self.now = datetime(2026, 4, 29, 12, 0, tzinfo=UTC)
        self.commit_count = 0
        self.sources: dict[str, dict[str, Any]] = {
            "msu-official": {
                "source_id": deterministic_university_id("source:msu-official"),
                "source_key": "msu-official",
                "source_type": "official_site",
                "trust_tier": "authoritative",
                "is_active": True,
                "metadata": json_to_db({"bundle": "mvp"}),
            },
            "study-aggregator": {
                "source_id": deterministic_university_id("source:study-aggregator"),
                "source_key": "study-aggregator",
                "source_type": "aggregator",
                "trust_tier": "trusted",
                "is_active": True,
                "metadata": json_to_db({"bundle": "mvp"}),
            },
            "qs-world-ranking": {
                "source_id": deterministic_university_id("source:qs-world-ranking"),
                "source_key": "qs-world-ranking",
                "source_type": "ranking",
                "trust_tier": "trusted",
                "is_active": True,
                "metadata": json_to_db({"bundle": "mvp"}),
            },
        }
        self.raw_artifacts: dict[UUID, dict[str, Any]] = {}
        self.parsed_documents: dict[tuple[UUID, str], dict[str, Any]] = {}
        self.fragments: dict[UUID, dict[str, Any]] = {}
        self.claims: dict[UUID, dict[str, Any]] = {}
        self.evidence: dict[UUID, dict[str, Any]] = {}
        self.universities: dict[UUID, dict[str, Any]] = {}
        self.resolved_facts: dict[UUID, dict[str, Any]] = {}
        self.card_versions: dict[tuple[UUID, int], dict[str, Any]] = {}
        self.delivery_cards: dict[tuple[UUID, int], dict[str, Any]] = {}
        self.search_docs: dict[tuple[UUID, int], dict[str, Any]] = {}

    def execute(self, statement: str, params: dict[str, Any]) -> MappingResult:
        sql = " ".join(statement.split()).lower()
        if "insert into parsing.parsed_document" in sql:
            return MappingResult(row=self._upsert_parsed_document(params))
        if "insert into parsing.extracted_fragment" in sql:
            return MappingResult(row=self._upsert_fragment(params))
        if (
            "from parsing.parsed_document" in sql
            and "where parsed_document_id = :parsed_document_id" in sql
        ):
            return MappingResult(row=self._get_parsed_document(params["parsed_document_id"]))
        if (
            "from parsing.extracted_fragment as ef" in sql
            and "join ingestion.raw_artifact as ra" in sql
        ):
            return MappingResult(rows=self._list_fragments_for_claim_build(params))
        if "insert into normalize.claim_evidence" in sql:
            return MappingResult(row=self._upsert_evidence(params))
        if "insert into normalize.claim " in sql:
            return MappingResult(row=self._upsert_claim(params))
        if "from ingestion.source" in sql:
            return MappingResult(row=self.sources.get(params["source_key"]))
        if "where canonical_domain = :canonical_domain" in sql and "from core.university" in sql:
            return MappingResult(row=self._find_university_by_domain(params["canonical_domain"]))
        if "where canonical_name = :canonical_name" in sql and "from core.university" in sql:
            return MappingResult(row=self._find_university_by_name(params["canonical_name"]))
        if "similarity(canonical_name::text" in sql:
            return MappingResult(rows=[])
        if "from normalize.claim_evidence" in sql and "from core.university" in sql:
            return MappingResult(rows=self._list_evidence_for_university(params["university_id"]))
        if "from normalize.claim" in sql and "from core.university" in sql:
            return MappingResult(rows=self._list_claims_for_university(params["university_id"]))
        if "insert into core.university" in sql:
            return MappingResult(row=self._upsert_university(params))
        if "insert into core.resolved_fact" in sql:
            return MappingResult(row=self._upsert_resolved_fact(params))
        if "insert into core.card_version" in sql:
            return MappingResult(row=self._upsert_card_version(params))
        if "insert into delivery.university_card" in sql:
            return MappingResult(row=self._upsert_delivery_card(params))
        if "insert into delivery.university_search_doc" in sql:
            return MappingResult(row=self._upsert_search_doc(params))
        raise AssertionError(f"Unexpected SQL statement: {statement}")

    def commit(self) -> None:
        self.commit_count += 1

    def seed_raw_artifact(self, entry: FixtureBundleEntry, payload: bytes) -> None:
        self.raw_artifacts[entry.raw_artifact_id] = {
            "raw_artifact_id": entry.raw_artifact_id,
            "crawl_run_id": entry.crawl_run_id,
            "source_key": entry.source_key,
            "source_url": entry.endpoint_url,
            "final_url": entry.final_url or entry.endpoint_url,
            "http_status": entry.http_status,
            "content_type": entry.content_type,
            "content_length": len(payload),
            "sha256": entry.sha256,
            "storage_bucket": "fixture-bundle",
            "storage_object_key": entry.fixture_file,
            "etag": entry.etag,
            "last_modified": entry.last_modified,
            "fetched_at": entry.fetched_at,
            "metadata": json_to_db(
                {
                    "parser_profile": entry.parser_profile,
                    "requested_at": entry.requested_at.isoformat(),
                    "response_headers": entry.response_headers,
                    "fixture_id": entry.fixture_id,
                }
            ),
        }

    def _upsert_parsed_document(self, params: dict[str, Any]) -> dict[str, Any]:
        key = (params["raw_artifact_id"], params["parser_version"])
        existing = self.parsed_documents.get(key)
        row = dict(params)
        if existing is not None:
            row["parsed_document_id"] = existing["parsed_document_id"]
            row["metadata"] = json_to_db(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                }
            )
        self.parsed_documents[key] = row
        return row

    def _upsert_fragment(self, params: dict[str, Any]) -> dict[str, Any]:
        existing = self.fragments.get(params["fragment_id"])
        row = dict(params)
        if existing is not None:
            row["metadata"] = json_to_db(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                }
            )
        self.fragments[params["fragment_id"]] = row
        return row

    def _get_parsed_document(self, parsed_document_id: UUID) -> dict[str, Any] | None:
        return next(
            (
                row
                for row in self.parsed_documents.values()
                if row["parsed_document_id"] == parsed_document_id
            ),
            None,
        )

    def _list_fragments_for_claim_build(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        parsed_document_id = params["parsed_document_id"]
        rows: list[dict[str, Any]] = []
        for fragment in self.fragments.values():
            if fragment["parsed_document_id"] != parsed_document_id:
                continue
            raw_artifact = self.raw_artifacts[fragment["raw_artifact_id"]]
            rows.append(
                {
                    **fragment,
                    "source_url": raw_artifact["source_url"],
                    "captured_at": raw_artifact["fetched_at"],
                }
            )
        return sorted(rows, key=lambda row: (row["field_name"], str(row["fragment_id"])))

    def _upsert_claim(self, params: dict[str, Any]) -> dict[str, Any]:
        existing = self.claims.get(params["claim_id"])
        row = {
            **params,
            "created_at": existing["created_at"] if existing else self.now,
        }
        if existing is not None:
            row["metadata"] = json_to_db(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                }
            )
        self.claims[params["claim_id"]] = row
        return row

    def _upsert_evidence(self, params: dict[str, Any]) -> dict[str, Any]:
        existing = self.evidence.get(params["evidence_id"])
        row = dict(params)
        if existing is not None:
            row["metadata"] = json_to_db(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                }
            )
        self.evidence[params["evidence_id"]] = row
        return row

    def _find_university_by_domain(self, canonical_domain: str) -> dict[str, Any] | None:
        for row in self.universities.values():
            if row["canonical_domain"] == canonical_domain:
                return row
        return None

    def _find_university_by_name(self, canonical_name: str) -> dict[str, Any] | None:
        for row in self.universities.values():
            if row["canonical_name"].strip().lower() == canonical_name.strip().lower():
                return row
        return None

    def _list_claims_for_university(self, university_id: UUID) -> list[dict[str, Any]]:
        university = self.universities[university_id]
        claim_ids = [
            UUID(claim_id)
            for claim_id in json_from_db(university["metadata"])["claim_ids"]
        ]
        rows = [self.claims[claim_id] for claim_id in claim_ids]
        return sorted(
            rows,
            key=lambda row: (row["source_key"], row["field_name"], str(row["claim_id"])),
        )

    def _list_evidence_for_university(self, university_id: UUID) -> list[dict[str, Any]]:
        claim_rows = self._list_claims_for_university(university_id)
        claim_ids = {row["claim_id"] for row in claim_rows}
        rows = [row for row in self.evidence.values() if row["claim_id"] in claim_ids]
        return sorted(
            rows,
            key=lambda row: (row["source_key"], row["source_url"], str(row["evidence_id"])),
        )

    def _upsert_university(self, params: dict[str, Any]) -> dict[str, Any]:
        existing = self.universities.get(params["university_id"])
        row = {
            **params,
            "created_at": existing["created_at"] if existing else self.now,
        }
        if existing is not None:
            row["metadata"] = json_to_db(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                }
            )
        self.universities[params["university_id"]] = row
        return row

    def _upsert_resolved_fact(self, params: dict[str, Any]) -> dict[str, Any]:
        existing = self.resolved_facts.get(params["resolved_fact_id"])
        row = {
            **params,
            "resolved_at": existing["resolved_at"] if existing else self.now,
        }
        if existing is not None:
            row["metadata"] = json_to_db(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                }
            )
        self.resolved_facts[params["resolved_fact_id"]] = row
        return row

    def _upsert_card_version(self, params: dict[str, Any]) -> dict[str, Any]:
        key = (params["university_id"], params["card_version"])
        row = {
            **params,
            "generated_at": self.now,
        }
        self.card_versions[key] = row
        return row

    def _upsert_delivery_card(self, params: dict[str, Any]) -> dict[str, Any]:
        key = (params["university_id"], params["card_version"])
        self.delivery_cards[key] = dict(params)
        return self.delivery_cards[key]

    def _upsert_search_doc(self, params: dict[str, Any]) -> dict[str, Any]:
        key = (params["university_id"], params["card_version"])
        self.search_docs[key] = dict(params)
        return self.search_docs[key]


def manifest() -> FixtureBundleManifest:
    return FixtureBundleManifest.read(MANIFEST_PATH)


def fixture_payload(entry: FixtureBundleEntry) -> bytes:
    return (MANIFEST_PATH.parent / entry.normalized_fixture_file).read_bytes()


def build_context(entry: FixtureBundleEntry) -> FetchContext:
    return FetchContext(
        crawl_run_id=entry.crawl_run_id,
        source_key=entry.source_key,
        endpoint_url=entry.endpoint_url,
        priority=entry.priority,
        trigger="replay",
        parser_profile=entry.parser_profile,
        requested_at=entry.requested_at,
        render_mode=entry.crawl_policy.render_mode,
        timeout_seconds=entry.crawl_policy.timeout_seconds,
        max_retries=entry.crawl_policy.max_retries,
        retry_backoff_seconds=entry.crawl_policy.retry_backoff_seconds,
        respect_robots_txt=entry.crawl_policy.respect_robots_txt,
        allowed_content_types=entry.crawl_policy.allowed_content_types,
        request_headers=entry.crawl_policy.request_headers,
        metadata={"fixture_id": entry.fixture_id, "bundle": "mvp-demo-bundle"},
    )


def build_artifact(entry: FixtureBundleEntry, payload: bytes) -> FetchedArtifact:
    return FetchedArtifact(
        raw_artifact_id=entry.raw_artifact_id,
        crawl_run_id=entry.crawl_run_id,
        source_key=entry.source_key,
        source_url=entry.endpoint_url,
        final_url=entry.final_url or entry.endpoint_url,
        http_status=entry.http_status,
        content_type=entry.content_type,
        response_headers=entry.response_headers,
        content_length=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        fetched_at=entry.fetched_at,
        render_mode=entry.crawl_policy.render_mode,
        etag=entry.etag,
        last_modified=entry.last_modified,
        content=payload,
        metadata={"fixture_id": entry.fixture_id},
    )


def build_adapter(entry: FixtureBundleEntry, artifact: FetchedArtifact):
    fetcher = FakeFetcher(artifact)
    if entry.fixture_id == "official":
        return OfficialSiteAdapter(fetcher=fetcher)
    if entry.fixture_id == "aggregator":
        return AggregatorAdapter(fetcher=fetcher)
    if entry.fixture_id == "ranking":
        return RankingAdapter(fetcher=fetcher)
    raise AssertionError(f"Unsupported fixture_id: {entry.fixture_id}")


def persist_bundle_parsed_documents(
    session: InMemoryRegressionSession,
) -> dict[str, Any]:
    repository = ParsedDocumentRepository(session=session, sql_text=lambda value: value)
    service = ParsedDocumentPersistenceService(repository)
    persisted: dict[str, Any] = {}
    for entry in manifest().entries:
        payload = fixture_payload(entry)
        context = build_context(entry)
        artifact = build_artifact(entry, payload)
        adapter = build_adapter(entry, artifact)
        session.seed_raw_artifact(entry, payload)
        execution_result = asyncio.run(adapter.execute(context))
        document, fragments = service.persist_successful_execution(
            execution_result=execution_result
        )
        persisted[entry.fixture_id] = {
            "entry": entry,
            "context": context,
            "artifact": artifact,
            "execution_result": execution_result,
            "document": document,
            "fragments": fragments,
        }
    return persisted


def build_normalize_payload(parsed_document) -> NormalizeRequestPayload:
    return NormalizeRequestPayload(
        crawl_run_id=parsed_document.crawl_run_id,
        source_key=parsed_document.source_key,
        parsed_document_id=parsed_document.parsed_document_id,
        parser_version=parsed_document.parser_version,
        normalizer_version="normalizer.regression.0.1.0",
        metadata={"trigger": "regression"},
    )


def test_mvp_bundle_integrity_matches_manifest_sha_and_lengths() -> None:
    bundle = manifest()

    assert bundle.bundle_name == "mvp-demo-bundle"
    assert [entry.fixture_id for entry in bundle.entries] == [
        "official",
        "aggregator",
        "ranking",
    ]
    for entry in bundle.entries:
        payload = fixture_payload(entry)
        assert len(payload) == entry.content_length
        assert hashlib.sha256(payload).hexdigest() == entry.sha256


def test_captured_bundle_adapter_regression_suite() -> None:
    persisted = persist_bundle_parsed_documents(InMemoryRegressionSession())

    for fixture_id, data in persisted.items():
        expected = EXPECTED_FIELDS_BY_FIXTURE_ID[fixture_id]
        execution_result = data["execution_result"]
        fragments = data["fragments"]
        by_field = {fragment.field_name: fragment for fragment in fragments}

        assert execution_result.extracted_fragments == expected["count"]
        assert len(fragments) == expected["count"]
        assert data["document"].extracted_fragment_count == expected["count"]
        assert data["document"].entity_hint == "Example University"

        for field_name, expected_value in expected.items():
            if field_name == "count":
                continue
            assert by_field[field_name].value == expected_value


def test_captured_bundle_normalization_regression_suite() -> None:
    session = InMemoryRegressionSession()
    persisted = persist_bundle_parsed_documents(session)

    claim_service = ClaimBuildService(
        ClaimBuildRepository(session=session, sql_text=lambda value: value)
    )
    bootstrap_service = UniversityBootstrapService(
        UniversityBootstrapRepository(session=session, sql_text=lambda value: value),
        policy_matrix=FieldResolutionPolicyMatrix(),
    )
    fact_service = ResolvedFactGenerationService(
        ResolvedFactRepository(session=session, sql_text=lambda value: value),
        policy_matrix=FieldResolutionPolicyMatrix(),
    )
    projection_service = UniversityCardProjectionService(
        UniversityCardProjectionRepository(session=session, sql_text=lambda value: value),
        search_doc_service=UniversitySearchDocProjectionService(
            UniversitySearchDocProjectionRepository(session=session, sql_text=lambda value: value)
        ),
    )

    projection_result = None
    final_fact_result = None
    final_bootstrap_result = None
    for fixture_id in ("official", "aggregator", "ranking"):
        parsed_document = persisted[fixture_id]["document"]
        claim_result = claim_service.build_claims_from_extracted_fragments(
            build_normalize_payload(parsed_document)
        )
        bootstrap_result = bootstrap_service.consolidate_claims(claim_result)
        fact_result = fact_service.generate_for_bootstrap(bootstrap_result)
        projection_result = projection_service.create_projection(fact_result)
        final_bootstrap_result = bootstrap_result
        final_fact_result = fact_result

    assert projection_result is not None
    assert final_bootstrap_result is not None
    assert final_fact_result is not None

    university = final_bootstrap_result.university
    assert university.university_id == deterministic_university_id("msu-official")
    assert university.canonical_name == "Example University"
    assert university.canonical_domain == "example.edu"
    assert university.city_name == "Moscow"
    assert sorted(university.metadata["source_keys"]) == [
        "msu-official",
        "qs-world-ranking",
        "study-aggregator",
    ]

    snapshots = {
        snapshot["source_key"]: snapshot
        for snapshot in university.metadata["source_snapshots"]
    }
    assert set(snapshots) == {
        "msu-official",
        "study-aggregator",
        "qs-world-ranking",
    }

    facts_by_field = {fact.field_name: fact for fact in final_fact_result.facts}
    assert facts_by_field["canonical_name"].value == "Example University"
    assert facts_by_field["canonical_name"].metadata["source_key"] == "msu-official"
    assert facts_by_field["contacts.website"].value == "https://example.edu"
    assert facts_by_field["contacts.website"].metadata["source_key"] == "msu-official"
    assert facts_by_field["location.city"].value == "Moscow"
    assert facts_by_field["location.city"].metadata["source_key"] == "msu-official"

    rating_fact = facts_by_field["ratings.qs-world:2026:world_overall:example-university"]
    assert rating_fact.value == {
        "provider": "QS World University Rankings",
        "year": 2026,
        "metric": "world_overall",
        "value": "151",
    }
    assert rating_fact.metadata["source_key"] == "qs-world-ranking"

    card = projection_result.projection.card
    assert card.canonical_name.value == "Example University"
    assert card.canonical_name.sources[0].source_key == "msu-official"
    assert card.contacts.website == "https://example.edu"
    assert card.location.city == "Moscow"
    assert len(card.ratings) == 1
    assert card.ratings[0].provider == "QS World University Rankings"
    assert card.ratings[0].year == 2026
    assert card.ratings[0].metric == "world_overall"
    assert card.ratings[0].value == "151"

    search_doc = projection_result.search_doc
    assert search_doc.canonical_name == "Example University"
    assert search_doc.website_domain == "example.edu"
    assert search_doc.city_name == "Moscow"
    assert search_doc.search_document["ratings"][0]["provider"] == (
        "QS World University Rankings"
    )
