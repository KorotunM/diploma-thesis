from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from apps.backend.app.cards import UniversityCardReadRepository, UniversityCardReadService
from apps.backend.app.dependencies import (
    get_university_card_read_service,
    get_university_provenance_read_service,
    get_university_search_service,
)
from apps.backend.app.main import app
from apps.backend.app.provenance import (
    UniversityProvenanceReadService,
    UniversityProvenanceRepository,
)
from apps.backend.app.search import UniversitySearchRepository, UniversitySearchService
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
from apps.normalizer.app.resolution import SourceTrustTier
from apps.normalizer.app.search_docs import (
    UniversitySearchDocProjectionRepository,
    UniversitySearchDocProjectionService,
)
from apps.normalizer.app.universities import (
    UniversityBootstrapRepository,
    UniversityBootstrapService,
    deterministic_university_id,
)
from libs.contracts.events import NormalizeRequestPayload


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


class InMemoryHappyPathSession:
    def __init__(self) -> None:
        self.now = datetime(2026, 4, 29, 11, 0, tzinfo=UTC)
        self.commit_count = 0
        self.sources: dict[str, dict[str, Any]] = {
            "msu-official": {
                "source_id": uuid4(),
                "source_key": "msu-official",
                "source_type": "official_site",
                "trust_tier": SourceTrustTier.AUTHORITATIVE.value,
                "is_active": True,
                "metadata": json_to_db({"owner": "admissions"}),
            }
        }
        self.raw_artifacts: dict[UUID, dict[str, Any]] = {}
        self.parsed_documents: dict[UUID, dict[str, Any]] = {}
        self.fragments_by_document: dict[UUID, list[dict[str, Any]]] = {}
        self.claims: dict[UUID, dict[str, Any]] = {}
        self.evidence: dict[UUID, dict[str, Any]] = {}
        self.universities: dict[UUID, dict[str, Any]] = {}
        self.resolved_facts: dict[UUID, dict[str, Any]] = {}
        self.card_versions: dict[tuple[UUID, int], dict[str, Any]] = {}
        self.delivery_cards: dict[tuple[UUID, int], dict[str, Any]] = {}
        self.search_docs: dict[tuple[UUID, int], dict[str, Any]] = {}
        self.seed_official_document()

    def seed_official_document(self) -> None:
        crawl_run_id = uuid4()
        parsed_document_id = uuid4()
        raw_artifact_id = uuid4()
        self.raw_artifacts[raw_artifact_id] = {
            "raw_artifact_id": raw_artifact_id,
            "crawl_run_id": crawl_run_id,
            "source_key": "msu-official",
            "source_url": "https://www.example.edu/admissions",
            "final_url": "https://www.example.edu/admissions",
            "http_status": 200,
            "content_type": "text/html",
            "content_length": 4096,
            "sha256": "a" * 64,
            "storage_bucket": "raw-html",
            "storage_object_key": "msu-official/aa/bb/example.html",
            "etag": '"official-etag"',
            "last_modified": "Tue, 29 Apr 2026 09:55:00 GMT",
            "fetched_at": datetime(2026, 4, 29, 10, 55, tzinfo=UTC),
            "metadata": json_to_db({"seeded": True}),
        }
        self.parsed_documents[parsed_document_id] = {
            "parsed_document_id": parsed_document_id,
            "crawl_run_id": crawl_run_id,
            "raw_artifact_id": raw_artifact_id,
            "source_key": "msu-official",
            "parser_profile": "official_site.default",
            "parser_version": "official.0.1.0",
            "entity_type": "university",
            "entity_hint": "Example University",
            "extracted_fragment_count": 4,
            "parsed_at": datetime(2026, 4, 29, 10, 57, tzinfo=UTC),
            "metadata": json_to_db({"adapter": "official_sites"}),
        }
        self.fragments_by_document[parsed_document_id] = [
            self._fragment_row(
                parsed_document_id=parsed_document_id,
                raw_artifact_id=raw_artifact_id,
                field_name="canonical_name",
                value="Example University",
                value_type="str",
                locator="h1",
                confidence=0.99,
            ),
            self._fragment_row(
                parsed_document_id=parsed_document_id,
                raw_artifact_id=raw_artifact_id,
                field_name="contacts.website",
                value="https://www.example.edu/admissions",
                value_type="str",
                locator="a.site-link",
                confidence=0.93,
            ),
            self._fragment_row(
                parsed_document_id=parsed_document_id,
                raw_artifact_id=raw_artifact_id,
                field_name="location.city",
                value="Moscow",
                value_type="str",
                locator=".city",
                confidence=0.89,
            ),
            self._fragment_row(
                parsed_document_id=parsed_document_id,
                raw_artifact_id=raw_artifact_id,
                field_name="location.country_code",
                value="RU",
                value_type="str",
                locator=".country",
                confidence=0.88,
            ),
        ]

    def _fragment_row(
        self,
        *,
        parsed_document_id: UUID,
        raw_artifact_id: UUID,
        field_name: str,
        value: Any,
        value_type: str,
        locator: str,
        confidence: float,
    ) -> dict[str, Any]:
        return {
            "fragment_id": uuid4(),
            "parsed_document_id": parsed_document_id,
            "raw_artifact_id": raw_artifact_id,
            "source_key": "msu-official",
            "source_url": "https://www.example.edu/admissions",
            "captured_at": datetime(2026, 4, 29, 10, 55, tzinfo=UTC),
            "field_name": field_name,
            "value": json_to_db({"value": value}),
            "value_type": value_type,
            "locator": locator,
            "confidence": confidence,
            "metadata": json_to_db({"seeded": True}),
        }

    def execute(self, statement: str, params: dict[str, Any]) -> MappingResult:
        sql = " ".join(statement.split()).lower()
        if (
            "from parsing.parsed_document" in sql
            and "where parsed_document_id = :parsed_document_id" in sql
        ):
            return MappingResult(row=self.parsed_documents.get(params["parsed_document_id"]))
        if (
            "from parsing.extracted_fragment as ef" in sql
            and "join ingestion.raw_artifact as ra" in sql
        ):
            return MappingResult(
                rows=self.fragments_by_document.get(params["parsed_document_id"], [])
            )
        if "insert into normalize.claim_evidence" in sql:
            return MappingResult(row=self._upsert_evidence(params))
        if "insert into normalize.claim " in sql:
            return MappingResult(row=self._upsert_claim(params))
        if "from ingestion.source" in sql:
            return MappingResult(row=self.sources.get(params["source_key"]))
        if "from core.university" in sql and "where canonical_domain = :canonical_domain" in sql:
            return MappingResult(row=self._find_university_by_domain(params["canonical_domain"]))
        if "from core.university" in sql and "where canonical_name = :canonical_name" in sql:
            return MappingResult(row=self._find_university_by_name(params["canonical_name"]))
        if "from normalize.claim_evidence" in sql and "from core.university" in sql:
            return MappingResult(rows=self._list_evidence_for_university(params["university_id"]))
        if "from normalize.claim" in sql and "from core.university" in sql:
            return MappingResult(rows=self._list_claims_for_university(params["university_id"]))
        if "from core.university" in sql and "where university_id = :university_id" in sql:
            return MappingResult(row=self.universities.get(params["university_id"]))
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
        if "from delivery.university_search_doc as search_doc" in sql:
            return MappingResult(rows=self._search_docs_query(params))
        if "from delivery.university_card as projection" in sql:
            return MappingResult(row=self._latest_projection_context(params["university_id"]))
        if "from delivery.university_card" in sql and "order by card_version desc" in sql:
            return MappingResult(row=self._latest_delivery_card(params["university_id"]))
        if "select field_name" in sql and "from core.resolved_fact" in sql:
            return MappingResult(
                rows=self._list_card_facts(
                    university_id=params["university_id"],
                    card_version=params["card_version"],
                )
            )
        if "select resolved_fact_id" in sql and "from core.resolved_fact" in sql:
            return MappingResult(
                rows=self._list_resolved_facts_trace(
                    university_id=params["university_id"],
                    card_version=params["card_version"],
                )
            )
        if "with selected_claim_documents as" in sql:
            return MappingResult(
                rows=self._list_raw_artifacts_for_card(
                    university_id=params["university_id"],
                    card_version=params["card_version"],
                )
            )
        if "join normalize.claim_evidence as evidence" in sql and "distinct" in sql:
            return MappingResult(
                rows=self._list_evidence_for_card(
                    university_id=params["university_id"],
                    card_version=params["card_version"],
                )
            )
        if "join parsing.parsed_document as document" in sql and "distinct" in sql:
            return MappingResult(
                rows=self._list_parsed_documents_for_card(
                    university_id=params["university_id"],
                    card_version=params["card_version"],
                )
            )
        if "join normalize.claim as claim" in sql and "distinct" in sql:
            return MappingResult(
                rows=self._list_claims_for_card(
                    university_id=params["university_id"],
                    card_version=params["card_version"],
                )
            )
        raise AssertionError(f"Unexpected SQL statement: {statement}")

    def commit(self) -> None:
        self.commit_count += 1

    def _upsert_claim(self, params: dict[str, Any]) -> dict[str, Any]:
        existing = self.claims.get(params["claim_id"])
        row = {
            **params,
            "created_at": existing["created_at"] if existing else self.now,
        }
        if existing is not None:
            row["metadata"] = json.dumps(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        self.claims[params["claim_id"]] = row
        return row

    def _upsert_evidence(self, params: dict[str, Any]) -> dict[str, Any]:
        existing = self.evidence.get(params["evidence_id"])
        row = dict(params)
        if existing is not None:
            row["metadata"] = json.dumps(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                },
                ensure_ascii=False,
                sort_keys=True,
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
            UUID(claim_id) for claim_id in json_from_db(university["metadata"])["claim_ids"]
        ]
        return [self.claims[claim_id] for claim_id in claim_ids]

    def _list_evidence_for_university(self, university_id: UUID) -> list[dict[str, Any]]:
        university = self.universities[university_id]
        claim_ids = {
            UUID(claim_id) for claim_id in json_from_db(university["metadata"])["claim_ids"]
        }
        return [
            row
            for row in self.evidence.values()
            if row["claim_id"] in claim_ids
        ]

    def _upsert_university(self, params: dict[str, Any]) -> dict[str, Any]:
        existing = self.universities.get(params["university_id"])
        row = {
            **params,
            "created_at": existing["created_at"] if existing else self.now,
        }
        if existing is not None:
            row["metadata"] = json.dumps(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                },
                ensure_ascii=False,
                sort_keys=True,
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
            row["metadata"] = json.dumps(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                },
                ensure_ascii=False,
                sort_keys=True,
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
        row = dict(params)
        self.delivery_cards[key] = row
        return row

    def _upsert_search_doc(self, params: dict[str, Any]) -> dict[str, Any]:
        key = (params["university_id"], params["card_version"])
        row = dict(params)
        self.search_docs[key] = row
        return row

    def _search_docs_query(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        query = (params.get("query") or "").strip().lower()
        city = (params.get("city") or "").strip().lower()
        country_code = (params.get("country_code") or "").strip().upper()
        source_type = (params.get("source_type") or "").strip().lower()
        offset = int(params["offset"])
        limit = int(params["limit"])

        rows: list[dict[str, Any]] = []
        for row in self.search_docs.values():
            university_row = self.universities[row["university_id"]]
            university_metadata = json_from_db(university_row["metadata"])
            search_document = json_from_db(row["search_document"])
            haystack = " ".join(
                [
                    row["canonical_name"],
                    row["canonical_name_normalized"],
                    row["website_domain"] or "",
                    row["city_name"] or "",
                    row["country_code"] or "",
                    *list(row["aliases"] or []),
                ]
            ).lower()
            if query and query not in haystack:
                continue
            if city and (row["city_name"] or "").lower() != city:
                continue
            if country_code and (row["country_code"] or "").upper() != country_code:
                continue
            if source_type:
                source_types = {
                    str(university_metadata.get("source_type", "")).lower(),
                    *[
                        str(snapshot.get("source_type", "")).lower()
                        for snapshot in university_metadata.get("source_snapshots", [])
                        if isinstance(snapshot, dict)
                    ],
                }
                if source_type not in source_types:
                    continue

            text_rank = 0.91 if query else 0.0
            trigram_score = 0.83 if query else 0.0
            rows.append(
                {
                    "university_id": row["university_id"],
                    "card_version": row["card_version"],
                    "canonical_name": row["canonical_name"],
                    "website_url": row["website_url"],
                    "website_domain": row["website_domain"],
                    "country_code": row["country_code"],
                    "city_name": row["city_name"],
                    "aliases": list(row["aliases"] or []),
                    "metadata": row["metadata"],
                    "generated_at": row["generated_at"],
                    "text_rank": text_rank,
                    "trigram_score": trigram_score,
                    "combined_score": (text_rank * 0.7) + (trigram_score * 0.3),
                    "total_count": 0,
                    "search_document": search_document,
                }
            )

        rows.sort(key=lambda item: (-item["combined_score"], item["canonical_name"]))
        total_count = len(rows)
        paged = rows[offset : offset + limit]
        for row in paged:
            row["total_count"] = total_count
        return paged

    def _latest_projection_context(self, university_id: UUID) -> dict[str, Any] | None:
        projection = self._latest_delivery_card(university_id)
        if projection is None:
            return None
        card_version = self.card_versions[(university_id, projection["card_version"])]
        return {
            "university_id": projection["university_id"],
            "card_version": projection["card_version"],
            "card_json": projection["card_json"],
            "projection_generated_at": projection["generated_at"],
            "card_generated_at": card_version["generated_at"],
            "normalizer_version": card_version["normalizer_version"],
        }

    def _latest_delivery_card(self, university_id: UUID) -> dict[str, Any] | None:
        candidates = [
            row
            for (candidate_university_id, _), row in self.delivery_cards.items()
            if candidate_university_id == university_id
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda row: row["card_version"])

    def _list_card_facts(
        self,
        *,
        university_id: UUID,
        card_version: int,
    ) -> list[dict[str, Any]]:
        return [
            {
                "field_name": row["field_name"],
                "resolution_policy": row["resolution_policy"],
                "fact_score": row["fact_score"],
                "metadata": row["metadata"],
            }
            for row in self._facts_for_card(university_id=university_id, card_version=card_version)
        ]

    def _list_resolved_facts_trace(
        self,
        *,
        university_id: UUID,
        card_version: int,
    ) -> list[dict[str, Any]]:
        return list(self._facts_for_card(university_id=university_id, card_version=card_version))

    def _facts_for_card(
        self,
        *,
        university_id: UUID,
        card_version: int,
    ) -> list[dict[str, Any]]:
        return [
            row
            for row in self.resolved_facts.values()
            if row["university_id"] == university_id
            and row["card_version"] == card_version
        ]

    def _list_claims_for_card(
        self,
        *,
        university_id: UUID,
        card_version: int,
    ) -> list[dict[str, Any]]:
        claim_ids = self._selected_ids(
            university_id=university_id,
            card_version=card_version,
            key="selected_claim_ids",
        )
        rows = [self.claims[claim_id] for claim_id in claim_ids]
        return sorted(rows, key=lambda row: (row["created_at"], str(row["claim_id"])))

    def _list_evidence_for_card(
        self,
        *,
        university_id: UUID,
        card_version: int,
    ) -> list[dict[str, Any]]:
        evidence_ids = self._selected_ids(
            university_id=university_id,
            card_version=card_version,
            key="selected_evidence_ids",
        )
        rows = [self.evidence[evidence_id] for evidence_id in evidence_ids]
        return sorted(rows, key=lambda row: (row["captured_at"], str(row["evidence_id"])))

    def _list_parsed_documents_for_card(
        self,
        *,
        university_id: UUID,
        card_version: int,
    ) -> list[dict[str, Any]]:
        claim_rows = self._list_claims_for_card(
            university_id=university_id,
            card_version=card_version,
        )
        parsed_document_ids = {
            row["parsed_document_id"]
            for row in claim_rows
        }
        rows = [
            self.parsed_documents[parsed_document_id]
            for parsed_document_id in parsed_document_ids
        ]
        return sorted(
            rows,
            key=lambda row: (row["parsed_at"], str(row["parsed_document_id"])),
        )

    def _list_raw_artifacts_for_card(
        self,
        *,
        university_id: UUID,
        card_version: int,
    ) -> list[dict[str, Any]]:
        parsed_documents = self._list_parsed_documents_for_card(
            university_id=university_id,
            card_version=card_version,
        )
        evidence_rows = self._list_evidence_for_card(
            university_id=university_id,
            card_version=card_version,
        )
        raw_artifact_ids = {
            row["raw_artifact_id"] for row in parsed_documents
        } | {
            row["raw_artifact_id"] for row in evidence_rows
        }
        rows = [self.raw_artifacts[raw_artifact_id] for raw_artifact_id in raw_artifact_ids]
        return sorted(rows, key=lambda row: (row["fetched_at"], str(row["raw_artifact_id"])))

    def _selected_ids(
        self,
        *,
        university_id: UUID,
        card_version: int,
        key: str,
    ) -> list[UUID]:
        selected: list[UUID] = []
        for row in self._facts_for_card(university_id=university_id, card_version=card_version):
            metadata = json_from_db(row["metadata"])
            for value in metadata.get(key, []):
                selected.append(UUID(str(value)))
        return selected


def build_normalizer_services(
    session: InMemoryHappyPathSession,
) -> tuple[
    ClaimBuildService,
    UniversityBootstrapService,
    ResolvedFactGenerationService,
    UniversityCardProjectionService,
]:
    claim_repository = ClaimBuildRepository(session=session, sql_text=lambda value: value)
    bootstrap_repository = UniversityBootstrapRepository(
        session=session,
        sql_text=lambda value: value,
    )
    facts_repository = ResolvedFactRepository(session=session, sql_text=lambda value: value)
    cards_repository = UniversityCardProjectionRepository(
        session=session,
        sql_text=lambda value: value,
    )
    search_docs_repository = UniversitySearchDocProjectionRepository(
        session=session,
        sql_text=lambda value: value,
    )
    return (
        ClaimBuildService(claim_repository),
        UniversityBootstrapService(bootstrap_repository),
        ResolvedFactGenerationService(facts_repository),
        UniversityCardProjectionService(
            cards_repository,
            search_doc_service=UniversitySearchDocProjectionService(search_docs_repository),
        ),
    )


def build_backend_services(
    session: InMemoryHappyPathSession,
) -> tuple[
    UniversitySearchService,
    UniversityCardReadService,
    UniversityProvenanceReadService,
]:
    return (
        UniversitySearchService(
            UniversitySearchRepository(session=session, sql_text=lambda value: value)
        ),
        UniversityCardReadService(
            UniversityCardReadRepository(session=session, sql_text=lambda value: value)
        ),
        UniversityProvenanceReadService(
            UniversityProvenanceRepository(session=session, sql_text=lambda value: value)
        ),
    )


def payload_for(session: InMemoryHappyPathSession) -> NormalizeRequestPayload:
    parsed_document = next(iter(session.parsed_documents.values()))
    return NormalizeRequestPayload(
        crawl_run_id=parsed_document["crawl_run_id"],
        source_key=parsed_document["source_key"],
        parsed_document_id=parsed_document["parsed_document_id"],
        parser_version=parsed_document["parser_version"],
        normalizer_version="normalizer.0.1.0",
    )


def test_search_to_card_to_evidence_happy_path() -> None:
    session = InMemoryHappyPathSession()
    claim_service, bootstrap_service, fact_service, projection_service = (
        build_normalizer_services(session)
    )
    claim_result = claim_service.build_claims_from_extracted_fragments(payload_for(session))
    bootstrap_result = bootstrap_service.consolidate_claims(claim_result)
    fact_result = fact_service.generate_for_bootstrap(bootstrap_result)
    projection_result = projection_service.create_projection(fact_result)
    search_service, card_service, provenance_service = build_backend_services(session)

    app.dependency_overrides[get_university_search_service] = lambda: search_service
    app.dependency_overrides[get_university_card_read_service] = lambda: card_service
    app.dependency_overrides[get_university_provenance_read_service] = (
        lambda: provenance_service
    )
    try:
        client = TestClient(app)
        search_response = client.get(
            "/api/v1/search",
            params={"query": "Example University", "page": 1, "page_size": 10},
        )
        assert search_response.status_code == 200
        search_body = search_response.json()
        assert search_body["query"] == "Example University"
        assert search_body["total"] == 1
        assert search_body["items"][0]["canonical_name"] == "Example University"
        assert search_body["items"][0]["website"] == "https://www.example.edu/admissions"
        assert search_body["items"][0]["match_signals"] == ["full_text", "trigram"]

        university_id = search_body["items"][0]["university_id"]
        assert university_id == str(deterministic_university_id("msu-official"))

        card_response = client.get(f"/api/v1/universities/{university_id}")
        assert card_response.status_code == 200
        card_body = card_response.json()
        assert card_body["canonical_name"]["value"] == "Example University"
        assert card_body["location"]["city"] == "Moscow"
        assert card_body["contacts"]["website"] == "https://www.example.edu/admissions"
        assert card_body["field_attribution"]["canonical_name"]["source_key"] == (
            "msu-official"
        )
        assert card_body["source_rationale"][0]["source_key"] == "msu-official"

        provenance_response = client.get(f"/api/v1/universities/{university_id}/provenance")
        assert provenance_response.status_code == 200
        provenance_body = provenance_response.json()
        assert provenance_body["university_id"] == university_id
        assert provenance_body["delivery_projection"]["card_version"] == (
            projection_result.projection.card_version
        )
        assert len(provenance_body["resolved_facts"]) == 4
        assert len(provenance_body["claims"]) == 4
        assert len(provenance_body["claim_evidence"]) == 4
        assert len(provenance_body["parsed_documents"]) == 1
        assert len(provenance_body["raw_artifacts"]) == 1
        assert provenance_body["raw_artifacts"][0]["source_url"] == (
            "https://www.example.edu/admissions"
        )
        assert provenance_body["claims"][0]["source_key"] == "msu-official"
        assert provenance_body["chain"][-1] == "delivery_projection"
        assert session.commit_count == 4
    finally:
        app.dependency_overrides.clear()
