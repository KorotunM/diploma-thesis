from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
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
from apps.normalizer.app.parse_completed.consumer import ParseCompletedConsumer
from apps.normalizer.app.parse_completed.service import ParseCompletedProcessingService
from apps.normalizer.app.persistence import json_from_db, json_to_db
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
from apps.parser.app.crawl_requests.consumer import CrawlRequestConsumer
from apps.parser.app.crawl_requests.service import CrawlRequestProcessingService
from apps.parser.app.parse_completed import ParseCompletedEmitter
from apps.parser.app.parsed_documents import (
    ParsedDocumentPersistenceService,
    ParsedDocumentRepository,
)
from apps.parser.app.raw_artifacts import RawArtifactPersistenceService, RawArtifactRepository
from apps.scheduler.app.discovery.models import DiscoveryMaterializationRequest
from apps.scheduler.app.discovery.service import SourceEndpointDiscoveryService
from apps.scheduler.app.runs.models import ManualCrawlTriggerRequest
from apps.scheduler.app.runs.service import ManualCrawlTriggerService
from apps.scheduler.app.sources.endpoint_repository import SourceEndpointRepository
from apps.scheduler.app.sources.repository import SourceRepository
from libs.source_sdk import FetchContext, FetchedArtifact
from libs.source_sdk.stores import MinIORawArtifactStore
from libs.storage import MinIOObjectWriteResult
from scripts.source_bootstrap.workflow import (
    LiveSourceSeedService,
    SchedulerSourceRegistryGateway,
    build_live_mvp_source_seed_specs,
)

FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "parser_ingestion"


class MappingResult:
    def __init__(
        self,
        *,
        row: dict[str, Any] | None = None,
        rows: list[dict[str, Any]] | None = None,
        scalar: Any | None = None,
    ) -> None:
        self._row = row
        self._rows = rows or []
        self._scalar = scalar

    def mappings(self) -> MappingResult:
        return self

    def one(self) -> dict[str, Any]:
        assert self._row is not None
        return self._row

    def one_or_none(self) -> dict[str, Any] | None:
        return self._row

    def all(self) -> list[dict[str, Any]]:
        return self._rows

    def scalar_one(self) -> Any:
        return self._scalar

    def scalar_one_or_none(self) -> Any:
        return self._scalar


class InMemorySourceRegistrySession:
    def __init__(self) -> None:
        self.sources: dict[str, dict[str, Any]] = {}
        self.endpoints: dict[UUID, dict[str, Any]] = {}
        self.commit_count = 0

    def execute(self, statement: str, params: dict[str, Any]) -> MappingResult:
        sql = " ".join(statement.split()).lower()

        if sql.startswith("select source_id from ingestion.source"):
            source = self.sources.get(params["source_key"])
            return MappingResult(scalar=None if source is None else source["source_id"])

        if sql.startswith("select") and "from ingestion.source_endpoint" in sql:
            return MappingResult(row=self._select_endpoint_row(params))

        if sql.startswith("select") and "from ingestion.source " in sql:
            return MappingResult(row=self.sources.get(params["source_key"]))

        if sql.startswith("insert into ingestion.source_endpoint"):
            row = {
                "endpoint_id": params["endpoint_id"],
                "source_id": params["source_id"],
                "source_key": params["source_key"],
                "endpoint_url": params["endpoint_url"],
                "parser_profile": params["parser_profile"],
                "crawl_policy": params["crawl_policy"],
            }
            self.endpoints[params["endpoint_id"]] = row
            return MappingResult(row=row)

        if sql.startswith("update ingestion.source_endpoint"):
            row = self.endpoints.get(params["endpoint_id"])
            if row is None or row["source_key"] != params["source_key"]:
                return MappingResult(row=None)
            if params["endpoint_url"] is not None:
                row["endpoint_url"] = params["endpoint_url"]
            if params["parser_profile"] is not None:
                row["parser_profile"] = params["parser_profile"]
            if params["crawl_policy_is_set"]:
                row["crawl_policy"] = params["crawl_policy"]
            return MappingResult(row=row)

        if sql.startswith("insert into ingestion.source "):
            row = {
                "source_id": params["source_id"],
                "source_key": params["source_key"],
                "source_type": params["source_type"],
                "trust_tier": params["trust_tier"],
                "is_active": params["is_active"],
                "metadata": params["metadata"],
            }
            self.sources[params["source_key"]] = row
            return MappingResult(row=row)

        if sql.startswith("update ingestion.source "):
            row = self.sources.get(params["source_key"])
            if row is None:
                return MappingResult(row=None)
            if params["source_type"] is not None:
                row["source_type"] = params["source_type"]
            if params["trust_tier"] is not None:
                row["trust_tier"] = params["trust_tier"]
            if params["is_active"] is not None:
                row["is_active"] = params["is_active"]
            if params["metadata_is_set"]:
                row["metadata"] = params["metadata"]
            return MappingResult(row=row)

        raise AssertionError(f"Unexpected scheduler SQL statement: {statement}")

    def commit(self) -> None:
        self.commit_count += 1

    def _select_endpoint_row(self, params: dict[str, Any]) -> dict[str, Any] | None:
        for row in self.endpoints.values():
            if row["source_key"] != params["source_key"]:
                continue
            if "endpoint_url" in params and row["endpoint_url"] == params["endpoint_url"]:
                return row
            if "endpoint_id" in params and row["endpoint_id"] == params["endpoint_id"]:
                return row
        return None


class InMemoryLiveMvpSession:
    def __init__(self, *, sources: dict[str, dict[str, Any]]) -> None:
        self.now = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
        self.commit_count = 0
        self.sources = {
            key: dict(value)
            for key, value in sources.items()
        }
        self.raw_artifacts: dict[UUID, dict[str, Any]] = {}
        self.raw_artifacts_by_source_sha: dict[tuple[str, str], UUID] = {}
        self.parsed_documents: dict[UUID, dict[str, Any]] = {}
        self.parsed_documents_by_raw_version: dict[tuple[UUID, str], UUID] = {}
        self.fragments_by_document: dict[UUID, list[dict[str, Any]]] = {}
        self.fragments_by_id: dict[UUID, dict[str, Any]] = {}
        self.claims: dict[UUID, dict[str, Any]] = {}
        self.evidence: dict[UUID, dict[str, Any]] = {}
        self.universities: dict[UUID, dict[str, Any]] = {}
        self.resolved_facts: dict[UUID, dict[str, Any]] = {}
        self.card_versions: dict[tuple[UUID, int], dict[str, Any]] = {}
        self.delivery_cards: dict[tuple[UUID, int], dict[str, Any]] = {}
        self.search_docs: dict[tuple[UUID, int], dict[str, Any]] = {}

    def execute(self, statement: str, params: dict[str, Any]) -> MappingResult:
        sql = " ".join(statement.split()).lower()

        if "insert into ingestion.raw_artifact" in sql:
            return MappingResult(row=self._upsert_raw_artifact(params))
        if "insert into parsing.parsed_document" in sql:
            return MappingResult(row=self._upsert_parsed_document(params))
        if "insert into parsing.extracted_fragment" in sql:
            return MappingResult(row=self._upsert_fragment(params))
        if (
            "from parsing.parsed_document" in sql
            and "where parsed_document_id = :parsed_document_id" in sql
        ):
            return MappingResult(row=self.parsed_documents.get(params["parsed_document_id"]))
        if "from parsing.extracted_fragment as ef" in sql and "join ingestion.raw_artifact as ra" in sql:
            return MappingResult(
                rows=sorted(
                    [
                        fragment
                        for fragment in self.fragments_by_id.values()
                        if fragment["parsed_document_id"] == params["parsed_document_id"]
                    ],
                    key=lambda row: (row["field_name"], str(row["fragment_id"])),
                )
            )
        if "from ingestion.source" in sql:
            return MappingResult(row=self.sources.get(params["source_key"]))
        if "insert into normalize.claim_evidence" in sql:
            return MappingResult(row=self._upsert_evidence(params))
        if "insert into normalize.claim " in sql:
            return MappingResult(row=self._upsert_claim(params))
        if "from core.university" in sql and "where canonical_domain = :canonical_domain" in sql:
            return MappingResult(row=self._find_university_by_domain(params["canonical_domain"]))
        if "from core.university" in sql and "where canonical_name = :canonical_name" in sql:
            return MappingResult(row=self._find_university_by_name(params["canonical_name"]))
        if "similarity(canonical_name::text" in sql:
            return MappingResult(rows=[])
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
        raise AssertionError(f"Unexpected live-flow SQL statement: {statement}")

    def commit(self) -> None:
        self.commit_count += 1

    def _upsert_raw_artifact(self, params: dict[str, Any]) -> dict[str, Any]:
        key = (params["source_key"], params["sha256"])
        existing_id = self.raw_artifacts_by_source_sha.get(key)
        existing = self.raw_artifacts.get(existing_id) if existing_id is not None else None
        row = dict(params)
        if existing is not None:
            row["raw_artifact_id"] = existing["raw_artifact_id"]
            row["metadata"] = json_to_db(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                }
            )
        self.raw_artifacts[row["raw_artifact_id"]] = row
        self.raw_artifacts_by_source_sha[key] = row["raw_artifact_id"]
        return row

    def _upsert_parsed_document(self, params: dict[str, Any]) -> dict[str, Any]:
        key = (params["raw_artifact_id"], params["parser_version"])
        existing_id = self.parsed_documents_by_raw_version.get(key)
        existing = self.parsed_documents.get(existing_id) if existing_id is not None else None
        row = dict(params)
        if existing is not None:
            row["parsed_document_id"] = existing["parsed_document_id"]
            row["metadata"] = json_to_db(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                }
            )
        self.parsed_documents[row["parsed_document_id"]] = row
        self.parsed_documents_by_raw_version[key] = row["parsed_document_id"]
        self.fragments_by_document.setdefault(row["parsed_document_id"], [])
        return row

    def _upsert_fragment(self, params: dict[str, Any]) -> dict[str, Any]:
        existing = self.fragments_by_id.get(params["fragment_id"])
        row = dict(params)
        if existing is not None:
            row["metadata"] = json_to_db(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                }
            )
            fragments = self.fragments_by_document[existing["parsed_document_id"]]
            for index, fragment in enumerate(fragments):
                if fragment["fragment_id"] == params["fragment_id"]:
                    fragments[index] = row
                    break
        else:
            self.fragments_by_document.setdefault(params["parsed_document_id"], []).append(row)
        raw_artifact = self.raw_artifacts[row["raw_artifact_id"]]
        enriched = {
            **row,
            "source_url": raw_artifact["source_url"],
            "captured_at": raw_artifact["fetched_at"],
        }
        self.fragments_by_id[row["fragment_id"]] = enriched
        return row

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
        parsed_document_ids = {row["parsed_document_id"] for row in claim_rows}
        rows = [
            self.parsed_documents[parsed_document_id]
            for parsed_document_id in parsed_document_ids
        ]
        return sorted(rows, key=lambda row: (row["parsed_at"], str(row["parsed_document_id"])))

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


class InMemoryPipelineRunRepository:
    def __init__(self) -> None:
        self.record: dict[str, Any] | None = None
        self.commit_count = 0

    def create(self, **kwargs) -> dict[str, Any]:
        self.record = {
            "run_id": kwargs["run_id"],
            "run_type": kwargs["run_type"],
            "status": kwargs["status"],
            "trigger_type": kwargs["trigger_type"],
            "source_key": kwargs["source_key"],
            "started_at": datetime(2026, 5, 2, 12, 10, tzinfo=UTC),
            "finished_at": None,
            "metadata": kwargs["metadata"],
        }
        return self.record

    def transition(self, **kwargs) -> dict[str, Any] | None:
        if self.record is None:
            return None
        self.record = {
            **self.record,
            "status": kwargs["status"],
            "metadata": {
                **self.record["metadata"],
                **(kwargs.get("metadata_patch") or {}),
            },
        }
        if kwargs.get("finish"):
            self.record["finished_at"] = datetime(2026, 5, 2, 12, 11, tzinfo=UTC)
        return self.record

    def commit(self) -> None:
        self.commit_count += 1


class CapturingPublisher:
    def __init__(self, *, exchange_name: str) -> None:
        self.exchange_name = exchange_name
        self.calls: list[dict[str, Any]] = []

    def publish(
        self,
        payload: Any,
        *,
        queue_name: str,
        headers: dict[str, Any] | None = None,
    ) -> SimpleNamespace:
        self.calls.append(
            {
                "payload": payload,
                "queue_name": queue_name,
                "headers": headers or {},
            }
        )
        routing_key = "high" if queue_name.endswith(".high") else "bulk"
        return SimpleNamespace(
            queue_name=queue_name,
            exchange_name=self.exchange_name,
            routing_key=routing_key,
        )


class StaticDiscoveryFetcher:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.calls: list[str] = []

    def fetch_bytes(
        self,
        *,
        url: str,
        timeout_seconds: int,
        request_headers: dict[str, str],
        allowed_content_types: list[str],
    ) -> bytes:
        self.calls.append(url)
        return self.payload


class FixtureFetcher:
    def __init__(self, fixtures: dict[str, tuple[bytes, str]]) -> None:
        self.fixtures = fixtures
        self.calls: list[str] = []

    async def fetch(self, context: FetchContext) -> FetchedArtifact:
        self.calls.append(context.endpoint_url)
        payload, content_type = self.fixtures[context.endpoint_url]
        return FetchedArtifact(
            raw_artifact_id=uuid4(),
            crawl_run_id=context.crawl_run_id,
            source_key=context.source_key,
            source_url=context.endpoint_url,
            final_url=context.endpoint_url,
            http_status=200,
            content_type=content_type,
            response_headers={"content-type": content_type},
            content_length=len(payload),
            sha256=hashlib.sha256(payload).hexdigest(),
            fetched_at=datetime(2026, 5, 2, 12, 12, tzinfo=UTC),
            render_mode=context.render_mode,
            content=payload,
            metadata={"fixture_endpoint_url": context.endpoint_url},
        )


class InMemoryMinIOStorage:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def ensure_bucket(self, bucket_name: str) -> bool:
        return False

    def put_bytes(
        self,
        *,
        bucket_name: str,
        object_name: str,
        payload: bytes,
        content_type: str,
        metadata: dict[str, str] | None = None,
    ) -> MinIOObjectWriteResult:
        self.objects[(bucket_name, object_name)] = payload
        return MinIOObjectWriteResult(
            bucket_name=bucket_name,
            object_name=object_name,
            etag="fixture-etag",
            version_id="fixture-v1",
        )


def build_pipeline_services(
    session: InMemoryLiveMvpSession,
) -> tuple[
    ParseCompletedProcessingService,
    UniversitySearchService,
    UniversityCardReadService,
    UniversityProvenanceReadService,
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
    parse_completed_service = ParseCompletedProcessingService(
        claim_build_service=ClaimBuildService(claim_repository),
        university_bootstrap_service=UniversityBootstrapService(bootstrap_repository),
        resolved_fact_generation_service=ResolvedFactGenerationService(facts_repository),
        university_card_projection_service=UniversityCardProjectionService(
            cards_repository,
            search_doc_service=UniversitySearchDocProjectionService(search_docs_repository),
        ),
    )
    return (
        parse_completed_service,
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


def build_parser_consumer(
    session: InMemoryLiveMvpSession,
    *,
    fixture_fetcher: FixtureFetcher,
    parse_completed_publisher: CapturingPublisher,
) -> CrawlRequestConsumer:
    raw_artifact_repository = RawArtifactRepository(session=session, sql_text=lambda value: value)
    parsed_document_repository = ParsedDocumentRepository(
        session=session,
        sql_text=lambda value: value,
    )
    processing_service = CrawlRequestProcessingService(
        fetcher=fixture_fetcher,
        raw_artifact_service=RawArtifactPersistenceService(
            raw_store=MinIORawArtifactStore(InMemoryMinIOStorage()),
            repository=raw_artifact_repository,
        ),
        parsed_document_service=ParsedDocumentPersistenceService(parsed_document_repository),
        source_adapters=(
            AggregatorAdapter(fetcher=fixture_fetcher),
            OfficialSiteAdapter(fetcher=fixture_fetcher),
        ),
        parse_completed_emitter=ParseCompletedEmitter(publisher=parse_completed_publisher),
    )
    return CrawlRequestConsumer(service=processing_service)


def load_fixture_bytes(name: str) -> bytes:
    return (FIXTURE_ROOT / name).read_bytes()


def test_live_mvp_flow_covers_bootstrap_discovery_and_search_to_provenance() -> None:
    registry_session = InMemorySourceRegistrySession()
    source_repository = SourceRepository(registry_session, sql_text=lambda value: value)
    endpoint_repository = SourceEndpointRepository(
        registry_session,
        sql_text=lambda value: value,
    )
    seed_service = LiveSourceSeedService(
        gateway=SchedulerSourceRegistryGateway(
            session=registry_session,
            source_repository=source_repository,
            endpoint_repository=endpoint_repository,
        ),
        seed_specs=build_live_mvp_source_seed_specs(),
    )

    seed_result = seed_service.bootstrap()

    assert seed_result.source_count == 3
    assert seed_result.endpoint_count == 5
    assert registry_session.commit_count == 1

    sitemap_payload = b"""
    <urlset>
      <url><loc>https://tabiturient.ru/vuzu/altgaki</loc></url>
      <url><loc>https://tabiturient.ru/vuzu/altgaki/about</loc></url>
      <url><loc>https://tabiturient.ru/globalrating/</loc></url>
    </urlset>
    """
    discovery_service = SourceEndpointDiscoveryService(
        source_repository=source_repository,
        endpoint_repository=endpoint_repository,
        fetcher=StaticDiscoveryFetcher(sitemap_payload),
    )
    discovery_result = discovery_service.materialize_discovered_endpoints(
        DiscoveryMaterializationRequest(
            discovery_run_id=uuid4(),
            source_key="tabiturient-aggregator",
            dry_run=False,
        )
    )

    assert discovery_result.discovered_total == 1
    assert discovery_result.materialized_count == 1
    assert discovery_result.items[0].endpoint_url == "https://tabiturient.ru/vuzu/altgaki"

    discovered_endpoint = endpoint_repository.get_by_url(
        "tabiturient-aggregator",
        "https://tabiturient.ru/vuzu/altgaki",
    )
    assert discovered_endpoint is not None
    assert discovered_endpoint.parser_profile == "aggregator.tabiturient.university_html"

    kubsu_endpoint = endpoint_repository.get_by_url(
        "kubsu-official",
        "https://www.kubsu.ru/ru/abiturient",
    )
    assert kubsu_endpoint is not None

    scheduler_publisher = CapturingPublisher(exchange_name="parser.jobs")
    manual_crawl_service = ManualCrawlTriggerService(
        endpoint_repository=endpoint_repository,
        run_repository=InMemoryPipelineRunRepository(),
        publisher=scheduler_publisher,
    )
    crawl_run_id = uuid4()
    accepted = manual_crawl_service.trigger_manual_crawl(
        ManualCrawlTriggerRequest(
            crawl_run_id=crawl_run_id,
            source_key="kubsu-official",
            endpoint_id=kubsu_endpoint.endpoint_id,
            priority="high",
            requested_at=datetime(2026, 5, 2, 12, 11, tzinfo=UTC),
            metadata={"requested_by": "e2e-live-mvp"},
        )
    )

    assert accepted.pipeline_run.status.value == "published"
    assert scheduler_publisher.calls[0]["queue_name"] == "parser.high"

    live_session = InMemoryLiveMvpSession(sources=registry_session.sources)
    parse_completed_publisher = CapturingPublisher(exchange_name="normalize.jobs")
    parser_consumer = build_parser_consumer(
        live_session,
        fixture_fetcher=FixtureFetcher(
            {
                "https://www.kubsu.ru/ru/abiturient": (
                    load_fixture_bytes("kubsu_abiturient_page.html"),
                    "text/html; charset=utf-8",
                ),
            }
        ),
        parse_completed_publisher=parse_completed_publisher,
    )
    parser_result = parser_consumer.handle_message(
        accepted.event.model_dump(mode="json")
    )

    assert parser_result.parsed_document is not None
    assert parser_result.parsed_document.source_key == "kubsu-official"
    assert parser_result.parsed_document.parser_profile == "official_site.kubsu.abiturient_html"
    assert parser_result.parsed_document.entity_hint == "Кубанский государственный университет"
    assert len(parser_result.extracted_fragments) == 4
    assert parse_completed_publisher.calls[0]["queue_name"] == "normalize.high"

    parse_completed_service, search_service, card_service, provenance_service = (
        build_pipeline_services(live_session)
    )
    normalizer_result = ParseCompletedConsumer(
        service=parse_completed_service
    ).handle_message(parse_completed_publisher.calls[0]["payload"])

    assert normalizer_result.projection_result.projection.university_id == (
        deterministic_university_id("kubsu-official")
    )
    assert normalizer_result.projection_result.projection.card.canonical_name.value == (
        "Кубанский государственный университет"
    )

    app.dependency_overrides[get_university_search_service] = lambda: search_service
    app.dependency_overrides[get_university_card_read_service] = lambda: card_service
    app.dependency_overrides[get_university_provenance_read_service] = (
        lambda: provenance_service
    )
    try:
        client = TestClient(app)
        search_response = client.get(
            "/api/v1/search",
            params={"query": "Кубанский", "page": 1, "page_size": 10},
        )
        assert search_response.status_code == 200
        search_body = search_response.json()
        assert search_body["total"] == 1
        assert search_body["items"][0]["canonical_name"] == (
            "Кубанский государственный университет"
        )
        assert search_body["items"][0]["website"] == "https://www.kubsu.ru"
        assert search_body["items"][0]["match_signals"] == ["full_text", "trigram"]

        university_id = search_body["items"][0]["university_id"]
        assert university_id == str(deterministic_university_id("kubsu-official"))

        card_response = client.get(f"/api/v1/universities/{university_id}")
        assert card_response.status_code == 200
        card_body = card_response.json()
        assert card_body["canonical_name"]["value"] == (
            "Кубанский государственный университет"
        )
        assert card_body["contacts"]["website"] == "https://www.kubsu.ru"
        assert card_body["field_attribution"]["canonical_name"]["source_key"] == (
            "kubsu-official"
        )
        assert card_body["source_rationale"][0]["source_key"] == "kubsu-official"

        provenance_response = client.get(
            f"/api/v1/universities/{university_id}/provenance"
        )
        assert provenance_response.status_code == 200
        provenance_body = provenance_response.json()
        assert provenance_body["university_id"] == university_id
        assert provenance_body["delivery_projection"]["card_version"] == (
            normalizer_result.projection_result.projection.card_version
        )
        assert len(provenance_body["raw_artifacts"]) == 1
        assert len(provenance_body["parsed_documents"]) == 1
        assert len(provenance_body["claims"]) == len(parser_result.extracted_fragments)
        assert len(provenance_body["claim_evidence"]) == len(parser_result.extracted_fragments)
        assert len(provenance_body["resolved_facts"]) == len(parser_result.extracted_fragments)
        assert provenance_body["raw_artifacts"][0]["source_url"] == (
            "https://www.kubsu.ru/ru/abiturient"
        )
        assert provenance_body["parsed_documents"][0]["parser_profile"] == (
            "official_site.kubsu.abiturient_html"
        )
        assert provenance_body["chain"][-1] == "delivery_projection"
    finally:
        app.dependency_overrides.clear()
