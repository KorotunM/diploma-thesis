from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from apps.backend.app.cards import UniversityCardReadRepository, UniversityCardReadService
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
from apps.normalizer.app.resolution import (
    CANONICAL_FIELD_POLICY,
    FieldResolutionPolicyMatrix,
    SourceTrustTier,
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


class InMemoryDualSourceSession:
    def __init__(self) -> None:
        self.now = datetime(2026, 4, 26, 14, 0, tzinfo=UTC)
        self.commit_count = 0
        self.sources: dict[str, dict[str, Any]] = {
            "msu-official": {
                "source_id": uuid4(),
                "source_key": "msu-official",
                "source_type": "official_site",
                "trust_tier": SourceTrustTier.AUTHORITATIVE.value,
                "is_active": True,
                "metadata": json_to_db({"owner": "admissions"}),
            },
            "msu-aggregator": {
                "source_id": uuid4(),
                "source_key": "msu-aggregator",
                "source_type": "aggregator",
                "trust_tier": SourceTrustTier.TRUSTED.value,
                "is_active": True,
                "metadata": json_to_db({"provider": "example-directory"}),
            },
        }
        self.parsed_documents: dict[UUID, dict[str, Any]] = {}
        self.fragments_by_document: dict[UUID, list[dict[str, Any]]] = {}
        self.claims: dict[UUID, dict[str, Any]] = {}
        self.evidence: dict[UUID, dict[str, Any]] = {}
        self.universities: dict[UUID, dict[str, Any]] = {}
        self.resolved_facts: dict[UUID, dict[str, Any]] = {}
        self.card_versions: dict[tuple[UUID, int], dict[str, Any]] = {}
        self.delivery_cards: dict[tuple[UUID, int], dict[str, Any]] = {}
        self.seed_documents()

    def seed_documents(self) -> None:
        official_document_id = uuid4()
        official_raw_artifact_id = uuid4()
        official_row = {
            "parsed_document_id": official_document_id,
            "crawl_run_id": uuid4(),
            "raw_artifact_id": official_raw_artifact_id,
            "source_key": "msu-official",
            "parser_profile": "official_site.default",
            "parser_version": "official.0.1.0",
            "entity_type": "university",
            "entity_hint": "Example University",
            "parsed_at": datetime(2026, 4, 26, 13, 0, tzinfo=UTC),
            "metadata": json_to_db({"adapter": "official_sites"}),
        }
        self.parsed_documents[official_document_id] = official_row
        self.fragments_by_document[official_document_id] = [
            self._fragment_row(
                parsed_document_id=official_document_id,
                raw_artifact_id=official_raw_artifact_id,
                source_key="msu-official",
                source_url="https://www.example.edu/admissions",
                field_name="canonical_name",
                value="Example University",
                value_type="str",
                locator="h1",
                confidence=0.91,
            ),
            self._fragment_row(
                parsed_document_id=official_document_id,
                raw_artifact_id=official_raw_artifact_id,
                source_key="msu-official",
                source_url="https://www.example.edu/admissions",
                field_name="contacts.website",
                value="https://www.example.edu/admissions",
                value_type="str",
                locator="a.site-link",
                confidence=0.87,
            ),
            self._fragment_row(
                parsed_document_id=official_document_id,
                raw_artifact_id=official_raw_artifact_id,
                source_key="msu-official",
                source_url="https://www.example.edu/admissions",
                field_name="location.city",
                value="Moscow",
                value_type="str",
                locator=".city",
                confidence=0.84,
            ),
            self._fragment_row(
                parsed_document_id=official_document_id,
                raw_artifact_id=official_raw_artifact_id,
                source_key="msu-official",
                source_url="https://www.example.edu/admissions",
                field_name="location.country_code",
                value="RU",
                value_type="str",
                locator=".country",
                confidence=0.83,
            ),
        ]

        aggregator_document_id = uuid4()
        aggregator_raw_artifact_id = uuid4()
        aggregator_row = {
            "parsed_document_id": aggregator_document_id,
            "crawl_run_id": uuid4(),
            "raw_artifact_id": aggregator_raw_artifact_id,
            "source_key": "msu-aggregator",
            "parser_profile": "aggregator.default",
            "parser_version": "aggregator.0.1.0",
            "entity_type": "university",
            "entity_hint": "Example University Directory",
            "parsed_at": datetime(2026, 4, 26, 13, 10, tzinfo=UTC),
            "metadata": json_to_db({"adapter": "aggregators"}),
        }
        self.parsed_documents[aggregator_document_id] = aggregator_row
        self.fragments_by_document[aggregator_document_id] = [
            self._fragment_row(
                parsed_document_id=aggregator_document_id,
                raw_artifact_id=aggregator_raw_artifact_id,
                source_key="msu-aggregator",
                source_url="https://directory.example.com/universities/example",
                field_name="canonical_name",
                value="Example University Directory",
                value_type="str",
                locator="$.name",
                confidence=0.99,
            ),
            self._fragment_row(
                parsed_document_id=aggregator_document_id,
                raw_artifact_id=aggregator_raw_artifact_id,
                source_key="msu-aggregator",
                source_url="https://directory.example.com/universities/example",
                field_name="contacts.website",
                value="https://example.edu",
                value_type="str",
                locator="$.website",
                confidence=0.98,
            ),
            self._fragment_row(
                parsed_document_id=aggregator_document_id,
                raw_artifact_id=aggregator_raw_artifact_id,
                source_key="msu-aggregator",
                source_url="https://directory.example.com/universities/example",
                field_name="location.city",
                value="Moscow City",
                value_type="str",
                locator="$.city",
                confidence=0.97,
            ),
            self._fragment_row(
                parsed_document_id=aggregator_document_id,
                raw_artifact_id=aggregator_raw_artifact_id,
                source_key="msu-aggregator",
                source_url="https://directory.example.com/universities/example",
                field_name="location.country_code",
                value="RU",
                value_type="str",
                locator="$.country_code",
                confidence=0.96,
            ),
            self._fragment_row(
                parsed_document_id=aggregator_document_id,
                raw_artifact_id=aggregator_raw_artifact_id,
                source_key="msu-aggregator",
                source_url="https://directory.example.com/universities/example",
                field_name="aliases",
                value=["Example U"],
                value_type="list",
                locator="$.aliases[0]",
                confidence=0.95,
            ),
        ]

    @staticmethod
    def _fragment_row(
        *,
        parsed_document_id: UUID,
        raw_artifact_id: UUID,
        source_key: str,
        source_url: str,
        field_name: str,
        value,
        value_type: str,
        locator: str,
        confidence: float,
    ) -> dict[str, Any]:
        return {
            "fragment_id": uuid4(),
            "parsed_document_id": parsed_document_id,
            "raw_artifact_id": raw_artifact_id,
            "source_key": source_key,
            "source_url": source_url,
            "captured_at": datetime(2026, 4, 26, 12, 55, tzinfo=UTC),
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
        if "from parsing.extracted_fragment" in sql and "join ingestion.raw_artifact as ra" in sql:
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
        if "insert into core.university" in sql:
            return MappingResult(row=self._upsert_university(params))
        if "insert into core.resolved_fact" in sql:
            return MappingResult(row=self._upsert_resolved_fact(params))
        if "insert into core.card_version" in sql:
            return MappingResult(row=self._upsert_card_version(params))
        if "insert into delivery.university_card" in sql:
            return MappingResult(row=self._upsert_delivery_card(params))
        if "from delivery.university_card" in sql and "order by card_version desc" in sql:
            return MappingResult(row=self._latest_delivery_card(params["university_id"]))
        if "from core.resolved_fact" in sql and "card_version = :card_version" in sql:
            return MappingResult(
                rows=self._list_resolved_facts(
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

    def _latest_delivery_card(self, university_id: UUID) -> dict[str, Any] | None:
        candidates = [
            row for (candidate_university_id, _), row in self.delivery_cards.items()
            if candidate_university_id == university_id
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda row: row["card_version"])

    def _list_resolved_facts(
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


def build_services(session: InMemoryDualSourceSession) -> tuple[
    ClaimBuildService,
    UniversityBootstrapService,
    ResolvedFactGenerationService,
    UniversityCardProjectionService,
    UniversityCardReadService,
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
    backend_repository = UniversityCardReadRepository(
        session=session,
        sql_text=lambda value: value,
    )
    policy_matrix = FieldResolutionPolicyMatrix()
    return (
        ClaimBuildService(claim_repository),
        UniversityBootstrapService(
            bootstrap_repository,
            policy_matrix=policy_matrix,
        ),
        ResolvedFactGenerationService(
            facts_repository,
            policy_matrix=policy_matrix,
        ),
        UniversityCardProjectionService(cards_repository),
        UniversityCardReadService(backend_repository),
    )


def payload_for(
    session: InMemoryDualSourceSession,
    *,
    source_key: str,
) -> NormalizeRequestPayload:
    parsed_document = next(
        row for row in session.parsed_documents.values() if row["source_key"] == source_key
    )
    return NormalizeRequestPayload(
        crawl_run_id=parsed_document["crawl_run_id"],
        source_key=source_key,
        parsed_document_id=parsed_document["parsed_document_id"],
        parser_version=parsed_document["parser_version"],
        normalizer_version="normalizer.0.1.0",
    )


def test_dual_source_merge_prefers_authoritative_claims_and_exposes_rationale() -> None:
    session = InMemoryDualSourceSession()
    (
        claim_service,
        bootstrap_service,
        fact_service,
        projection_service,
        backend_card_service,
    ) = build_services(session)

    official_claims = claim_service.build_claims_from_extracted_fragments(
        payload_for(session, source_key="msu-official")
    )
    official_bootstrap = bootstrap_service.consolidate_claims(official_claims)
    aggregator_claims = claim_service.build_claims_from_extracted_fragments(
        payload_for(session, source_key="msu-aggregator")
    )
    merged_bootstrap = bootstrap_service.consolidate_claims(aggregator_claims)
    fact_result = fact_service.generate_for_bootstrap(merged_bootstrap)
    projection_result = projection_service.create_projection(fact_result)
    card_response = backend_card_service.get_latest_card(
        projection_result.projection.university_id
    )

    assert official_bootstrap.university.university_id == deterministic_university_id(
        "msu-official"
    )
    assert merged_bootstrap.university.university_id == official_bootstrap.university.university_id
    assert {source.source_key for source in merged_bootstrap.sources_used} == {
        "msu-official",
        "msu-aggregator",
    }
    assert merged_bootstrap.university.metadata["merge_strategy"] == (
        "authoritative_anchor_exact_match_merge"
    )
    assert merged_bootstrap.university.metadata["match_strategy"] == "exact"
    assert merged_bootstrap.university.metadata["matched_by"] == "canonical_domain"
    assert merged_bootstrap.university.metadata["matched_value"] == "example.edu"

    facts_by_field = {fact.field_name: fact for fact in fact_result.facts}
    assert facts_by_field["canonical_name"].value == "Example University"
    assert facts_by_field["contacts.website"].value == "https://www.example.edu/admissions"
    assert facts_by_field["location.city"].value == "Moscow"
    assert facts_by_field["canonical_name"].resolution_policy == CANONICAL_FIELD_POLICY
    assert facts_by_field["canonical_name"].metadata["source_key"] == "msu-official"
    assert facts_by_field["canonical_name"].metadata["source_trust_tier"] == (
        SourceTrustTier.AUTHORITATIVE.value
    )
    assert facts_by_field["canonical_name"].metadata["source_urls"] == [
        "https://www.example.edu/admissions"
    ]

    assert card_response.university_id == projection_result.projection.university_id
    assert card_response.canonical_name.value == "Example University"
    assert card_response.contacts.website == "https://www.example.edu/admissions"
    assert card_response.location.city == "Moscow"
    assert card_response.field_attribution["canonical_name"].source_key == "msu-official"
    assert card_response.field_attribution["canonical_name"].source_trust_tier == (
        SourceTrustTier.AUTHORITATIVE.value
    )
    assert "winner=msu-official" in card_response.field_attribution["canonical_name"].rationale
    assert "contenders=" in card_response.field_attribution["canonical_name"].rationale
    assert "msu-official" in card_response.field_attribution["canonical_name"].rationale
    assert "msu-aggregator" in card_response.field_attribution["canonical_name"].rationale
    assert card_response.source_rationale[0].source_key == "msu-official"
    assert card_response.source_rationale[0].selected_fields == [
        "canonical_name",
        "contacts.website",
        "location.city",
        "location.country_code",
    ]
    assert session.commit_count == 6
