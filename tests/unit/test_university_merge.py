from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from apps.normalizer.app.claims import (
    ClaimBuildResult,
    ClaimEvidenceRecord,
    ClaimRecord,
    ParsedDocumentSnapshot,
)
from apps.normalizer.app.persistence import json_from_db, json_to_db
from apps.normalizer.app.resolution import SourceTrustTier
from apps.normalizer.app.universities import (
    UniversityBootstrapRepository,
    UniversityBootstrapService,
    deterministic_university_id,
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


class FakeUniversityMergeSession:
    def __init__(self) -> None:
        self.created_at = datetime(2026, 4, 26, 9, 0, tzinfo=UTC)
        self.commit_count = 0
        self.sources: dict[str, dict[str, Any]] = {
            "msu-official": {
                "source_id": uuid4(),
                "source_key": "msu-official",
                "source_type": "official_site",
                "trust_tier": SourceTrustTier.AUTHORITATIVE.value,
                "is_active": True,
                "metadata": json_to_db({}),
            },
            "msu-aggregator": {
                "source_id": uuid4(),
                "source_key": "msu-aggregator",
                "source_type": "aggregator",
                "trust_tier": SourceTrustTier.TRUSTED.value,
                "is_active": True,
                "metadata": json_to_db({}),
            },
        }
        self.universities: dict[UUID, dict[str, Any]] = {}
        self.claim_rows: dict[UUID, dict[str, Any]] = {}
        self.evidence_rows: dict[UUID, dict[str, Any]] = {}

    def execute(self, statement: str, params: dict[str, Any]) -> MappingResult:
        sql = " ".join(statement.split()).lower()
        if "from ingestion.source" in sql:
            return MappingResult(row=self.sources.get(params["source_key"]))
        if "from core.university" in sql and "where canonical_domain = :canonical_domain" in sql:
            return MappingResult(row=self._find_university(params["canonical_domain"]))
        if "from normalize.claim_evidence" in sql and "from core.university" in sql:
            return MappingResult(rows=self._evidence_for_university(params["university_id"]))
        if "from normalize.claim" in sql and "from core.university" in sql:
            return MappingResult(rows=self._claims_for_university(params["university_id"]))
        if "insert into core.university" in sql:
            return MappingResult(row=self._upsert_university(params))
        raise AssertionError(f"Unexpected SQL statement: {statement}")

    def commit(self) -> None:
        self.commit_count += 1

    def _find_university(self, canonical_domain: str) -> dict[str, Any] | None:
        for row in self.universities.values():
            if row["canonical_domain"] == canonical_domain:
                return row
        return None

    def _claims_for_university(self, university_id: UUID) -> list[dict[str, Any]]:
        university = self.universities[university_id]
        claim_ids = [
            UUID(claim_id) for claim_id in json_from_db(university["metadata"])["claim_ids"]
        ]
        return [self.claim_rows[claim_id] for claim_id in claim_ids]

    def _evidence_for_university(self, university_id: UUID) -> list[dict[str, Any]]:
        university = self.universities[university_id]
        claim_ids = {
            UUID(claim_id) for claim_id in json_from_db(university["metadata"])["claim_ids"]
        }
        return [
            row
            for row in self.evidence_rows.values()
            if row["claim_id"] in claim_ids
        ]

    def _upsert_university(self, params: dict[str, Any]) -> dict[str, Any]:
        existing = self.universities.get(params["university_id"])
        row = {
            **params,
            "created_at": existing["created_at"] if existing else self.created_at,
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


def build_claim_result(
    *,
    source_key: str,
    parser_profile: str,
    parser_version: str,
    canonical_name: str,
    website: str,
    city: str,
    country_code: str,
) -> ClaimBuildResult:
    parsed_document_id = uuid4()
    raw_artifact_id = uuid4()
    parsed_document = ParsedDocumentSnapshot(
        parsed_document_id=parsed_document_id,
        crawl_run_id=uuid4(),
        raw_artifact_id=raw_artifact_id,
        source_key=source_key,
        parser_profile=parser_profile,
        parser_version=parser_version,
        entity_type="university",
        entity_hint=canonical_name,
        parsed_at=datetime(2026, 4, 26, 8, 55, tzinfo=UTC),
        metadata={},
    )
    claim_specs = [
        ("canonical_name", canonical_name, 0.91),
        ("contacts.website", website, 0.89),
        ("location.city", city, 0.87),
        ("location.country_code", country_code, 0.85),
    ]
    claims: list[ClaimRecord] = []
    evidence: list[ClaimEvidenceRecord] = []
    for index, (field_name, value, confidence) in enumerate(claim_specs):
        claim = ClaimRecord(
            claim_id=uuid4(),
            parsed_document_id=parsed_document_id,
            source_key=source_key,
            field_name=field_name,
            value=value,
            value_type="str",
            entity_hint=canonical_name,
            parser_version=parser_version,
            normalizer_version="normalizer.0.1.0",
            parser_confidence=confidence,
            created_at=datetime(2026, 4, 26, 9, 0, tzinfo=UTC),
            metadata={"fragment_id": str(uuid4())},
        )
        claims.append(claim)
        evidence.append(
            ClaimEvidenceRecord(
                evidence_id=uuid4(),
                claim_id=claim.claim_id,
                source_key=source_key,
                source_url=website,
                raw_artifact_id=raw_artifact_id,
                fragment_id=uuid4(),
                captured_at=datetime(2026, 4, 26, 8, 50, tzinfo=UTC),
                metadata={"field_name": field_name, "position": index},
            )
        )
    return ClaimBuildResult(
        parsed_document=parsed_document,
        claims=claims,
        evidence=evidence,
    )


def register_claim_result(
    session: FakeUniversityMergeSession,
    claim_result: ClaimBuildResult,
) -> None:
    for claim in claim_result.claims:
        session.claim_rows[claim.claim_id] = {
            "claim_id": claim.claim_id,
            "parsed_document_id": claim.parsed_document_id,
            "source_key": claim.source_key,
            "field_name": claim.field_name,
            "value_json": json_to_db({"value": claim.value, "value_type": claim.value_type}),
            "entity_hint": claim.entity_hint,
            "parser_version": claim.parser_version,
            "normalizer_version": claim.normalizer_version,
            "parser_confidence": claim.parser_confidence,
            "created_at": claim.created_at,
            "metadata": json_to_db(claim.metadata),
        }
    for record in claim_result.evidence:
        session.evidence_rows[record.evidence_id] = {
            "evidence_id": record.evidence_id,
            "claim_id": record.claim_id,
            "raw_artifact_id": record.raw_artifact_id,
            "fragment_id": record.fragment_id,
            "source_key": record.source_key,
            "source_url": record.source_url,
            "captured_at": record.captured_at,
            "metadata": json_to_db(record.metadata),
        }


def build_service(session: FakeUniversityMergeSession) -> UniversityBootstrapService:
    return UniversityBootstrapService(
        UniversityBootstrapRepository(session=session, sql_text=lambda value: value)
    )


def test_secondary_aggregator_claims_merge_into_existing_authoritative_university() -> None:
    session = FakeUniversityMergeSession()
    service = build_service(session)
    official = build_claim_result(
        source_key="msu-official",
        parser_profile="official_site.default",
        parser_version="official.0.1.0",
        canonical_name="Example University",
        website="https://www.example.edu/admissions",
        city="Moscow",
        country_code="RU",
    )
    aggregator = build_claim_result(
        source_key="msu-aggregator",
        parser_profile="aggregator.default",
        parser_version="aggregator.0.1.0",
        canonical_name="Example University Directory",
        website="https://example.edu",
        city="Moscow City",
        country_code="RU",
    )
    register_claim_result(session, official)
    register_claim_result(session, aggregator)

    first = service.bootstrap_single_source_authoritative(official)
    merged = service.consolidate_claims(aggregator)

    assert first.university.university_id == deterministic_university_id("msu-official")
    assert merged.university.university_id == first.university.university_id
    assert merged.source.source_key == "msu-official"
    assert [source.source_key for source in merged.sources_used] == [
        "msu-aggregator",
        "msu-official",
    ]
    assert {claim.source_key for claim in merged.claims_used} == {
        "msu-official",
        "msu-aggregator",
    }
    assert len(merged.claims_used) == 8
    assert len(merged.evidence_used) == 8
    assert merged.university.canonical_domain == "example.edu"
    assert merged.university.metadata["merge_strategy"] == (
        "authoritative_anchor_exact_domain_merge"
    )
    assert merged.university.metadata["matched_by"] == "canonical_domain"
    assert merged.university.metadata["source_key"] == "msu-official"
    assert merged.university.metadata["source_keys"] == [
        "msu-aggregator",
        "msu-official",
    ]
    assert len(merged.university.metadata["claim_ids"]) == 8
    snapshots = {
        snapshot["source_key"]: snapshot
        for snapshot in merged.university.metadata["source_snapshots"]
    }
    assert set(snapshots) == {"msu-official", "msu-aggregator"}
    assert snapshots["msu-official"]["trust_tier"] == SourceTrustTier.AUTHORITATIVE.value
    assert snapshots["msu-aggregator"]["trust_tier"] == SourceTrustTier.TRUSTED.value
    assert session.commit_count == 2
