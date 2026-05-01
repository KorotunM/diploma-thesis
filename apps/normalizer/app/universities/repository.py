from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from apps.normalizer.app.claims import ClaimEvidenceRecord, ClaimRecord
from apps.normalizer.app.persistence import json_from_db, json_to_db, sql_text

from .models import (
    SourceAuthorityRecord,
    UniversityBootstrapCandidate,
    UniversityRecord,
    UniversitySimilarityCandidate,
)


class UniversityBootstrapRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def get_source(self, source_key: str) -> SourceAuthorityRecord | None:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    source_id,
                    source_key,
                    source_type,
                    trust_tier,
                    is_active,
                    metadata
                FROM ingestion.source
                WHERE source_key = :source_key
                """
            ),
            {"source_key": source_key},
        )
        row = result.mappings().one_or_none()
        if row is None:
            return None
        return self._source_from_row(row)

    def find_university_by_canonical_domain(
        self,
        canonical_domain: str,
    ) -> UniversityRecord | None:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    university_id,
                    canonical_name,
                    canonical_domain,
                    country_code,
                    city_name,
                    created_at,
                    metadata
                FROM core.university
                WHERE canonical_domain = :canonical_domain
                """
            ),
            {"canonical_domain": canonical_domain},
        )
        row = result.mappings().one_or_none()
        if row is None:
            return None
        return self._university_from_row(row)

    def find_university_by_canonical_name(
        self,
        canonical_name: str,
    ) -> UniversityRecord | None:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    university_id,
                    canonical_name,
                    canonical_domain,
                    country_code,
                    city_name,
                    created_at,
                    metadata
                FROM core.university
                WHERE canonical_name = :canonical_name
                """
            ),
            {"canonical_name": canonical_name},
        )
        row = result.mappings().one_or_none()
        if row is None:
            return None
        return self._university_from_row(row)

    def find_university_by_id(
        self,
        university_id: UUID,
    ) -> UniversityRecord | None:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    university_id,
                    canonical_name,
                    canonical_domain,
                    country_code,
                    city_name,
                    created_at,
                    metadata
                FROM core.university
                WHERE university_id = :university_id
                """
            ),
            {"university_id": university_id},
        )
        row = result.mappings().one_or_none()
        if row is None:
            return None
        return self._university_from_row(row)

    def find_universities_by_canonical_name_similarity(
        self,
        canonical_name: str,
        *,
        threshold: float,
        limit: int,
    ) -> list[UniversitySimilarityCandidate]:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    university_id,
                    canonical_name,
                    canonical_domain,
                    country_code,
                    city_name,
                    created_at,
                    metadata,
                    similarity(canonical_name::text, :canonical_name) AS similarity_score
                FROM core.university
                WHERE similarity(canonical_name::text, :canonical_name) >= :threshold
                ORDER BY similarity_score DESC, canonical_name ASC, university_id ASC
                LIMIT :limit
                """
            ),
            {
                "canonical_name": canonical_name,
                "threshold": threshold,
                "limit": limit,
            },
        )
        return [
            UniversitySimilarityCandidate(
                university=self._university_from_row(row),
                similarity_score=float(row["similarity_score"]),
            )
            for row in result.mappings().all()
        ]

    def list_claims_for_university(
        self,
        university_id: UUID,
    ) -> list[ClaimRecord]:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    claim_id,
                    parsed_document_id,
                    source_key,
                    field_name,
                    value_json,
                    entity_hint,
                    parser_version,
                    normalizer_version,
                    parser_confidence,
                    created_at,
                    metadata
                FROM normalize.claim
                WHERE claim_id::text IN (
                    SELECT jsonb_array_elements_text(
                        COALESCE(metadata -> 'claim_ids', '[]'::jsonb)
                    )
                    FROM core.university
                    WHERE university_id = :university_id
                )
                ORDER BY source_key ASC, field_name ASC, claim_id ASC
                """
            ),
            {"university_id": university_id},
        )
        return [self._claim_from_row(row) for row in result.mappings().all()]

    def list_evidence_for_university(
        self,
        university_id: UUID,
    ) -> list[ClaimEvidenceRecord]:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    evidence_id,
                    claim_id,
                    raw_artifact_id,
                    fragment_id,
                    source_key,
                    source_url,
                    captured_at,
                    metadata
                FROM normalize.claim_evidence
                WHERE claim_id::text IN (
                    SELECT jsonb_array_elements_text(
                        COALESCE(metadata -> 'claim_ids', '[]'::jsonb)
                    )
                    FROM core.university
                    WHERE university_id = :university_id
                )
                ORDER BY source_key ASC, source_url ASC, evidence_id ASC
                """
            ),
            {"university_id": university_id},
        )
        return [self._evidence_from_row(row) for row in result.mappings().all()]

    def upsert_university(
        self,
        candidate: UniversityBootstrapCandidate,
    ) -> UniversityRecord:
        result = self._session.execute(
            self._sql_text(
                """
                INSERT INTO core.university (
                    university_id,
                    canonical_name,
                    canonical_domain,
                    country_code,
                    city_name,
                    metadata
                )
                VALUES (
                    :university_id,
                    :canonical_name,
                    :canonical_domain,
                    :country_code,
                    :city_name,
                    CAST(:metadata AS jsonb)
                )
                ON CONFLICT (university_id)
                DO UPDATE SET
                    canonical_name = EXCLUDED.canonical_name,
                    canonical_domain = EXCLUDED.canonical_domain,
                    country_code = EXCLUDED.country_code,
                    city_name = EXCLUDED.city_name,
                    metadata = core.university.metadata || EXCLUDED.metadata
                RETURNING
                    university_id,
                    canonical_name,
                    canonical_domain,
                    country_code,
                    city_name,
                    created_at,
                    metadata
                """
            ),
            {
                "university_id": candidate.university_id,
                "canonical_name": candidate.canonical_name,
                "canonical_domain": candidate.canonical_domain,
                "country_code": candidate.country_code,
                "city_name": candidate.city_name,
                "metadata": json_to_db(candidate.metadata),
            },
        )
        return self._university_from_row(result.mappings().one())

    def commit(self) -> None:
        self._session.commit()

    @staticmethod
    def _source_from_row(row: Any) -> SourceAuthorityRecord:
        return SourceAuthorityRecord(
            source_id=row["source_id"],
            source_key=row["source_key"],
            source_type=row["source_type"],
            trust_tier=row["trust_tier"],
            is_active=row["is_active"],
            metadata=json_from_db(row["metadata"]),
        )

    @staticmethod
    def _claim_from_row(row: Any) -> ClaimRecord:
        value_payload = json_from_db(row["value_json"])
        return ClaimRecord(
            claim_id=row["claim_id"],
            parsed_document_id=row["parsed_document_id"],
            source_key=row["source_key"],
            field_name=row["field_name"],
            value=value_payload.get("value"),
            value_type=value_payload.get("value_type", "unknown"),
            entity_hint=row["entity_hint"],
            parser_version=row["parser_version"],
            normalizer_version=row["normalizer_version"],
            parser_confidence=row["parser_confidence"],
            created_at=row["created_at"],
            metadata=json_from_db(row["metadata"]),
        )

    @staticmethod
    def _evidence_from_row(row: Any) -> ClaimEvidenceRecord:
        return ClaimEvidenceRecord(
            evidence_id=row["evidence_id"],
            claim_id=row["claim_id"],
            source_key=row["source_key"],
            source_url=row["source_url"],
            raw_artifact_id=row["raw_artifact_id"],
            fragment_id=row["fragment_id"],
            captured_at=row["captured_at"],
            metadata=json_from_db(row["metadata"]),
        )

    @staticmethod
    def _university_from_row(row: Any) -> UniversityRecord:
        return UniversityRecord(
            university_id=row["university_id"],
            canonical_name=row["canonical_name"],
            canonical_domain=row["canonical_domain"],
            country_code=row["country_code"],
            city_name=row["city_name"],
            created_at=row["created_at"],
            metadata=json_from_db(row["metadata"]),
        )


def deterministic_university_id(source_key: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"core.university:{source_key}")
