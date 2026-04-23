from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from apps.backend.app.persistence import json_from_db, sql_text
from libs.domain.university import UniversityCard

from .models import (
    ClaimEvidenceTrace,
    ClaimTrace,
    DeliveryProjectionTrace,
    ParsedDocumentTrace,
    RawArtifactTrace,
    ResolvedFactTrace,
)


class UniversityProvenanceRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def get_latest_projection_context(
        self,
        university_id: UUID,
    ) -> DeliveryProjectionTrace | None:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    projection.university_id,
                    projection.card_version,
                    projection.card_json,
                    projection.generated_at AS projection_generated_at,
                    card_version.generated_at AS card_generated_at,
                    card_version.normalizer_version
                FROM delivery.university_card AS projection
                LEFT JOIN core.card_version AS card_version
                    ON card_version.university_id = projection.university_id
                    AND card_version.card_version = projection.card_version
                WHERE projection.university_id = :university_id
                ORDER BY projection.card_version DESC
                LIMIT 1
                """
            ),
            {"university_id": university_id},
        )
        row = result.mappings().one_or_none()
        if row is None:
            return None
        return self._projection_from_row(row)

    def list_resolved_facts(
        self,
        *,
        university_id: UUID,
        card_version: int,
    ) -> list[ResolvedFactTrace]:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    resolved_fact_id,
                    university_id,
                    field_name,
                    value_json,
                    fact_score,
                    resolution_policy,
                    card_version,
                    resolved_at,
                    metadata
                FROM core.resolved_fact
                WHERE university_id = :university_id
                  AND card_version = :card_version
                ORDER BY field_name ASC, resolved_fact_id ASC
                """
            ),
            {
                "university_id": university_id,
                "card_version": card_version,
            },
        )
        return [self._resolved_fact_from_row(row) for row in result.mappings().all()]

    def list_claims_for_card(
        self,
        *,
        university_id: UUID,
        card_version: int,
    ) -> list[ClaimTrace]:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT DISTINCT
                    claim.claim_id,
                    claim.parsed_document_id,
                    claim.source_key,
                    claim.field_name,
                    claim.value_json,
                    claim.entity_hint,
                    claim.parser_version,
                    claim.normalizer_version,
                    claim.parser_confidence,
                    claim.created_at,
                    claim.metadata
                FROM core.resolved_fact AS fact
                CROSS JOIN LATERAL jsonb_array_elements_text(
                    COALESCE(fact.metadata -> 'selected_claim_ids', '[]'::jsonb)
                ) AS selected_claim(claim_id)
                JOIN normalize.claim AS claim
                    ON claim.claim_id = selected_claim.claim_id::uuid
                WHERE fact.university_id = :university_id
                  AND fact.card_version = :card_version
                ORDER BY claim.created_at ASC, claim.claim_id ASC
                """
            ),
            {
                "university_id": university_id,
                "card_version": card_version,
            },
        )
        return [self._claim_from_row(row) for row in result.mappings().all()]

    def list_claim_evidence_for_card(
        self,
        *,
        university_id: UUID,
        card_version: int,
    ) -> list[ClaimEvidenceTrace]:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT DISTINCT
                    evidence.evidence_id,
                    evidence.claim_id,
                    evidence.raw_artifact_id,
                    evidence.fragment_id,
                    evidence.source_key,
                    evidence.source_url,
                    evidence.captured_at,
                    evidence.metadata
                FROM core.resolved_fact AS fact
                CROSS JOIN LATERAL jsonb_array_elements_text(
                    COALESCE(fact.metadata -> 'selected_evidence_ids', '[]'::jsonb)
                ) AS selected_evidence(evidence_id)
                JOIN normalize.claim_evidence AS evidence
                    ON evidence.evidence_id = selected_evidence.evidence_id::uuid
                WHERE fact.university_id = :university_id
                  AND fact.card_version = :card_version
                ORDER BY evidence.captured_at ASC, evidence.evidence_id ASC
                """
            ),
            {
                "university_id": university_id,
                "card_version": card_version,
            },
        )
        return [self._claim_evidence_from_row(row) for row in result.mappings().all()]

    def list_parsed_documents_for_card(
        self,
        *,
        university_id: UUID,
        card_version: int,
    ) -> list[ParsedDocumentTrace]:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT DISTINCT
                    document.parsed_document_id,
                    document.crawl_run_id,
                    document.raw_artifact_id,
                    document.source_key,
                    document.parser_profile,
                    document.parser_version,
                    document.entity_type,
                    document.entity_hint,
                    document.extracted_fragment_count,
                    document.parsed_at,
                    document.metadata
                FROM core.resolved_fact AS fact
                CROSS JOIN LATERAL jsonb_array_elements_text(
                    COALESCE(fact.metadata -> 'selected_claim_ids', '[]'::jsonb)
                ) AS selected_claim(claim_id)
                JOIN normalize.claim AS claim
                    ON claim.claim_id = selected_claim.claim_id::uuid
                JOIN parsing.parsed_document AS document
                    ON document.parsed_document_id = claim.parsed_document_id
                WHERE fact.university_id = :university_id
                  AND fact.card_version = :card_version
                ORDER BY document.parsed_at ASC, document.parsed_document_id ASC
                """
            ),
            {
                "university_id": university_id,
                "card_version": card_version,
            },
        )
        return [self._parsed_document_from_row(row) for row in result.mappings().all()]

    def list_raw_artifacts_for_card(
        self,
        *,
        university_id: UUID,
        card_version: int,
    ) -> list[RawArtifactTrace]:
        result = self._session.execute(
            self._sql_text(
                """
                WITH selected_claim_documents AS (
                    SELECT DISTINCT document.raw_artifact_id
                    FROM core.resolved_fact AS fact
                    CROSS JOIN LATERAL jsonb_array_elements_text(
                        COALESCE(fact.metadata -> 'selected_claim_ids', '[]'::jsonb)
                    ) AS selected_claim(claim_id)
                    JOIN normalize.claim AS claim
                        ON claim.claim_id = selected_claim.claim_id::uuid
                    JOIN parsing.parsed_document AS document
                        ON document.parsed_document_id = claim.parsed_document_id
                    WHERE fact.university_id = :university_id
                      AND fact.card_version = :card_version
                ),
                selected_evidence_artifacts AS (
                    SELECT DISTINCT evidence.raw_artifact_id
                    FROM core.resolved_fact AS fact
                    CROSS JOIN LATERAL jsonb_array_elements_text(
                        COALESCE(fact.metadata -> 'selected_evidence_ids', '[]'::jsonb)
                    ) AS selected_evidence(evidence_id)
                    JOIN normalize.claim_evidence AS evidence
                        ON evidence.evidence_id = selected_evidence.evidence_id::uuid
                    WHERE fact.university_id = :university_id
                      AND fact.card_version = :card_version
                ),
                stitched_artifacts AS (
                    SELECT raw_artifact_id FROM selected_claim_documents
                    UNION
                    SELECT raw_artifact_id FROM selected_evidence_artifacts
                )
                SELECT
                    artifact.raw_artifact_id,
                    artifact.crawl_run_id,
                    artifact.source_key,
                    artifact.source_url,
                    artifact.final_url,
                    artifact.http_status,
                    artifact.content_type,
                    artifact.content_length,
                    artifact.sha256,
                    artifact.storage_bucket,
                    artifact.storage_object_key,
                    artifact.etag,
                    artifact.last_modified,
                    artifact.fetched_at,
                    artifact.metadata
                FROM stitched_artifacts
                JOIN ingestion.raw_artifact AS artifact
                    ON artifact.raw_artifact_id = stitched_artifacts.raw_artifact_id
                ORDER BY artifact.fetched_at ASC, artifact.raw_artifact_id ASC
                """
            ),
            {
                "university_id": university_id,
                "card_version": card_version,
            },
        )
        return [self._raw_artifact_from_row(row) for row in result.mappings().all()]

    @staticmethod
    def _projection_from_row(row: Any) -> DeliveryProjectionTrace:
        return DeliveryProjectionTrace(
            university_id=row["university_id"],
            card_version=row["card_version"],
            card=UniversityCard.model_validate(json_from_db(row["card_json"])),
            projection_generated_at=row["projection_generated_at"],
            card_generated_at=row["card_generated_at"],
            normalizer_version=row["normalizer_version"],
        )

    @staticmethod
    def _resolved_fact_from_row(row: Any) -> ResolvedFactTrace:
        value_payload = json_from_db(row["value_json"])
        metadata = json_from_db(row["metadata"])
        return ResolvedFactTrace(
            resolved_fact_id=row["resolved_fact_id"],
            university_id=row["university_id"],
            field_name=row["field_name"],
            value=value_payload.get("value"),
            value_type=value_payload.get("value_type", "unknown"),
            fact_score=row["fact_score"],
            resolution_policy=row["resolution_policy"],
            selected_claim_ids=_uuid_list(metadata.get("selected_claim_ids")),
            selected_evidence_ids=_uuid_list(metadata.get("selected_evidence_ids")),
            card_version=row["card_version"],
            resolved_at=row["resolved_at"],
            metadata=metadata,
        )

    @staticmethod
    def _claim_from_row(row: Any) -> ClaimTrace:
        value_payload = json_from_db(row["value_json"])
        return ClaimTrace(
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
    def _claim_evidence_from_row(row: Any) -> ClaimEvidenceTrace:
        return ClaimEvidenceTrace(
            evidence_id=row["evidence_id"],
            claim_id=row["claim_id"],
            raw_artifact_id=row["raw_artifact_id"],
            fragment_id=row["fragment_id"],
            source_key=row["source_key"],
            source_url=row["source_url"],
            captured_at=row["captured_at"],
            metadata=json_from_db(row["metadata"]),
        )

    @staticmethod
    def _parsed_document_from_row(row: Any) -> ParsedDocumentTrace:
        return ParsedDocumentTrace(
            parsed_document_id=row["parsed_document_id"],
            crawl_run_id=row["crawl_run_id"],
            raw_artifact_id=row["raw_artifact_id"],
            source_key=row["source_key"],
            parser_profile=row["parser_profile"],
            parser_version=row["parser_version"],
            entity_type=row["entity_type"],
            entity_hint=row["entity_hint"],
            extracted_fragment_count=row["extracted_fragment_count"],
            parsed_at=row["parsed_at"],
            metadata=json_from_db(row["metadata"]),
        )

    @staticmethod
    def _raw_artifact_from_row(row: Any) -> RawArtifactTrace:
        return RawArtifactTrace(
            raw_artifact_id=row["raw_artifact_id"],
            crawl_run_id=row["crawl_run_id"],
            source_key=row["source_key"],
            source_url=row["source_url"],
            final_url=row["final_url"],
            http_status=row["http_status"],
            content_type=row["content_type"],
            content_length=row["content_length"],
            sha256=row["sha256"],
            storage_bucket=row["storage_bucket"],
            storage_object_key=row["storage_object_key"],
            etag=row["etag"],
            last_modified=row["last_modified"],
            fetched_at=row["fetched_at"],
            metadata=json_from_db(row["metadata"]),
        )


def _uuid_list(values: Any) -> list[UUID]:
    if not isinstance(values, list):
        return []
    parsed: list[UUID] = []
    for value in values:
        if isinstance(value, UUID):
            parsed.append(value)
        elif isinstance(value, str):
            parsed.append(UUID(value))
    return parsed
