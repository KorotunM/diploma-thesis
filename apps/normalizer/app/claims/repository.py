from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from apps.normalizer.app.persistence import json_from_db, json_to_db, sql_text

from .models import (
    ClaimEvidenceRecord,
    ClaimRecord,
    ExtractedFragmentSnapshot,
    ParsedDocumentSnapshot,
)


class ClaimBuildRepositoryError(ValueError):
    pass


class ClaimBuildRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def get_parsed_document(
        self,
        parsed_document_id: UUID,
    ) -> ParsedDocumentSnapshot | None:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    parsed_document_id,
                    crawl_run_id,
                    raw_artifact_id,
                    source_key,
                    parser_profile,
                    parser_version,
                    entity_type,
                    entity_hint,
                    parsed_at,
                    metadata
                FROM parsing.parsed_document
                WHERE parsed_document_id = :parsed_document_id
                """
            ),
            {"parsed_document_id": parsed_document_id},
        )
        row = result.mappings().one_or_none()
        if row is None:
            return None
        return self._document_from_row(row)

    def list_extracted_fragments(
        self,
        parsed_document_id: UUID,
    ) -> list[ExtractedFragmentSnapshot]:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    ef.fragment_id,
                    ef.parsed_document_id,
                    ef.raw_artifact_id,
                    ef.source_key,
                    ra.source_url,
                    ra.fetched_at AS captured_at,
                    ef.field_name,
                    ef.value,
                    ef.value_type,
                    ef.locator,
                    ef.confidence,
                    ef.metadata
                FROM parsing.extracted_fragment
                AS ef
                JOIN ingestion.raw_artifact AS ra
                    ON ra.raw_artifact_id = ef.raw_artifact_id
                WHERE ef.parsed_document_id = :parsed_document_id
                ORDER BY ef.field_name ASC, ef.fragment_id ASC
                """
            ),
            {"parsed_document_id": parsed_document_id},
        )
        return [self._fragment_from_row(row) for row in result.mappings().all()]

    def upsert_claim_evidence(
        self,
        *,
        claims: Sequence[ClaimRecord],
        fragments: Sequence[ExtractedFragmentSnapshot],
    ) -> list[ClaimEvidenceRecord]:
        fragments_by_id = {fragment.fragment_id: fragment for fragment in fragments}
        records: list[ClaimEvidenceRecord] = []
        for claim in claims:
            fragment = self._fragment_for_claim(
                claim=claim,
                fragments_by_id=fragments_by_id,
            )
            evidence_id = deterministic_evidence_id(
                claim_id=claim.claim_id,
                raw_artifact_id=fragment.raw_artifact_id,
                fragment_id=fragment.fragment_id,
            )
            result = self._session.execute(
                self._sql_text(
                    """
                    INSERT INTO normalize.claim_evidence (
                        evidence_id,
                        claim_id,
                        raw_artifact_id,
                        fragment_id,
                        source_key,
                        source_url,
                        captured_at,
                        metadata
                    )
                    VALUES (
                        :evidence_id,
                        :claim_id,
                        :raw_artifact_id,
                        :fragment_id,
                        :source_key,
                        :source_url,
                        :captured_at,
                        CAST(:metadata AS jsonb)
                    )
                    ON CONFLICT (evidence_id)
                    DO UPDATE SET
                        claim_id = EXCLUDED.claim_id,
                        raw_artifact_id = EXCLUDED.raw_artifact_id,
                        fragment_id = EXCLUDED.fragment_id,
                        source_key = EXCLUDED.source_key,
                        source_url = EXCLUDED.source_url,
                        captured_at = EXCLUDED.captured_at,
                        metadata = normalize.claim_evidence.metadata || EXCLUDED.metadata
                    RETURNING
                        evidence_id,
                        claim_id,
                        raw_artifact_id,
                        fragment_id,
                        source_key,
                        source_url,
                        captured_at,
                        metadata
                    """
                ),
                {
                    "evidence_id": evidence_id,
                    "claim_id": claim.claim_id,
                    "raw_artifact_id": fragment.raw_artifact_id,
                    "fragment_id": fragment.fragment_id,
                    "source_key": fragment.source_key,
                    "source_url": fragment.source_url,
                    "captured_at": fragment.captured_at,
                    "metadata": json_to_db(
                        {
                            "parsed_document_id": str(claim.parsed_document_id),
                            "field_name": claim.field_name,
                            "locator": fragment.locator,
                        }
                    ),
                },
            )
            records.append(self._evidence_from_row(result.mappings().one()))
        return records

    def upsert_claims_from_fragments(
        self,
        *,
        parsed_document: ParsedDocumentSnapshot,
        fragments: Sequence[ExtractedFragmentSnapshot],
        normalizer_version: str,
    ) -> list[ClaimRecord]:
        records: list[ClaimRecord] = []
        for fragment in fragments:
            claim_id = deterministic_claim_id(
                parsed_document_id=parsed_document.parsed_document_id,
                fragment_id=fragment.fragment_id,
                normalizer_version=normalizer_version,
            )
            result = self._session.execute(
                self._sql_text(
                    """
                    INSERT INTO normalize.claim (
                        claim_id,
                        parsed_document_id,
                        source_key,
                        field_name,
                        value_json,
                        entity_hint,
                        parser_version,
                        normalizer_version,
                        parser_confidence,
                        metadata
                    )
                    VALUES (
                        :claim_id,
                        :parsed_document_id,
                        :source_key,
                        :field_name,
                        CAST(:value_json AS jsonb),
                        :entity_hint,
                        :parser_version,
                        :normalizer_version,
                        :parser_confidence,
                        CAST(:metadata AS jsonb)
                    )
                    ON CONFLICT (claim_id)
                    DO UPDATE SET
                        parsed_document_id = EXCLUDED.parsed_document_id,
                        source_key = EXCLUDED.source_key,
                        field_name = EXCLUDED.field_name,
                        value_json = EXCLUDED.value_json,
                        entity_hint = EXCLUDED.entity_hint,
                        parser_version = EXCLUDED.parser_version,
                        normalizer_version = EXCLUDED.normalizer_version,
                        parser_confidence = EXCLUDED.parser_confidence,
                        metadata = normalize.claim.metadata || EXCLUDED.metadata
                    RETURNING
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
                    """
                ),
                {
                    "claim_id": claim_id,
                    "parsed_document_id": parsed_document.parsed_document_id,
                    "source_key": fragment.source_key,
                    "field_name": fragment.field_name,
                    "value_json": json_to_db(
                        {
                            "value": fragment.value,
                            "value_type": fragment.value_type,
                        }
                    ),
                    "entity_hint": parsed_document.entity_hint,
                    "parser_version": parsed_document.parser_version,
                    "normalizer_version": normalizer_version,
                    "parser_confidence": fragment.confidence,
                    "metadata": json_to_db(
                        self._claim_metadata(
                            parsed_document=parsed_document,
                            fragment=fragment,
                        )
                    ),
                },
            )
            records.append(self._claim_from_row(result.mappings().one()))
        return records

    def commit(self) -> None:
        self._session.commit()

    @staticmethod
    def _claim_metadata(
        *,
        parsed_document: ParsedDocumentSnapshot,
        fragment: ExtractedFragmentSnapshot,
    ) -> dict[str, Any]:
        return {
            "entity_type": parsed_document.entity_type,
            "fragment_id": str(fragment.fragment_id),
            "raw_artifact_id": str(fragment.raw_artifact_id),
            "locator": fragment.locator,
            "fragment_metadata": fragment.metadata,
            "parser_profile": parsed_document.parser_profile,
        }

    @staticmethod
    def _fragment_for_claim(
        *,
        claim: ClaimRecord,
        fragments_by_id: dict[UUID, ExtractedFragmentSnapshot],
    ) -> ExtractedFragmentSnapshot:
        fragment_ref = claim.metadata.get("fragment_id")
        fragment_id: UUID | None = None
        if isinstance(fragment_ref, UUID):
            fragment_id = fragment_ref
        elif isinstance(fragment_ref, str):
            try:
                fragment_id = UUID(fragment_ref)
            except ValueError as exc:
                raise ClaimBuildRepositoryError(
                    f"Claim {claim.claim_id} has invalid fragment reference {fragment_ref}."
                ) from exc

        fragment = fragments_by_id.get(fragment_id)
        if fragment is None:
            raise ClaimBuildRepositoryError(
                f"Fragment {fragment_ref} was not found for claim {claim.claim_id}."
            )
        return fragment

    @staticmethod
    def _document_from_row(row: Any) -> ParsedDocumentSnapshot:
        return ParsedDocumentSnapshot(
            parsed_document_id=row["parsed_document_id"],
            crawl_run_id=row["crawl_run_id"],
            raw_artifact_id=row["raw_artifact_id"],
            source_key=row["source_key"],
            parser_profile=row["parser_profile"],
            parser_version=row["parser_version"],
            entity_type=row["entity_type"],
            entity_hint=row["entity_hint"],
            parsed_at=row["parsed_at"],
            metadata=json_from_db(row["metadata"]),
        )

    @staticmethod
    def _fragment_from_row(row: Any) -> ExtractedFragmentSnapshot:
        value_payload = json_from_db(row["value"])
        return ExtractedFragmentSnapshot(
            fragment_id=row["fragment_id"],
            parsed_document_id=row["parsed_document_id"],
            raw_artifact_id=row["raw_artifact_id"],
            source_key=row["source_key"],
            source_url=row["source_url"],
            captured_at=row["captured_at"],
            field_name=row["field_name"],
            value=value_payload.get("value"),
            value_type=row["value_type"],
            locator=row["locator"],
            confidence=row["confidence"],
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


def deterministic_claim_id(
    *,
    parsed_document_id: UUID,
    fragment_id: UUID,
    normalizer_version: str,
) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        f"normalize.claim:{parsed_document_id}:{fragment_id}:{normalizer_version}",
    )


def deterministic_evidence_id(
    *,
    claim_id: UUID,
    raw_artifact_id: UUID,
    fragment_id: UUID | None,
) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        f"normalize.claim_evidence:{claim_id}:{raw_artifact_id}:{fragment_id}",
    )
