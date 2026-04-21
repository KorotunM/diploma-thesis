from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any
from uuid import uuid4

from apps.parser.app.persistence import json_from_db, json_to_db, sql_text
from libs.source_sdk import ExtractedFragment, IntermediateRecord, ParserExecutionResult

from .models import ExtractedFragmentRecord, ParsedDocumentRecord


class ParsedDocumentPersistenceError(ValueError):
    pass


class ParsedDocumentRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def upsert_document(
        self,
        *,
        execution_result: ParserExecutionResult,
    ) -> ParsedDocumentRecord:
        self._validate_execution_result(execution_result)
        artifact = execution_result.artifact
        assert artifact is not None
        primary_record = self._primary_intermediate_record(execution_result.intermediate_records)
        source_key = self._source_key(
            artifact_source_key=artifact.source_key,
            primary_record=primary_record,
        )
        parsed_document_id = uuid4()
        result = self._session.execute(
            self._sql_text(
                """
                INSERT INTO parsing.parsed_document (
                    parsed_document_id,
                    crawl_run_id,
                    raw_artifact_id,
                    source_key,
                    parser_profile,
                    parser_version,
                    entity_type,
                    entity_hint,
                    extracted_fragment_count,
                    parsed_at,
                    metadata
                )
                VALUES (
                    :parsed_document_id,
                    :crawl_run_id,
                    :raw_artifact_id,
                    :source_key,
                    :parser_profile,
                    :parser_version,
                    :entity_type,
                    :entity_hint,
                    :extracted_fragment_count,
                    :parsed_at,
                    CAST(:metadata AS jsonb)
                )
                ON CONFLICT (raw_artifact_id, parser_version)
                DO UPDATE SET
                    source_key = EXCLUDED.source_key,
                    parser_profile = EXCLUDED.parser_profile,
                    entity_type = EXCLUDED.entity_type,
                    entity_hint = EXCLUDED.entity_hint,
                    extracted_fragment_count = EXCLUDED.extracted_fragment_count,
                    parsed_at = EXCLUDED.parsed_at,
                    metadata = parsing.parsed_document.metadata || EXCLUDED.metadata
                RETURNING
                    parsed_document_id,
                    crawl_run_id,
                    raw_artifact_id,
                    source_key,
                    parser_profile,
                    parser_version,
                    entity_type,
                    entity_hint,
                    extracted_fragment_count,
                    parsed_at,
                    metadata
                """
            ),
            {
                "parsed_document_id": parsed_document_id,
                "crawl_run_id": execution_result.crawl_run_id,
                "raw_artifact_id": artifact.raw_artifact_id,
                "source_key": source_key,
                "parser_profile": self._parser_profile(primary_record),
                "parser_version": execution_result.adapter_version,
                "entity_type": primary_record.entity_type,
                "entity_hint": primary_record.entity_hint,
                "extracted_fragment_count": len(execution_result.fragments),
                "parsed_at": execution_result.completed_at,
                "metadata": json_to_db(
                    self._document_metadata(
                        execution_result=execution_result,
                        primary_record=primary_record,
                    )
                ),
            },
        )
        return self._document_from_row(result.mappings().one())

    def upsert_fragments(
        self,
        *,
        parsed_document: ParsedDocumentRecord,
        fragments: Sequence[ExtractedFragment],
    ) -> list[ExtractedFragmentRecord]:
        records: list[ExtractedFragmentRecord] = []
        for fragment in fragments:
            result = self._session.execute(
                self._sql_text(
                    """
                    INSERT INTO parsing.extracted_fragment (
                        fragment_id,
                        parsed_document_id,
                        raw_artifact_id,
                        source_key,
                        field_name,
                        value,
                        value_type,
                        locator,
                        confidence,
                        metadata
                    )
                    VALUES (
                        :fragment_id,
                        :parsed_document_id,
                        :raw_artifact_id,
                        :source_key,
                        :field_name,
                        CAST(:value AS jsonb),
                        :value_type,
                        :locator,
                        :confidence,
                        CAST(:metadata AS jsonb)
                    )
                    ON CONFLICT (fragment_id)
                    DO UPDATE SET
                        parsed_document_id = EXCLUDED.parsed_document_id,
                        raw_artifact_id = EXCLUDED.raw_artifact_id,
                        source_key = EXCLUDED.source_key,
                        field_name = EXCLUDED.field_name,
                        value = EXCLUDED.value,
                        value_type = EXCLUDED.value_type,
                        locator = EXCLUDED.locator,
                        confidence = EXCLUDED.confidence,
                        metadata = parsing.extracted_fragment.metadata || EXCLUDED.metadata
                    RETURNING
                        fragment_id,
                        parsed_document_id,
                        raw_artifact_id,
                        source_key,
                        field_name,
                        value,
                        value_type,
                        locator,
                        confidence,
                        metadata
                    """
                ),
                {
                    "fragment_id": fragment.fragment_id,
                    "parsed_document_id": parsed_document.parsed_document_id,
                    "raw_artifact_id": fragment.raw_artifact_id
                    or parsed_document.raw_artifact_id,
                    "source_key": fragment.source_key or parsed_document.source_key,
                    "field_name": fragment.field_name,
                    "value": json_to_db({"value": fragment.value}),
                    "value_type": self._value_type(fragment.value),
                    "locator": fragment.locator,
                    "confidence": fragment.confidence,
                    "metadata": json_to_db(
                        {
                            **fragment.metadata,
                            "parsed_document_id": str(parsed_document.parsed_document_id),
                        }
                    ),
                },
            )
            records.append(self._fragment_from_row(result.mappings().one()))
        return records

    def commit(self) -> None:
        self._session.commit()

    @staticmethod
    def _validate_execution_result(execution_result: ParserExecutionResult) -> None:
        if execution_result.artifact is None:
            raise ParsedDocumentPersistenceError(
                "Parser execution artifact is required before parsed document persistence."
            )
        if execution_result.completed_at is None:
            raise ParsedDocumentPersistenceError(
                "Parser execution must be completed before parsed document persistence."
            )
        if not execution_result.intermediate_records:
            raise ParsedDocumentPersistenceError(
                "At least one intermediate record is required for parsed document persistence."
            )

    @staticmethod
    def _primary_intermediate_record(
        records: Sequence[IntermediateRecord],
    ) -> IntermediateRecord:
        return records[0]

    @staticmethod
    def _parser_profile(record: IntermediateRecord) -> str:
        profile = record.metadata.get("parser_profile")
        return profile if isinstance(profile, str) else "unknown"

    @staticmethod
    def _source_key(
        *,
        artifact_source_key: str | None,
        primary_record: IntermediateRecord,
    ) -> str:
        source_key = artifact_source_key or primary_record.source_key
        if not source_key:
            raise ParsedDocumentPersistenceError(
                "Source key is required for parsed document persistence."
            )
        return source_key

    @staticmethod
    def _document_metadata(
        *,
        execution_result: ParserExecutionResult,
        primary_record: IntermediateRecord,
    ) -> dict[str, Any]:
        return {
            **execution_result.metadata,
            "adapter_key": execution_result.adapter_key,
            "adapter_version": execution_result.adapter_version,
            "intermediate_record_ids": [
                str(record.record_id) for record in execution_result.intermediate_records
            ],
            "claim_count": sum(
                len(record.claims) for record in execution_result.intermediate_records
            ),
            "fragment_ids": [str(fragment.fragment_id) for fragment in execution_result.fragments],
            "primary_intermediate_record_id": str(primary_record.record_id),
        }

    @staticmethod
    def _value_type(value: Any) -> str:
        if isinstance(value, list):
            return "list"
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "float"
        if value is None:
            return "null"
        return "str"

    @staticmethod
    def _document_from_row(row: Any) -> ParsedDocumentRecord:
        return ParsedDocumentRecord(
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
    def _fragment_from_row(row: Any) -> ExtractedFragmentRecord:
        value_payload = json_from_db(row["value"])
        return ExtractedFragmentRecord(
            fragment_id=row["fragment_id"],
            parsed_document_id=row["parsed_document_id"],
            raw_artifact_id=row["raw_artifact_id"],
            source_key=row["source_key"],
            field_name=row["field_name"],
            value=value_payload.get("value"),
            value_type=row["value_type"],
            locator=row["locator"],
            confidence=row["confidence"],
            metadata=json_from_db(row["metadata"]),
        )
