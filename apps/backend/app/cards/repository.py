from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from apps.backend.app.persistence import json_from_db, sql_text
from libs.domain.university import UniversityCard

from .models import DeliveryUniversityCardRecord, ResolvedFactSelectionRecord


class UniversityCardReadRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def get_latest_by_university_id(
        self,
        university_id: UUID,
    ) -> DeliveryUniversityCardRecord | None:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    university_id,
                    card_version,
                    card_json,
                    generated_at
                FROM delivery.university_card
                WHERE university_id = :university_id
                ORDER BY card_version DESC
                LIMIT 1
                """
            ),
            {"university_id": university_id},
        )
        row = result.mappings().one_or_none()
        if row is None:
            return None
        return self._record_from_row(row)

    def list_resolved_facts(
        self,
        *,
        university_id: UUID,
        card_version: int,
    ) -> list[ResolvedFactSelectionRecord]:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    field_name,
                    resolution_policy,
                    fact_score,
                    metadata
                FROM core.resolved_fact
                WHERE university_id = :university_id
                  AND card_version = :card_version
                ORDER BY field_name ASC
                """
            ),
            {
                "university_id": university_id,
                "card_version": card_version,
            },
        )
        return [self._fact_from_row(row) for row in result.mappings().all()]

    @staticmethod
    def _record_from_row(row: Any) -> DeliveryUniversityCardRecord:
        return DeliveryUniversityCardRecord(
            university_id=row["university_id"],
            card_version=row["card_version"],
            card=UniversityCard.model_validate(json_from_db(row["card_json"])),
            generated_at=row["generated_at"],
        )

    @staticmethod
    def _fact_from_row(row: Any) -> ResolvedFactSelectionRecord:
        metadata = json_from_db(row["metadata"])
        return ResolvedFactSelectionRecord(
            field_name=row["field_name"],
            resolution_policy=row["resolution_policy"],
            fact_score=row["fact_score"],
            selected_claim_ids=_uuid_list(metadata.get("selected_claim_ids")),
            selected_evidence_ids=_uuid_list(metadata.get("selected_evidence_ids")),
            metadata=metadata,
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
