from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from apps.normalizer.app.persistence import json_from_db, json_to_db, sql_text

from .models import ResolvedFactCandidate, ResolvedFactRecord


class ResolvedFactRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def upsert_resolved_facts(
        self,
        candidates: Sequence[ResolvedFactCandidate],
    ) -> list[ResolvedFactRecord]:
        records: list[ResolvedFactRecord] = []
        for candidate in candidates:
            result = self._session.execute(
                self._sql_text(
                    """
                    INSERT INTO core.resolved_fact (
                        resolved_fact_id,
                        university_id,
                        field_name,
                        value_json,
                        fact_score,
                        resolution_policy,
                        card_version,
                        metadata
                    )
                    VALUES (
                        :resolved_fact_id,
                        :university_id,
                        :field_name,
                        CAST(:value_json AS jsonb),
                        :fact_score,
                        :resolution_policy,
                        :card_version,
                        CAST(:metadata AS jsonb)
                    )
                    ON CONFLICT (resolved_fact_id)
                    DO UPDATE SET
                        university_id = EXCLUDED.university_id,
                        field_name = EXCLUDED.field_name,
                        value_json = EXCLUDED.value_json,
                        fact_score = EXCLUDED.fact_score,
                        resolution_policy = EXCLUDED.resolution_policy,
                        card_version = EXCLUDED.card_version,
                        resolved_at = now(),
                        metadata = core.resolved_fact.metadata || EXCLUDED.metadata
                    RETURNING
                        resolved_fact_id,
                        university_id,
                        field_name,
                        value_json,
                        fact_score,
                        resolution_policy,
                        card_version,
                        resolved_at,
                        metadata
                    """
                ),
                {
                    "resolved_fact_id": candidate.resolved_fact_id,
                    "university_id": candidate.university_id,
                    "field_name": candidate.field_name,
                    "value_json": json_to_db(
                        {
                            "value": candidate.value,
                            "value_type": candidate.value_type,
                        }
                    ),
                    "fact_score": candidate.fact_score,
                    "resolution_policy": candidate.resolution_policy,
                    "card_version": candidate.card_version,
                    "metadata": json_to_db(
                        {
                            **candidate.metadata,
                            "selected_claim_ids": [
                                str(claim_id) for claim_id in candidate.selected_claim_ids
                            ],
                            "selected_evidence_ids": [
                                str(evidence_id)
                                for evidence_id in candidate.selected_evidence_ids
                            ],
                        }
                    ),
                },
            )
            records.append(self._fact_from_row(result.mappings().one()))
        return records

    def commit(self) -> None:
        self._session.commit()

    @staticmethod
    def _fact_from_row(row: Any) -> ResolvedFactRecord:
        value_payload = json_from_db(row["value_json"])
        metadata = json_from_db(row["metadata"])
        return ResolvedFactRecord(
            resolved_fact_id=row["resolved_fact_id"],
            university_id=row["university_id"],
            field_name=row["field_name"],
            value=value_payload.get("value"),
            value_type=value_payload.get("value_type", "unknown"),
            fact_score=row["fact_score"],
            resolution_policy=row["resolution_policy"],
            selected_claim_ids=[
                UUID(claim_id) for claim_id in metadata.get("selected_claim_ids", [])
            ],
            selected_evidence_ids=[
                UUID(evidence_id)
                for evidence_id in metadata.get("selected_evidence_ids", [])
            ],
            card_version=row["card_version"],
            resolved_at=row["resolved_at"],
            metadata=metadata,
        )


def deterministic_resolved_fact_id(
    *,
    university_id: UUID,
    field_name: str,
    card_version: int,
) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        f"core.resolved_fact:{university_id}:{field_name}:{card_version}",
    )
