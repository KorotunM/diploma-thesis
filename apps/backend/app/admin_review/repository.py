from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from uuid import UUID

from apps.backend.app.persistence import json_from_db, sql_text


class ReviewCaseRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def list_cases(
        self,
        *,
        status: str | None,
        limit: int,
        offset: int,
    ) -> tuple[int, list[dict[str, Any]]]:
        filters = []
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if status:
            filters.append("rc.status = :status")
            params["status"] = status
        where_sql = "WHERE " + " AND ".join(filters) if filters else ""
        result = self._session.execute(
            self._sql_text(
                f"""
                WITH filtered AS (
                    SELECT
                        rc.review_case_id,
                        rc.status,
                        rc.reason,
                        rc.priority,
                        rc.university_id,
                        rc.evidence_ids,
                        rc.metadata,
                        rc.created_at,
                        rc.updated_at,
                        rc.resolved_at,
                        rc.resolved_by,
                        rc.resolution,
                        rc.note,
                        COUNT(*) OVER() AS total
                    FROM ops.review_case rc
                    {where_sql}
                    ORDER BY
                        CASE rc.priority WHEN 'high' THEN 0 ELSE 1 END,
                        rc.created_at DESC
                    LIMIT :limit
                    OFFSET :offset
                )
                SELECT
                    filtered.*,
                    doc.canonical_name
                FROM filtered
                LEFT JOIN LATERAL (
                    SELECT university_search_doc.canonical_name
                    FROM delivery.university_search_doc
                    WHERE university_search_doc.university_id = filtered.university_id
                    ORDER BY university_search_doc.card_version DESC
                    LIMIT 1
                ) AS doc ON TRUE
                """
            ),
            params,
        )
        rows = [self._row_to_dict(row) for row in result.mappings().all()]
        total = int(rows[0]["total"]) if rows else 0
        for row in rows:
            row.pop("total", None)
        return total, rows

    def resolve_case(
        self,
        *,
        review_case_id: UUID,
        resolved_by: UUID,
        resolution: str,
        note: str | None,
    ) -> dict[str, Any] | None:
        result = self._session.execute(
            self._sql_text(
                """
                UPDATE ops.review_case
                SET
                    status = CASE
                        WHEN :resolution = 'ignored' THEN 'dismissed'
                        ELSE 'resolved'
                    END,
                    resolved_at = now(),
                    resolved_by = :resolved_by,
                    resolution = :resolution,
                    note = :note,
                    updated_at = now()
                WHERE review_case_id = :review_case_id
                RETURNING
                    review_case_id,
                    status,
                    reason,
                    priority,
                    university_id,
                    evidence_ids,
                    metadata,
                    created_at,
                    updated_at,
                    resolved_at,
                    resolved_by,
                    resolution,
                    note,
                    NULL::text AS canonical_name
                """
            ),
            {
                "review_case_id": review_case_id,
                "resolved_by": resolved_by,
                "resolution": resolution,
                "note": note,
            },
        )
        row = result.mappings().first()
        if row is None:
            return None
        self._session.commit()
        return self._row_to_dict(row)

    def upsert_case(
        self,
        *,
        review_case_id: UUID,
        reason: str,
        priority: str,
        university_id: UUID | None,
        evidence_ids: list[UUID],
        metadata: dict[str, Any],
    ) -> None:
        self._session.execute(
            self._sql_text(
                """
                INSERT INTO ops.review_case (
                    review_case_id,
                    status,
                    reason,
                    priority,
                    university_id,
                    evidence_ids,
                    metadata
                )
                VALUES (
                    :review_case_id,
                    'open',
                    :reason,
                    :priority,
                    :university_id,
                    :evidence_ids,
                    CAST(:metadata AS jsonb)
                )
                ON CONFLICT (review_case_id) DO UPDATE
                SET
                    reason = EXCLUDED.reason,
                    priority = EXCLUDED.priority,
                    university_id = EXCLUDED.university_id,
                    evidence_ids = EXCLUDED.evidence_ids,
                    metadata = EXCLUDED.metadata,
                    updated_at = now()
                """
            ),
            {
                "review_case_id": review_case_id,
                "reason": reason,
                "priority": priority,
                "university_id": university_id,
                "evidence_ids": evidence_ids,
                "metadata": json.dumps(metadata, ensure_ascii=False),
            },
        )

    @staticmethod
    def _row_to_dict(row: Any) -> dict[str, Any]:
        data = dict(row)
        data["metadata"] = json_from_db(data.get("metadata"))
        data["evidence_ids"] = list(data.get("evidence_ids") or [])
        return data
