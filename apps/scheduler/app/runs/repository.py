from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from apps.scheduler.app.persistence import json_from_db, json_to_db, sql_text

from .models import PipelineRunRecord, PipelineRunStatus, PipelineRunType, PipelineTriggerType


class PipelineRunRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def create(
        self,
        *,
        run_id: UUID,
        run_type: PipelineRunType,
        status: PipelineRunStatus,
        trigger_type: PipelineTriggerType,
        source_key: str | None,
        metadata: dict[str, Any],
    ) -> PipelineRunRecord:
        result = self._session.execute(
            self._sql_text(
                """
                INSERT INTO ops.pipeline_run (
                    run_id,
                    run_type,
                    status,
                    trigger_type,
                    source_key,
                    metadata
                )
                VALUES (
                    :run_id,
                    :run_type,
                    :status,
                    :trigger_type,
                    :source_key,
                    CAST(:metadata AS jsonb)
                )
                RETURNING
                    run_id,
                    run_type,
                    status,
                    trigger_type,
                    source_key,
                    started_at,
                    finished_at,
                    metadata
                """
            ),
            {
                "run_id": run_id,
                "run_type": run_type.value,
                "status": status.value,
                "trigger_type": trigger_type.value,
                "source_key": source_key,
                "metadata": json_to_db(metadata),
            },
        )
        return self._record_from_row(result.mappings().one())

    def get(self, run_id: UUID) -> PipelineRunRecord | None:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    run_id,
                    run_type,
                    status,
                    trigger_type,
                    source_key,
                    started_at,
                    finished_at,
                    metadata
                FROM ops.pipeline_run
                WHERE run_id = :run_id
                """
            ),
            {"run_id": run_id},
        )
        row = result.mappings().one_or_none()
        if row is None:
            return None
        return self._record_from_row(row)

    def transition(
        self,
        *,
        run_id: UUID,
        status: PipelineRunStatus,
        metadata_patch: dict[str, Any] | None = None,
        finish: bool = False,
    ) -> PipelineRunRecord | None:
        result = self._session.execute(
            self._sql_text(
                """
                UPDATE ops.pipeline_run
                SET
                    status = :status,
                    finished_at = CASE
                        WHEN :finish THEN now()
                        ELSE finished_at
                    END,
                    metadata = metadata || CAST(:metadata_patch AS jsonb)
                WHERE run_id = :run_id
                RETURNING
                    run_id,
                    run_type,
                    status,
                    trigger_type,
                    source_key,
                    started_at,
                    finished_at,
                    metadata
                """
            ),
            {
                "run_id": run_id,
                "status": status.value,
                "finish": finish,
                "metadata_patch": json_to_db(metadata_patch),
            },
        )
        row = result.mappings().one_or_none()
        if row is None:
            return None
        return self._record_from_row(row)

    @staticmethod
    def _record_from_row(row: Any) -> PipelineRunRecord:
        return PipelineRunRecord(
            run_id=row["run_id"],
            run_type=row["run_type"],
            status=row["status"],
            trigger_type=row["trigger_type"],
            source_key=row["source_key"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            metadata=json_from_db(row["metadata"]),
        )
