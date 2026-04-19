from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .models import CreateSourceRequest, SourceRecord, UpdateSourceRequest
from .serialization import json_from_db, json_to_db


class SourceAlreadyExistsError(ValueError):
    def __init__(self, source_key: str) -> None:
        super().__init__(f"Source already exists: {source_key}")
        self.source_key = source_key


def _sql_text(statement: str) -> Any:
    try:
        from sqlalchemy import text
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "SQLAlchemy is required for source registry persistence. "
            "Install project runtime dependencies before using scheduler repositories."
        ) from exc
    return text(statement)


class SourceRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = _sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def create(self, request: CreateSourceRequest) -> SourceRecord:
        if self.get_by_key(request.source_key) is not None:
            raise SourceAlreadyExistsError(request.source_key)

        result = self._session.execute(
            self._sql_text(
                """
                INSERT INTO ingestion.source (
                    source_id,
                    source_key,
                    source_type,
                    trust_tier,
                    is_active,
                    metadata
                )
                VALUES (
                    :source_id,
                    :source_key,
                    :source_type,
                    :trust_tier,
                    :is_active,
                    CAST(:metadata AS jsonb)
                )
                RETURNING
                    source_id,
                    source_key,
                    source_type,
                    trust_tier,
                    is_active,
                    metadata
                """
            ),
            {
                "source_id": request.source_id,
                "source_key": request.source_key,
                "source_type": request.source_type.value,
                "trust_tier": request.trust_tier.value,
                "is_active": request.is_active,
                "metadata": json_to_db(request.metadata),
            },
        )
        return self._record_from_row(result.mappings().one())

    def get_by_key(self, source_key: str) -> SourceRecord | None:
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
        return self._record_from_row(row)

    def list(
        self,
        *,
        limit: int,
        offset: int,
        include_inactive: bool,
    ) -> tuple[list[SourceRecord], int]:
        params = {
            "limit": limit,
            "offset": offset,
            "include_inactive": include_inactive,
        }
        items_result = self._session.execute(
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
                WHERE (:include_inactive OR is_active = true)
                ORDER BY source_key
                LIMIT :limit
                OFFSET :offset
                """
            ),
            params,
        )
        count_result = self._session.execute(
            self._sql_text(
                """
                SELECT count(*) AS total
                FROM ingestion.source
                WHERE (:include_inactive OR is_active = true)
                """
            ),
            params,
        )

        records = [self._record_from_row(row) for row in items_result.mappings().all()]
        return records, int(count_result.scalar_one())

    def update(self, source_key: str, request: UpdateSourceRequest) -> SourceRecord | None:
        metadata_is_set = "metadata" in request.model_fields_set
        result = self._session.execute(
            self._sql_text(
                """
                UPDATE ingestion.source
                SET
                    source_type = COALESCE(:source_type, source_type),
                    trust_tier = COALESCE(:trust_tier, trust_tier),
                    is_active = COALESCE(:is_active, is_active),
                    metadata = CASE
                        WHEN :metadata_is_set THEN CAST(:metadata AS jsonb)
                        ELSE metadata
                    END
                WHERE source_key = :source_key
                RETURNING
                    source_id,
                    source_key,
                    source_type,
                    trust_tier,
                    is_active,
                    metadata
                """
            ),
            {
                "source_key": source_key,
                "source_type": request.source_type.value if request.source_type else None,
                "trust_tier": request.trust_tier.value if request.trust_tier else None,
                "is_active": request.is_active,
                "metadata_is_set": metadata_is_set,
                "metadata": json_to_db(request.metadata) if metadata_is_set else "{}",
            },
        )
        row = result.mappings().one_or_none()
        if row is None:
            return None
        return self._record_from_row(row)

    @staticmethod
    def _record_from_row(row: Any) -> SourceRecord:
        return SourceRecord(
            source_id=row["source_id"],
            source_key=row["source_key"],
            source_type=row["source_type"],
            trust_tier=row["trust_tier"],
            is_active=row["is_active"],
            metadata=json_from_db(row["metadata"]),
        )
