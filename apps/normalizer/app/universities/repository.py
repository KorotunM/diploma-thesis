from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from apps.normalizer.app.persistence import json_from_db, json_to_db, sql_text

from .models import (
    SourceAuthorityRecord,
    UniversityBootstrapCandidate,
    UniversityRecord,
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
