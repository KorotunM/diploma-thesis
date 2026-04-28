from __future__ import annotations

from collections.abc import Callable
from typing import Any

from apps.backend.app.persistence import json_from_db, sql_text

from .models import UniversitySearchHitRecord


class UniversitySearchRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def search(
        self,
        *,
        query: str,
        normalized_query: str,
        limit: int,
    ) -> list[UniversitySearchHitRecord]:
        result = self._session.execute(
            self._sql_text(
                """
                WITH ranked AS (
                    SELECT
                        university_id,
                        card_version,
                        canonical_name,
                        website_url,
                        website_domain,
                        country_code,
                        city_name,
                        aliases,
                        metadata,
                        generated_at,
                        CASE
                            WHEN search_text @@ plainto_tsquery('simple', :query)
                            THEN ts_rank_cd(search_text, plainto_tsquery('simple', :query))
                            ELSE 0.0
                        END AS text_rank,
                        GREATEST(
                            similarity(canonical_name, :query),
                            similarity(canonical_name_normalized, :normalized_query),
                            similarity(COALESCE(website_domain, ''), :normalized_query)
                        ) AS trigram_score
                    FROM delivery.university_search_doc
                    WHERE search_text @@ plainto_tsquery('simple', :query)
                       OR canonical_name % :query
                       OR canonical_name_normalized % :normalized_query
                       OR website_domain % :normalized_query
                )
                SELECT
                    university_id,
                    card_version,
                    canonical_name,
                    website_url,
                    website_domain,
                    country_code,
                    city_name,
                    aliases,
                    metadata,
                    generated_at,
                    text_rank,
                    trigram_score,
                    ((text_rank * 0.7) + (trigram_score * 0.3)) AS combined_score
                FROM ranked
                ORDER BY
                    CASE WHEN text_rank > 0 THEN 1 ELSE 0 END DESC,
                    combined_score DESC,
                    canonical_name ASC,
                    university_id ASC
                LIMIT :limit
                """
            ),
            {
                "query": query,
                "normalized_query": normalized_query,
                "limit": limit,
            },
        )
        return [self._hit_from_row(row) for row in result.mappings().all()]

    @staticmethod
    def _hit_from_row(row: Any) -> UniversitySearchHitRecord:
        return UniversitySearchHitRecord(
            university_id=row["university_id"],
            card_version=row["card_version"],
            canonical_name=row["canonical_name"],
            website_url=row["website_url"],
            website_domain=row["website_domain"],
            country_code=row["country_code"],
            city_name=row["city_name"],
            aliases=list(row["aliases"] or []),
            generated_at=row["generated_at"],
            text_rank=float(row["text_rank"]),
            trigram_score=float(row["trigram_score"]),
            combined_score=float(row["combined_score"]),
            metadata=json_from_db(row["metadata"]),
        )
