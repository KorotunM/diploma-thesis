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
        query: str | None,
        normalized_query: str | None,
        city: str | None,
        country_code: str | None,
        source_type: str | None,
        limit: int,
        offset: int,
    ) -> list[UniversitySearchHitRecord]:
        query_present = bool(query and normalized_query)
        params: dict[str, Any] = {"limit": limit, "offset": offset}

        # --- text-search expressions (only when query is given) ---
        if query_present:
            params["query"] = query
            params["normalized_query"] = normalized_query
            query_predicate = (
                "search_doc.search_text @@ plainto_tsquery('simple', :query)"
                " OR search_doc.canonical_name % :query"
                " OR search_doc.canonical_name_normalized % :normalized_query"
                " OR search_doc.website_domain % :normalized_query"
            )
            text_rank_expression = (
                "CASE"
                " WHEN search_doc.search_text @@ plainto_tsquery('simple', :query)"
                " THEN ts_rank_cd(search_doc.search_text, plainto_tsquery('simple', :query))"
                " ELSE 0.0 END"
            )
            trigram_expression = (
                "GREATEST("
                " similarity(search_doc.canonical_name, :query),"
                " similarity(search_doc.canonical_name_normalized, :normalized_query),"
                " similarity(COALESCE(search_doc.website_domain, ''), :normalized_query)"
                ")"
            )
        else:
            query_predicate = "TRUE"
            text_rank_expression = "0.0"
            trigram_expression = "0.0"

        # --- optional filter clauses (only added when value is present) ---
        extra_filters: list[str] = []

        if city:
            params["city"] = city
            extra_filters.append("lower(search_doc.city_name) = lower(:city)")

        if country_code:
            params["country_code"] = country_code
            extra_filters.append("upper(search_doc.country_code) = upper(:country_code)")

        if source_type:
            params["source_type"] = source_type
            extra_filters.append(
                "(lower(COALESCE(university.metadata ->> 'source_type', '')) = lower(:source_type)"
                " OR EXISTS ("
                "   SELECT 1 FROM jsonb_array_elements("
                "     COALESCE(university.metadata -> 'source_snapshots', '[]'::jsonb)"
                "   ) AS snapshot"
                "   WHERE lower(COALESCE(snapshot ->> 'source_type', '')) = lower(:source_type)"
                " ))"
            )

        where_clause = f"({query_predicate})"
        for f in extra_filters:
            where_clause += f" AND {f}"

        result = self._session.execute(
            self._sql_text(
                f"""
                WITH ranked AS (
                    SELECT
                        search_doc.university_id,
                        search_doc.card_version,
                        search_doc.canonical_name,
                        search_doc.website_url,
                        search_doc.website_domain,
                        search_doc.country_code,
                        search_doc.city_name,
                        search_doc.aliases,
                        search_doc.metadata,
                        search_doc.generated_at,
                        {text_rank_expression} AS text_rank,
                        {trigram_expression} AS trigram_score
                    FROM delivery.university_search_doc AS search_doc
                    LEFT JOIN core.university AS university
                        ON university.university_id = search_doc.university_id
                    WHERE {where_clause}
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
                    ((text_rank * 0.7) + (trigram_score * 0.3)) AS combined_score,
                    COUNT(*) OVER() AS total_count
                FROM ranked
                ORDER BY
                    CASE WHEN text_rank > 0 THEN 1 ELSE 0 END DESC,
                    combined_score DESC,
                    canonical_name ASC,
                    university_id ASC
                LIMIT :limit
                OFFSET :offset
                """
            ),
            params,
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
            total_count=int(row["total_count"]),
            metadata=json_from_db(row["metadata"]),
        )
