from __future__ import annotations

import json
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
        sort_by: str = "rating",
        region: str | None = None,
        ege_subjects: list[str] | None = None,
        program_codes: list[str] | None = None,
        dormitory: bool = False,
        military_department: bool = False,
    ) -> list[UniversitySearchHitRecord]:
        query_present = bool(query and normalized_query)
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        sort_expression = {
            "rating": "rating_score DESC NULLS LAST",
            "budget_places": "budget_places DESC NULLS LAST",
            "avg_passing_score": "avg_passing_score DESC NULLS LAST",
        }.get(sort_by, "rating_score DESC NULLS LAST")

        program_code_predicate = ""
        if program_codes:
            params["program_codes_json"] = json.dumps(program_codes, ensure_ascii=False)
            program_code_predicate = (
                "EXISTS ("
                " SELECT 1"
                " FROM jsonb_array_elements("
                "   COALESCE(search_doc.search_document->'program_codes', '[]'::jsonb)"
                "   || COALESCE(card.card_json->'programs', '[]'::jsonb)"
                "   || COALESCE(card.card_json#>'{admission,programs}', '[]'::jsonb)"
                " ) AS program_item"
                " WHERE ("
                "   CASE"
                "     WHEN jsonb_typeof(program_item) = 'string' THEN program_item #>> '{}'"
                "     ELSE program_item ->> 'code'"
                "   END"
                " ) IN (SELECT jsonb_array_elements_text(CAST(:program_codes_json AS jsonb)))"
                ")"
            )

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
            if program_code_predicate:
                query_predicate = f"{query_predicate} OR {program_code_predicate}"
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
            query_predicate = program_code_predicate or "TRUE"
            text_rank_expression = "0.0"
            trigram_expression = "0.0"

        # --- optional filter clauses (only added when value is present) ---
        extra_filters: list[str] = []

        if city:
            params["city"] = city
            extra_filters.append("lower(search_doc.city_name) LIKE lower(:city) || '%'")

        if region:
            params["region"] = region
            extra_filters.append("lower(search_doc.region_name) LIKE lower(:region) || '%'")

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

        if ege_subjects:
            params["ege_subjects_json"] = json.dumps(ege_subjects, ensure_ascii=False)
            extra_filters.append(
                "COALESCE(search_doc.search_document->'program_ege_subjects', '[]'::jsonb)"
                " @> CAST(:ege_subjects_json AS jsonb)"
            )

        if dormitory:
            extra_filters.append(
                "("
                " COALESCE("
                "   search_doc.search_document->'dormitory',"
                "   card.card_json->'dormitory'"
                " )"
                " IS NOT NULL"
                " AND COALESCE("
                "   search_doc.search_document->'dormitory',"
                "   card.card_json->'dormitory'"
                " )"
                " <> '{}'::jsonb"
                " AND lower(COALESCE("
                "   search_doc.search_document #>> '{dormitory,available}',"
                "   search_doc.search_document #>> '{dormitory,has_dormitory}',"
                "   search_doc.search_document #>> '{dormitory,exists}',"
                "   search_doc.search_document #>> '{dormitory,provided}',"
                "   search_doc.search_document #>> '{dormitory,value}',"
                "   card.card_json #>> '{dormitory,available}',"
                "   card.card_json #>> '{dormitory,has_dormitory}',"
                "   card.card_json #>> '{dormitory,exists}',"
                "   card.card_json #>> '{dormitory,provided}',"
                "   card.card_json #>> '{dormitory,value}',"
                "   'true'"
                " )) NOT IN ('false', '0', 'no', 'нет')"
                ")"
            )

        if military_department:
            extra_filters.append(
                "("
                " lower(COALESCE("
                "   search_doc.search_document #>> '{military_department,available}',"
                "   search_doc.search_document #>> '{military_department,has_military_department}',"
                "   search_doc.search_document #>> '{military_department,exists}',"
                "   search_doc.search_document #>> '{military_department,provided}',"
                "   search_doc.search_document #>> '{military_department,value}',"
                "   search_doc.search_document #>> '{institutional,military_department}',"
                "   card.card_json #>> '{military_department,available}',"
                "   card.card_json #>> '{military_department,has_military_department}',"
                "   card.card_json #>> '{military_department,exists}',"
                "   card.card_json #>> '{military_department,provided}',"
                "   card.card_json #>> '{military_department,value}',"
                "   card.card_json #>> '{institutional,military_department}',"
                "   card.card_json #>> '{military,department}'"
                " )) IN ('true', '1', 'yes', 'да', 'есть')"
                " OR ("
                "   COALESCE("
                "     search_doc.search_document->'military_department',"
                "     card.card_json->'military_department'"
                "   ) IS NOT NULL"
                "   AND COALESCE("
                "     search_doc.search_document->'military_department',"
                "     card.card_json->'military_department'"
                "   ) <> '{}'::jsonb"
                " )"
                ")"
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
                        (search_doc.search_document->>'logo_url') AS logo_url,
                        search_doc.website_domain,
                        search_doc.country_code,
                        search_doc.city_name,
                        search_doc.region_name,
                        search_doc.aliases,
                        search_doc.metadata,
                        search_doc.generated_at,
                        rating_summary.rating_score,
                        COALESCE(
                            rating_summary.rating_category,
                            NULLIF(
                                search_doc.search_document #>> '{{institutional,category}}',
                                ''
                            ),
                            NULLIF(card.card_json #>> '{{institutional,category}}', '')
                        ) AS rating_category,
                        COALESCE(
                            CAST(
                                NULLIF(
                                    search_doc.search_document #>> '{{stats,budget_places}}',
                                    ''
                                ) AS integer
                            ),
                            CAST(
                                NULLIF(card.card_json #>> '{{stats,budget_places}}', '')
                                AS integer
                            )
                        ) AS budget_places,
                        COALESCE(
                            CAST(
                                NULLIF(
                                    search_doc.search_document #>> '{{stats,paid_places}}',
                                    ''
                                ) AS integer
                            ),
                            CAST(
                                NULLIF(card.card_json #>> '{{stats,paid_places}}', '')
                                AS integer
                            )
                        ) AS paid_places,
                        COALESCE(
                            CAST(
                                NULLIF(
                                    search_doc.search_document #>> '{{stats,avg_passing_score}}',
                                    ''
                                ) AS double precision
                            ),
                            CAST(
                                NULLIF(card.card_json #>> '{{stats,avg_passing_score}}', '')
                                AS double precision
                            )
                        ) AS avg_passing_score,
                        {text_rank_expression} AS text_rank,
                        {trigram_expression} AS trigram_score
                    FROM delivery.university_search_doc AS search_doc
                    LEFT JOIN delivery.university_card AS card
                        ON card.university_id = search_doc.university_id
                        AND card.card_version = search_doc.card_version
                    LEFT JOIN LATERAL (
                        SELECT
                            AVG(CAST(rating->>'value' AS double precision)) AS rating_score,
                            MAX(NULLIF(rating->>'category', '')) AS rating_category
                        FROM jsonb_array_elements(
                            COALESCE(
                                search_doc.search_document->'ratings',
                                card.card_json->'ratings',
                                '[]'::jsonb
                            )
                        ) AS rating
                        WHERE (rating->>'value') ~ '^[0-9]+(\\.[0-9]+)?$'
                    ) AS rating_summary ON TRUE
                    LEFT JOIN core.university AS university
                        ON university.university_id = search_doc.university_id
                    WHERE {where_clause}
                )
                SELECT
                    university_id,
                    card_version,
                    canonical_name,
                    website_url,
                    logo_url,
                    website_domain,
                    country_code,
                    city_name,
                    region_name,
                    aliases,
                    metadata,
                    generated_at,
                    rating_score,
                    rating_category,
                    budget_places,
                    paid_places,
                    avg_passing_score,
                    text_rank,
                    trigram_score,
                    ((text_rank * 0.7) + (trigram_score * 0.3)) AS combined_score,
                    COUNT(*) OVER() AS total_count
                FROM ranked
                ORDER BY
                    {sort_expression},
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
            logo_url=row.get("logo_url"),
            website_domain=row["website_domain"],
            country_code=row["country_code"],
            city_name=row["city_name"],
            region_name=row["region_name"],
            aliases=list(row["aliases"] or []),
            generated_at=row["generated_at"],
            rating_score=(
                float(row["rating_score"]) if row.get("rating_score") is not None else None
            ),
            rating_category=row.get("rating_category"),
            budget_places=(
                int(row["budget_places"]) if row.get("budget_places") is not None else None
            ),
            paid_places=int(row["paid_places"]) if row.get("paid_places") is not None else None,
            avg_passing_score=(
                float(row["avg_passing_score"])
                if row.get("avg_passing_score") is not None
                else None
            ),
            text_rank=float(row["text_rank"]),
            trigram_score=float(row["trigram_score"]),
            combined_score=float(row["combined_score"]),
            total_count=int(row["total_count"]),
            metadata=json_from_db(row["metadata"]),
        )
