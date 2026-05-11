from __future__ import annotations

from collections.abc import Callable
from typing import Any

from apps.backend.app.persistence import sql_text


class RankingsRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def fetch_ranked(
        self,
        *,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        result = self._session.execute(
            self._sql_text(
                """
                WITH scored AS (
                    SELECT
                        sd.university_id::text            AS university_id,
                        sd.canonical_name,
                        (sd.search_document->>'logo_url') AS logo_url,
                        sd.city_name,
                        sd.region_name,
                        AVG(
                            CAST(rating->>'value' AS double precision)
                        ) AS composite_score,
                        COUNT(*)                          AS rating_count
                    FROM delivery.university_search_doc AS sd,
                         jsonb_array_elements(
                             COALESCE(sd.search_document->'ratings', '[]'::jsonb)
                         ) AS rating
                    WHERE (rating->>'value') ~ '^[0-9]+(\.[0-9]+)?$'
                    GROUP BY
                        sd.university_id,
                        sd.canonical_name,
                        sd.search_document,
                        sd.city_name,
                        sd.region_name
                    HAVING COUNT(*) > 0
                ),
                ranked AS (
                    SELECT
                        scored.*,
                        (SELECT COUNT(*) FROM scored) AS total,
                        ROW_NUMBER() OVER (ORDER BY composite_score DESC) AS rank
                    FROM scored
                ),
                latest_year AS (
                    SELECT MAX(year) AS year
                    FROM core.university_ranking_history
                    WHERE source_key = 'tabiturient-globalrating'
                )
                SELECT
                    ranked.*,
                    rh.change_direction,
                    rh.change_delta
                FROM ranked
                LEFT JOIN core.university_ranking_history rh
                    ON rh.university_id = ranked.university_id::uuid
                    AND rh.source_key = 'tabiturient-globalrating'
                    AND rh.year = (SELECT year FROM latest_year)
                ORDER BY ranked.composite_score DESC
                LIMIT :limit OFFSET :offset
                """
            ),
            {"limit": limit, "offset": offset},
        )
        return [dict(row) for row in result.mappings().all()]

    def count_ranked(self) -> int:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT COUNT(DISTINCT sd.university_id)
                FROM delivery.university_search_doc AS sd,
                     jsonb_array_elements(
                         COALESCE(sd.search_document->'ratings', '[]'::jsonb)
                     ) AS rating
                WHERE (rating->>'value') ~ '^[0-9]+(\.[0-9]+)?$'
                """
            )
        )
        return result.scalar() or 0
