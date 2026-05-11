from __future__ import annotations

from collections.abc import Callable
from typing import Any

from apps.backend.app.persistence import sql_text


class LocationSuggestRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def suggest_regions(self, q: str, limit: int) -> list[str]:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT DISTINCT region_name
                FROM delivery.university_search_doc
                WHERE region_name IS NOT NULL
                  AND region_name != ''
                  AND lower(region_name) LIKE lower(:prefix)
                ORDER BY region_name
                LIMIT :limit
                """
            ),
            {"prefix": f"{q}%", "limit": limit},
        )
        return [row[0] for row in result.all()]

    def suggest_cities(self, q: str, limit: int) -> list[str]:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT DISTINCT city_name
                FROM delivery.university_search_doc
                WHERE city_name IS NOT NULL
                  AND city_name != ''
                  AND lower(city_name) LIKE lower(:prefix)
                ORDER BY city_name
                LIMIT :limit
                """
            ),
            {"prefix": f"{q}%", "limit": limit},
        )
        return [row[0] for row in result.all()]

    def suggest_cities_by_region(self, region: str, limit: int) -> list[str]:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT DISTINCT city_name
                FROM delivery.university_search_doc
                WHERE city_name IS NOT NULL
                  AND city_name != ''
                  AND lower(region_name) = lower(:region)
                ORDER BY city_name
                LIMIT :limit
                """
            ),
            {"region": region, "limit": limit},
        )
        return [row[0] for row in result.all()]
