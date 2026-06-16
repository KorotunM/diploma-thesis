from __future__ import annotations

from collections.abc import Callable
from typing import Any

from apps.backend.app.persistence import sql_text


class ProgramDirectoryRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def list_programs(self) -> list[dict[str, Any]]:
        result = self._session.execute(
            self._sql_text(
                """
                WITH latest_cards AS (
                    SELECT DISTINCT ON (university_id)
                        university_id,
                        card_json
                    FROM delivery.university_card
                    ORDER BY university_id, card_version DESC
                ),
                program_rows AS (
                    SELECT
                        latest_cards.university_id,
                        program
                    FROM latest_cards,
                         jsonb_array_elements(
                            COALESCE(latest_cards.card_json->'programs', '[]'::jsonb)
                         ) AS program
                    WHERE COALESCE(program->>'code', '') <> ''
                ),
                grouped AS (
                    SELECT
                        program->>'code' AS code,
                        COALESCE(MAX(NULLIF(program->>'name', '')), program->>'code') AS name,
                        MAX(NULLIF(program->>'level', '')) AS level,
                        MAX(NULLIF(program->>'description', '')) AS description,
                        COUNT(DISTINCT university_id) AS university_count,
                        SUM(COALESCE(CAST(NULLIF(program->>'budget_places', '') AS integer), 0))
                            AS budget_places,
                        SUM(COALESCE(CAST(NULLIF(program->>'paid_places', '') AS integer), 0))
                            AS paid_places,
                        AVG(CAST(NULLIF(program->>'passing_score', '') AS double precision))
                            AS avg_passing_score,
                        MIN(CAST(NULLIF(program->>'tuition_per_year', '') AS integer))
                            AS min_tuition_per_year,
                        ARRAY_REMOVE(
                            ARRAY_AGG(DISTINCT exam->>'subject')
                                FILTER (WHERE NULLIF(exam->>'subject', '') IS NOT NULL),
                            NULL
                        ) AS ege_subjects
                    FROM program_rows
                    LEFT JOIN LATERAL jsonb_array_elements(
                        COALESCE(program_rows.program->'exams', '[]'::jsonb)
                    ) AS exam ON TRUE
                    GROUP BY program->>'code'
                )
                SELECT *
                FROM grouped
                ORDER BY university_count DESC, budget_places DESC, code ASC
                """
            )
        )
        return [dict(row) for row in result.mappings().all()]
