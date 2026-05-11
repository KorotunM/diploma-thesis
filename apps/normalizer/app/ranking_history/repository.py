from __future__ import annotations

from typing import Any

from apps.normalizer.app.persistence import sql_text

from .models import RankingHistoryRecord


class RankingHistoryRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    def upsert(self, record: RankingHistoryRecord) -> None:
        self._session.execute(
            sql_text(
                """
                INSERT INTO core.university_ranking_history (
                    university_id, source_key, provider, year, rank,
                    score, category, change_direction, change_delta,
                    canonical_name, external_id
                ) VALUES (
                    :university_id, :source_key, :provider, :year, :rank,
                    :score, :category, :change_direction, :change_delta,
                    :canonical_name, :external_id
                )
                ON CONFLICT (source_key, year, rank)
                DO UPDATE SET
                    university_id    = EXCLUDED.university_id,
                    score            = EXCLUDED.score,
                    category         = EXCLUDED.category,
                    change_direction = EXCLUDED.change_direction,
                    change_delta     = EXCLUDED.change_delta,
                    canonical_name   = EXCLUDED.canonical_name,
                    external_id      = EXCLUDED.external_id,
                    captured_at      = now()
                """
            ),
            {
                "university_id": str(record.university_id) if record.university_id else None,
                "source_key": record.source_key,
                "provider": record.provider,
                "year": record.year,
                "rank": record.rank,
                "score": record.score,
                "category": record.category,
                "change_direction": record.change_direction,
                "change_delta": record.change_delta,
                "canonical_name": record.canonical_name,
                "external_id": record.external_id,
            },
        )
        self._session.commit()
