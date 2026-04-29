from __future__ import annotations

from collections.abc import Callable
from typing import Any

from apps.scheduler.app.persistence import json_from_db, json_to_db, sql_text

from .models import SourceFreshnessContext


class SourceFreshnessRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def list_sources(self, *, include_inactive: bool) -> list[SourceFreshnessContext]:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    source.source_id,
                    source.source_key,
                    source.source_type,
                    source.trust_tier,
                    source.is_active,
                    source.metadata,
                    COALESCE(endpoint_stats.endpoint_count, 0) AS endpoint_count,
                    COALESCE(
                        endpoint_stats.scheduled_endpoint_count,
                        0
                    ) AS scheduled_endpoint_count,
                    endpoint_stats.refresh_interval_seconds,
                    COALESCE(endpoint_stats.endpoint_urls, ARRAY[]::text[]) AS endpoint_urls,
                    raw_stats.last_observed_at,
                    run_stats.last_attempted_at
                FROM ingestion.source AS source
                LEFT JOIN (
                    SELECT
                        endpoint.source_id,
                        count(*) AS endpoint_count,
                        count(*) FILTER (
                            WHERE COALESCE(
                                (endpoint.crawl_policy ->> 'schedule_enabled')::boolean,
                                false
                            )
                        ) AS scheduled_endpoint_count,
                        min(
                            CASE
                                WHEN COALESCE(
                                    (endpoint.crawl_policy ->> 'schedule_enabled')::boolean,
                                    false
                                )
                                THEN NULLIF(
                                    (endpoint.crawl_policy ->> 'interval_seconds')::integer,
                                    0
                                )
                                ELSE NULL
                            END
                        ) AS refresh_interval_seconds,
                        array_agg(
                            endpoint.endpoint_url
                            ORDER BY endpoint.endpoint_url
                        ) AS endpoint_urls
                    FROM ingestion.source_endpoint AS endpoint
                    GROUP BY endpoint.source_id
                ) AS endpoint_stats
                    ON endpoint_stats.source_id = source.source_id
                LEFT JOIN (
                    SELECT
                        artifact.source_key,
                        max(artifact.fetched_at) AS last_observed_at
                    FROM ingestion.raw_artifact AS artifact
                    GROUP BY artifact.source_key
                ) AS raw_stats
                    ON raw_stats.source_key = source.source_key
                LEFT JOIN (
                    SELECT
                        run.source_key,
                        max(run.started_at) AS last_attempted_at
                    FROM ops.pipeline_run AS run
                    WHERE run.run_type = 'crawl'
                    GROUP BY run.source_key
                ) AS run_stats
                    ON run_stats.source_key = source.source_key
                WHERE (:include_inactive OR source.is_active = true)
                ORDER BY source.source_key
                """
            ),
            {"include_inactive": include_inactive},
        )
        return [self._record_from_row(row) for row in result.mappings().all()]

    def merge_metadata(self, *, source_key: str, metadata_patch: dict[str, Any]) -> None:
        self._session.execute(
            self._sql_text(
                """
                UPDATE ingestion.source
                SET metadata = metadata || CAST(:metadata_patch AS jsonb)
                WHERE source_key = :source_key
                """
            ),
            {
                "source_key": source_key,
                "metadata_patch": json_to_db(metadata_patch),
            },
        )

    @staticmethod
    def _record_from_row(row: Any) -> SourceFreshnessContext:
        return SourceFreshnessContext(
            source_id=row["source_id"],
            source_key=row["source_key"],
            source_type=row["source_type"],
            trust_tier=row["trust_tier"],
            is_active=row["is_active"],
            endpoint_count=int(row["endpoint_count"] or 0),
            scheduled_endpoint_count=int(row["scheduled_endpoint_count"] or 0),
            refresh_interval_seconds=row["refresh_interval_seconds"],
            endpoint_urls=list(row["endpoint_urls"] or []),
            last_observed_at=row["last_observed_at"],
            last_attempted_at=row["last_attempted_at"],
            metadata=json_from_db(row["metadata"]),
        )
