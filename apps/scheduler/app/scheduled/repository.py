from __future__ import annotations

from collections.abc import Callable
from typing import Any

from apps.scheduler.app.persistence import json_from_db, sql_text
from apps.scheduler.app.sources.models import CrawlPolicy

from .models import ScheduledEndpointRecord


class ScheduledCrawlRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def list_scheduled_endpoints(self, *, limit: int) -> list[ScheduledEndpointRecord]:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    endpoint.endpoint_id,
                    endpoint.source_id,
                    source.source_key,
                    endpoint.endpoint_url,
                    endpoint.parser_profile,
                    endpoint.crawl_policy,
                    raw_stats.last_observed_at,
                    run_stats.last_attempted_at
                FROM ingestion.source_endpoint AS endpoint
                JOIN ingestion.source AS source
                    ON source.source_id = endpoint.source_id
                LEFT JOIN (
                    SELECT
                        artifact.source_key,
                        artifact.source_url,
                        max(artifact.fetched_at) AS last_observed_at
                    FROM ingestion.raw_artifact AS artifact
                    GROUP BY artifact.source_key, artifact.source_url
                ) AS raw_stats
                    ON raw_stats.source_key = source.source_key
                    AND raw_stats.source_url = endpoint.endpoint_url
                LEFT JOIN (
                    SELECT
                        run.source_key,
                        run.metadata ->> 'endpoint_id' AS endpoint_id,
                        max(run.started_at) AS last_attempted_at
                    FROM ops.pipeline_run AS run
                    WHERE run.run_type = 'crawl'
                    GROUP BY run.source_key, run.metadata ->> 'endpoint_id'
                ) AS run_stats
                    ON run_stats.source_key = source.source_key
                    AND run_stats.endpoint_id = endpoint.endpoint_id::text
                WHERE source.is_active = true
                  AND COALESCE(
                        (endpoint.crawl_policy ->> 'schedule_enabled')::boolean,
                        false
                  ) = true
                ORDER BY
                    COALESCE(raw_stats.last_observed_at, run_stats.last_attempted_at) NULLS FIRST,
                    source.source_key,
                    endpoint.endpoint_url
                LIMIT :limit
                """
            ),
            {"limit": limit},
        )
        return [self._record_from_row(row) for row in result.mappings().all()]

    @staticmethod
    def _record_from_row(row: Any) -> ScheduledEndpointRecord:
        return ScheduledEndpointRecord(
            endpoint_id=row["endpoint_id"],
            source_id=row["source_id"],
            source_key=row["source_key"],
            endpoint_url=row["endpoint_url"],
            parser_profile=row["parser_profile"],
            crawl_policy=CrawlPolicy.model_validate(json_from_db(row["crawl_policy"])),
            last_observed_at=row["last_observed_at"],
            last_attempted_at=row["last_attempted_at"],
        )
