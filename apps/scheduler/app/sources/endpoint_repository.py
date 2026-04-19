from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from apps.scheduler.app.persistence import json_from_db, json_to_db, sql_text

from .models import (
    CrawlPolicy,
    CreateSourceEndpointRequest,
    SourceEndpointRecord,
    UpdateSourceEndpointRequest,
)


class SourceNotFoundError(ValueError):
    def __init__(self, source_key: str) -> None:
        super().__init__(f"Source was not found: {source_key}")
        self.source_key = source_key


class SourceEndpointAlreadyExistsError(ValueError):
    def __init__(self, source_key: str, endpoint_url: str) -> None:
        super().__init__(f"Endpoint already exists for source {source_key}: {endpoint_url}")
        self.source_key = source_key
        self.endpoint_url = endpoint_url


class SourceEndpointRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def create(
        self,
        source_key: str,
        request: CreateSourceEndpointRequest,
    ) -> SourceEndpointRecord:
        source_id = self._get_source_id(source_key)
        endpoint_url = request.normalized_endpoint_url

        if self.get_by_url(source_key, endpoint_url) is not None:
            raise SourceEndpointAlreadyExistsError(source_key, endpoint_url)

        result = self._session.execute(
            self._sql_text(
                """
                INSERT INTO ingestion.source_endpoint (
                    endpoint_id,
                    source_id,
                    endpoint_url,
                    parser_profile,
                    crawl_policy
                )
                VALUES (
                    :endpoint_id,
                    :source_id,
                    :endpoint_url,
                    :parser_profile,
                    CAST(:crawl_policy AS jsonb)
                )
                RETURNING
                    endpoint_id,
                    source_id,
                    :source_key AS source_key,
                    endpoint_url,
                    parser_profile,
                    crawl_policy
                """
            ),
            {
                "endpoint_id": request.endpoint_id,
                "source_id": source_id,
                "source_key": source_key,
                "endpoint_url": endpoint_url,
                "parser_profile": request.parser_profile,
                "crawl_policy": json_to_db(request.crawl_policy.model_dump(mode="json")),
            },
        )
        return self._record_from_row(result.mappings().one())

    def get(self, source_key: str, endpoint_id: UUID) -> SourceEndpointRecord | None:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    endpoint.endpoint_id,
                    endpoint.source_id,
                    source.source_key,
                    endpoint.endpoint_url,
                    endpoint.parser_profile,
                    endpoint.crawl_policy
                FROM ingestion.source_endpoint endpoint
                JOIN ingestion.source source ON source.source_id = endpoint.source_id
                WHERE source.source_key = :source_key
                  AND endpoint.endpoint_id = :endpoint_id
                """
            ),
            {"source_key": source_key, "endpoint_id": endpoint_id},
        )
        row = result.mappings().one_or_none()
        if row is None:
            return None
        return self._record_from_row(row)

    def get_by_url(self, source_key: str, endpoint_url: str) -> SourceEndpointRecord | None:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    endpoint.endpoint_id,
                    endpoint.source_id,
                    source.source_key,
                    endpoint.endpoint_url,
                    endpoint.parser_profile,
                    endpoint.crawl_policy
                FROM ingestion.source_endpoint endpoint
                JOIN ingestion.source source ON source.source_id = endpoint.source_id
                WHERE source.source_key = :source_key
                  AND endpoint.endpoint_url = :endpoint_url
                """
            ),
            {"source_key": source_key, "endpoint_url": endpoint_url},
        )
        row = result.mappings().one_or_none()
        if row is None:
            return None
        return self._record_from_row(row)

    def list(
        self,
        source_key: str,
        *,
        limit: int,
        offset: int,
    ) -> tuple[list[SourceEndpointRecord], int]:
        if self._get_source_id(source_key) is None:
            raise SourceNotFoundError(source_key)

        params = {
            "source_key": source_key,
            "limit": limit,
            "offset": offset,
        }
        items_result = self._session.execute(
            self._sql_text(
                """
                SELECT
                    endpoint.endpoint_id,
                    endpoint.source_id,
                    source.source_key,
                    endpoint.endpoint_url,
                    endpoint.parser_profile,
                    endpoint.crawl_policy
                FROM ingestion.source_endpoint endpoint
                JOIN ingestion.source source ON source.source_id = endpoint.source_id
                WHERE source.source_key = :source_key
                ORDER BY endpoint.endpoint_url
                LIMIT :limit
                OFFSET :offset
                """
            ),
            params,
        )
        count_result = self._session.execute(
            self._sql_text(
                """
                SELECT count(*) AS total
                FROM ingestion.source_endpoint endpoint
                JOIN ingestion.source source ON source.source_id = endpoint.source_id
                WHERE source.source_key = :source_key
                """
            ),
            params,
        )

        records = [self._record_from_row(row) for row in items_result.mappings().all()]
        return records, int(count_result.scalar_one())

    def update(
        self,
        source_key: str,
        endpoint_id: UUID,
        request: UpdateSourceEndpointRequest,
    ) -> SourceEndpointRecord | None:
        endpoint_url = request.normalized_endpoint_url
        if endpoint_url is not None:
            existing_endpoint = self.get_by_url(source_key, endpoint_url)
            if existing_endpoint is not None and existing_endpoint.endpoint_id != endpoint_id:
                raise SourceEndpointAlreadyExistsError(source_key, endpoint_url)

        crawl_policy_is_set = "crawl_policy" in request.model_fields_set
        result = self._session.execute(
            self._sql_text(
                """
                UPDATE ingestion.source_endpoint endpoint
                SET
                    endpoint_url = COALESCE(:endpoint_url, endpoint_url),
                    parser_profile = COALESCE(:parser_profile, parser_profile),
                    crawl_policy = CASE
                        WHEN :crawl_policy_is_set THEN CAST(:crawl_policy AS jsonb)
                        ELSE crawl_policy
                    END
                FROM ingestion.source source
                WHERE source.source_id = endpoint.source_id
                  AND source.source_key = :source_key
                  AND endpoint.endpoint_id = :endpoint_id
                RETURNING
                    endpoint.endpoint_id,
                    endpoint.source_id,
                    source.source_key,
                    endpoint.endpoint_url,
                    endpoint.parser_profile,
                    endpoint.crawl_policy
                """
            ),
            {
                "source_key": source_key,
                "endpoint_id": endpoint_id,
                "endpoint_url": endpoint_url,
                "parser_profile": request.parser_profile,
                "crawl_policy_is_set": crawl_policy_is_set,
                "crawl_policy": (
                    json_to_db(request.crawl_policy.model_dump(mode="json"))
                    if crawl_policy_is_set and request.crawl_policy is not None
                    else "{}"
                ),
            },
        )
        row = result.mappings().one_or_none()
        if row is None:
            return None
        return self._record_from_row(row)

    def _get_source_id(self, source_key: str) -> UUID:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT source_id
                FROM ingestion.source
                WHERE source_key = :source_key
                """
            ),
            {"source_key": source_key},
        )
        source_id = result.scalar_one_or_none()
        if source_id is None:
            raise SourceNotFoundError(source_key)
        return source_id

    @staticmethod
    def _record_from_row(row: Any) -> SourceEndpointRecord:
        return SourceEndpointRecord(
            endpoint_id=row["endpoint_id"],
            source_id=row["source_id"],
            source_key=row["source_key"],
            endpoint_url=row["endpoint_url"],
            parser_profile=row["parser_profile"],
            crawl_policy=CrawlPolicy.model_validate(json_from_db(row["crawl_policy"])),
        )
