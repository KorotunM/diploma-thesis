from __future__ import annotations

from collections.abc import Callable
from typing import Any

from apps.parser.app.persistence import json_from_db, json_to_db, sql_text
from libs.source_sdk import FetchContext, FetchedArtifact

from .models import RawArtifactRecord


class RawArtifactPersistenceError(ValueError):
    pass


class RawArtifactRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def upsert_from_artifact(
        self,
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> RawArtifactRecord:
        self._validate_persistable_artifact(artifact)
        result = self._session.execute(
            self._sql_text(
                """
                INSERT INTO ingestion.raw_artifact (
                    raw_artifact_id,
                    crawl_run_id,
                    source_key,
                    source_url,
                    final_url,
                    http_status,
                    content_type,
                    content_length,
                    sha256,
                    storage_bucket,
                    storage_object_key,
                    etag,
                    last_modified,
                    fetched_at,
                    metadata
                )
                VALUES (
                    :raw_artifact_id,
                    :crawl_run_id,
                    :source_key,
                    :source_url,
                    :final_url,
                    :http_status,
                    :content_type,
                    :content_length,
                    :sha256,
                    :storage_bucket,
                    :storage_object_key,
                    :etag,
                    :last_modified,
                    :fetched_at,
                    CAST(:metadata AS jsonb)
                )
                ON CONFLICT (source_key, sha256)
                DO UPDATE SET
                    crawl_run_id = EXCLUDED.crawl_run_id,
                    source_url = EXCLUDED.source_url,
                    final_url = EXCLUDED.final_url,
                    http_status = EXCLUDED.http_status,
                    content_type = EXCLUDED.content_type,
                    content_length = EXCLUDED.content_length,
                    storage_bucket = EXCLUDED.storage_bucket,
                    storage_object_key = EXCLUDED.storage_object_key,
                    etag = EXCLUDED.etag,
                    last_modified = EXCLUDED.last_modified,
                    fetched_at = EXCLUDED.fetched_at,
                    metadata = ingestion.raw_artifact.metadata || EXCLUDED.metadata
                RETURNING
                    raw_artifact_id,
                    crawl_run_id,
                    source_key,
                    source_url,
                    final_url,
                    http_status,
                    content_type,
                    content_length,
                    sha256,
                    storage_bucket,
                    storage_object_key,
                    etag,
                    last_modified,
                    fetched_at,
                    metadata
                """
            ),
            {
                "raw_artifact_id": artifact.raw_artifact_id,
                "crawl_run_id": context.crawl_run_id,
                "source_key": context.source_key,
                "source_url": artifact.source_url,
                "final_url": artifact.final_url,
                "http_status": artifact.http_status,
                "content_type": artifact.content_type,
                "content_length": artifact.content_length,
                "sha256": artifact.sha256,
                "storage_bucket": artifact.storage_bucket,
                "storage_object_key": artifact.storage_object_key,
                "etag": artifact.etag,
                "last_modified": artifact.last_modified,
                "fetched_at": artifact.fetched_at,
                "metadata": json_to_db(self._build_metadata(context=context, artifact=artifact)),
            },
        )
        return self._record_from_row(result.mappings().one())

    def commit(self) -> None:
        self._session.commit()

    @staticmethod
    def _validate_persistable_artifact(artifact: FetchedArtifact) -> None:
        if not artifact.storage_bucket:
            raise RawArtifactPersistenceError(
                "Fetched artifact must have storage_bucket before metadata persistence."
            )
        if not artifact.storage_object_key:
            raise RawArtifactPersistenceError(
                "Fetched artifact must have storage_object_key before metadata persistence."
            )

    @staticmethod
    def _build_metadata(
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> dict[str, Any]:
        return {
            **artifact.metadata,
            "parser_profile": context.parser_profile,
            "priority": context.priority,
            "trigger": context.trigger,
            "requested_at": context.requested_at.isoformat(),
            "render_mode": artifact.render_mode,
            "response_headers": artifact.response_headers,
        }

    @staticmethod
    def _record_from_row(row: Any) -> RawArtifactRecord:
        return RawArtifactRecord(
            raw_artifact_id=row["raw_artifact_id"],
            crawl_run_id=row["crawl_run_id"],
            source_key=row["source_key"],
            source_url=row["source_url"],
            final_url=row["final_url"],
            http_status=row["http_status"],
            content_type=row["content_type"],
            content_length=row["content_length"],
            sha256=row["sha256"],
            storage_bucket=row["storage_bucket"],
            storage_object_key=row["storage_object_key"],
            etag=row["etag"],
            last_modified=row["last_modified"],
            fetched_at=row["fetched_at"],
            metadata=json_from_db(row["metadata"]),
        )
