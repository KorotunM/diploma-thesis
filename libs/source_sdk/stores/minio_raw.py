from __future__ import annotations

import hashlib
from typing import Any

from libs.source_sdk.base_adapter import FetchContext, FetchedArtifact
from libs.source_sdk.fetchers import content_media_type
from libs.storage import MinIOObjectWriteResult, MinIOStorageClient

RAW_BUCKET_BY_MEDIA_TYPE = {
    "application/json": "raw-json",
    "application/x-ndjson": "raw-json",
    "text/html": "raw-html",
    "text/plain": "raw-html",
    "text/xml": "raw-html",
    "application/xml": "raw-html",
}

OBJECT_EXTENSION_BY_MEDIA_TYPE = {
    "application/json": "json",
    "application/x-ndjson": "ndjson",
    "text/html": "html",
    "text/plain": "txt",
    "text/xml": "xml",
    "application/xml": "xml",
}


class RawArtifactContentError(ValueError):
    pass


def raw_bucket_for_content_type(content_type: str) -> str:
    media_type = content_media_type(content_type)
    return RAW_BUCKET_BY_MEDIA_TYPE.get(media_type, "raw-html")


def object_extension_for_content_type(content_type: str) -> str:
    media_type = content_media_type(content_type)
    return OBJECT_EXTENSION_BY_MEDIA_TYPE.get(media_type, "bin")


def build_sha256_object_key(
    *,
    source_key: str,
    sha256: str,
    content_type: str,
) -> str:
    extension = object_extension_for_content_type(content_type)
    return f"{source_key}/{sha256[0:2]}/{sha256[2:4]}/{sha256}.{extension}"


def build_raw_artifact_metadata(
    *,
    context: FetchContext,
    artifact: FetchedArtifact,
) -> dict[str, str]:
    metadata = {
        "crawl-run-id": str(context.crawl_run_id),
        "source-key": context.source_key,
        "source-url": artifact.source_url,
        "final-url": artifact.final_url or artifact.source_url,
        "sha256": artifact.sha256,
        "fetched-at": artifact.fetched_at.isoformat(),
        "parser-profile": context.parser_profile,
        "render-mode": artifact.render_mode,
    }
    if artifact.http_status is not None:
        metadata["http-status"] = str(artifact.http_status)
    if artifact.etag:
        metadata["etag"] = artifact.etag
    if artifact.last_modified:
        metadata["last-modified"] = artifact.last_modified
    return metadata


class MinIORawArtifactStore:
    def __init__(
        self,
        storage: MinIOStorageClient,
        *,
        ensure_bucket: bool = True,
    ) -> None:
        self._storage = storage
        self._ensure_bucket = ensure_bucket

    async def store_raw(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> FetchedArtifact:
        payload = self._validated_payload(artifact)
        bucket_name = raw_bucket_for_content_type(artifact.content_type)
        object_name = build_sha256_object_key(
            source_key=context.source_key,
            sha256=artifact.sha256,
            content_type=artifact.content_type,
        )
        if self._ensure_bucket:
            self._storage.ensure_bucket(bucket_name)

        write_result = self._storage.put_bytes(
            bucket_name=bucket_name,
            object_name=object_name,
            payload=payload,
            content_type=artifact.content_type,
            metadata=build_raw_artifact_metadata(context=context, artifact=artifact),
        )
        return self._artifact_with_storage_result(
            artifact=artifact,
            write_result=write_result,
        )

    @staticmethod
    def _validated_payload(artifact: FetchedArtifact) -> bytes:
        if artifact.content is None:
            raise RawArtifactContentError("Fetched artifact content is required for raw storage.")
        actual_sha256 = hashlib.sha256(artifact.content).hexdigest()
        if actual_sha256 != artifact.sha256:
            raise RawArtifactContentError(
                "Fetched artifact sha256 mismatch: "
                f"expected {artifact.sha256}, got {actual_sha256}."
            )
        return artifact.content

    @staticmethod
    def _artifact_with_storage_result(
        *,
        artifact: FetchedArtifact,
        write_result: MinIOObjectWriteResult,
    ) -> FetchedArtifact:
        storage_metadata: dict[str, Any] = {
            **artifact.metadata,
            "minio_etag": write_result.etag,
            "minio_version_id": write_result.version_id,
        }
        return artifact.model_copy(
            update={
                "storage_bucket": write_result.bucket_name,
                "storage_object_key": write_result.object_name,
                "metadata": storage_metadata,
            }
        )
