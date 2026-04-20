import hashlib
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from libs.source_sdk import FetchContext, FetchedArtifact
from libs.source_sdk.stores import (
    MinIORawArtifactStore,
    RawArtifactContentError,
    build_sha256_object_key,
    raw_bucket_for_content_type,
)
from libs.storage import MinIOObjectWriteResult


class FakeMinIOStorage:
    def __init__(self) -> None:
        self.ensure_bucket_calls: list[str] = []
        self.put_calls: list[dict] = []

    def ensure_bucket(self, bucket_name: str) -> bool:
        self.ensure_bucket_calls.append(bucket_name)
        return False

    def put_bytes(
        self,
        *,
        bucket_name: str,
        object_name: str,
        payload: bytes,
        content_type: str,
        metadata: dict[str, str] | None = None,
    ) -> MinIOObjectWriteResult:
        self.put_calls.append(
            {
                "bucket_name": bucket_name,
                "object_name": object_name,
                "payload": payload,
                "content_type": content_type,
                "metadata": metadata,
            }
        )
        return MinIOObjectWriteResult(
            bucket_name=bucket_name,
            object_name=object_name,
            etag="etag-1",
            version_id="v1",
        )


def build_context() -> FetchContext:
    return FetchContext(
        crawl_run_id=uuid4(),
        source_key="msu-official",
        endpoint_url="https://example.edu/start",
        parser_profile="official_site.default",
    )


def build_artifact(payload: bytes = b"<html>ok</html>") -> FetchedArtifact:
    return FetchedArtifact(
        raw_artifact_id=uuid4(),
        crawl_run_id=uuid4(),
        source_key="msu-official",
        source_url="https://example.edu/start",
        final_url="https://example.edu/final",
        http_status=200,
        content_type="text/html; charset=utf-8",
        content_length=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        fetched_at=datetime(2026, 4, 20, 10, 0, tzinfo=UTC),
        render_mode="http",
        etag='"abc123"',
        last_modified="Mon, 20 Apr 2026 10:00:00 GMT",
        content=payload,
        metadata={"parser_profile": "official_site.default"},
    )


def test_raw_bucket_and_sha256_object_key_are_content_addressed() -> None:
    sha256 = "abcdef" * 10 + "abcd"

    assert raw_bucket_for_content_type("application/json; charset=utf-8") == "raw-json"
    assert raw_bucket_for_content_type("text/html; charset=utf-8") == "raw-html"
    assert build_sha256_object_key(
        source_key="msu-official",
        sha256=sha256,
        content_type="application/json",
    ) == f"msu-official/ab/cd/{sha256}.json"


@pytest.mark.asyncio
async def test_minio_raw_artifact_store_writes_payload_with_sha256_object_key() -> None:
    context = build_context()
    artifact = build_artifact()
    storage = FakeMinIOStorage()

    stored = await MinIORawArtifactStore(storage).store_raw(context, artifact)

    expected_object_key = build_sha256_object_key(
        source_key="msu-official",
        sha256=artifact.sha256,
        content_type=artifact.content_type,
    )
    assert storage.ensure_bucket_calls == ["raw-html"]
    assert storage.put_calls == [
        {
            "bucket_name": "raw-html",
            "object_name": expected_object_key,
            "payload": b"<html>ok</html>",
            "content_type": "text/html; charset=utf-8",
            "metadata": {
                "crawl-run-id": str(context.crawl_run_id),
                "source-key": "msu-official",
                "source-url": "https://example.edu/start",
                "final-url": "https://example.edu/final",
                "sha256": artifact.sha256,
                "fetched-at": "2026-04-20T10:00:00+00:00",
                "parser-profile": "official_site.default",
                "render-mode": "http",
                "http-status": "200",
                "etag": '"abc123"',
                "last-modified": "Mon, 20 Apr 2026 10:00:00 GMT",
            },
        }
    ]
    assert stored.storage_bucket == "raw-html"
    assert stored.storage_object_key == expected_object_key
    assert stored.metadata["minio_etag"] == "etag-1"
    assert stored.metadata["minio_version_id"] == "v1"
    assert stored.content == artifact.content


@pytest.mark.asyncio
async def test_minio_raw_artifact_store_uses_json_bucket_for_json_payload() -> None:
    payload = b'{"ok": true}'
    context = build_context()
    artifact = build_artifact(payload).model_copy(
        update={
            "content_type": "application/json; charset=utf-8",
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
    )
    storage = FakeMinIOStorage()

    stored = await MinIORawArtifactStore(storage, ensure_bucket=False).store_raw(
        context,
        artifact,
    )

    assert storage.ensure_bucket_calls == []
    assert storage.put_calls[0]["bucket_name"] == "raw-json"
    assert storage.put_calls[0]["object_name"].endswith(f"{artifact.sha256}.json")
    assert stored.storage_bucket == "raw-json"


@pytest.mark.asyncio
async def test_minio_raw_artifact_store_rejects_missing_content() -> None:
    context = build_context()
    artifact = build_artifact().model_copy(update={"content": None})

    with pytest.raises(RawArtifactContentError, match="content is required"):
        await MinIORawArtifactStore(FakeMinIOStorage()).store_raw(context, artifact)


@pytest.mark.asyncio
async def test_minio_raw_artifact_store_rejects_sha256_mismatch() -> None:
    context = build_context()
    artifact = build_artifact().model_copy(update={"sha256": "0" * 64})

    with pytest.raises(RawArtifactContentError, match="sha256 mismatch"):
        await MinIORawArtifactStore(FakeMinIOStorage()).store_raw(context, artifact)
