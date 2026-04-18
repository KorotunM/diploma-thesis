from libs.storage.minio.client import (
    MinIOStorageClient,
    create_minio_client,
    probe_minio_bucket_readiness,
)
from libs.storage.settings import PlatformSettings


class FakeS3Error(Exception):
    pass


class FakePutResult:
    def __init__(self, etag: str, version_id: str | None) -> None:
        self.etag = etag
        self.version_id = version_id


class FakeMinioClientClass:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs


class FakeMinioClient:
    def __init__(
        self,
        existing_buckets: set[str] | None = None,
        failing_buckets: set[str] | None = None,
    ) -> None:
        self.existing_buckets = set(existing_buckets or set())
        self.failing_buckets = set(failing_buckets or set())
        self.created_buckets: list[tuple[str, str]] = []
        self.put_calls: list[dict[str, object]] = []

    def bucket_exists(self, bucket_name: str) -> bool:
        if bucket_name in self.failing_buckets:
            raise FakeS3Error(f"bucket check failed: {bucket_name}")
        return bucket_name in self.existing_buckets

    def make_bucket(self, bucket_name: str, location: str) -> None:
        self.created_buckets.append((bucket_name, location))
        self.existing_buckets.add(bucket_name)

    def put_object(
        self,
        bucket_name: str,
        object_name: str,
        *,
        data,
        length: int,
        content_type: str,
        metadata: dict[str, str] | None,
    ) -> FakePutResult:
        self.put_calls.append(
            {
                "bucket_name": bucket_name,
                "object_name": object_name,
                "payload": data.read(),
                "length": length,
                "content_type": content_type,
                "metadata": metadata,
            }
        )
        return FakePutResult(etag="etag-1", version_id="v1")


class FakeRuntime:
    client_class = FakeMinioClientClass
    error_class = FakeS3Error


def test_create_minio_client_uses_normalized_settings_endpoint(monkeypatch) -> None:
    monkeypatch.setattr("libs.storage.minio.client._load_minio_runtime", lambda: FakeRuntime())

    settings = PlatformSettings(service_name="parser").minio
    client = create_minio_client(settings)

    assert isinstance(client, FakeMinioClientClass)
    assert client.kwargs == {
        "endpoint": "minio:9000",
        "access_key": "aggregator",
        "secret_key": "aggregator-secret",
        "secure": False,
        "region": "us-east-1",
    }


def test_minio_storage_client_ensure_buckets_creates_only_missing() -> None:
    settings = PlatformSettings(service_name="parser").minio
    client = FakeMinioClient(existing_buckets={"raw-html"})
    storage = MinIOStorageClient(client=client, settings=settings)

    created = storage.ensure_buckets(("raw-html", "raw-json", "exports"))

    assert created == ["raw-json", "exports"]
    assert client.created_buckets == [
        ("raw-json", "us-east-1"),
        ("exports", "us-east-1"),
    ]


def test_minio_storage_client_put_bytes_wraps_payload_and_returns_result() -> None:
    settings = PlatformSettings(service_name="parser").minio
    client = FakeMinioClient(existing_buckets={"raw-html"})
    storage = MinIOStorageClient(client=client, settings=settings)

    result = storage.put_bytes(
        bucket_name="raw-html",
        object_name="objects/abc.html",
        payload=b"hello world",
        content_type="text/html",
        metadata={"sha256": "abc"},
    )

    assert result.bucket_name == "raw-html"
    assert result.object_name == "objects/abc.html"
    assert result.etag == "etag-1"
    assert result.version_id == "v1"
    assert client.put_calls == [
        {
            "bucket_name": "raw-html",
            "object_name": "objects/abc.html",
            "payload": b"hello world",
            "length": 11,
            "content_type": "text/html",
            "metadata": {"sha256": "abc"},
        }
    ]


def test_probe_minio_bucket_readiness_reports_missing_buckets() -> None:
    settings = PlatformSettings(service_name="parser").minio
    client = FakeMinioClient(existing_buckets={"raw-html", "raw-json"})
    storage = MinIOStorageClient(client=client, settings=settings)

    readiness = probe_minio_bucket_readiness(storage=storage, settings=settings)

    assert readiness.is_ready is False
    assert readiness.endpoint == "http://minio:9000"
    assert [bucket.bucket_name for bucket in readiness.buckets] == list(settings.buckets)
    assert readiness.buckets[0].exists is True
    assert readiness.buckets[2].exists is False


def test_probe_minio_bucket_readiness_captures_bucket_check_errors(monkeypatch) -> None:
    monkeypatch.setattr("libs.storage.minio.client._load_minio_runtime", lambda: FakeRuntime())

    settings = PlatformSettings(service_name="parser").minio
    client = FakeMinioClient(
        existing_buckets={"raw-html"},
        failing_buckets={"parsed-snapshots"},
    )
    storage = MinIOStorageClient(client=client, settings=settings)

    readiness = probe_minio_bucket_readiness(storage=storage, settings=settings)

    assert readiness.is_ready is False
    errored_bucket = next(
        bucket
        for bucket in readiness.buckets
        if bucket.bucket_name == "parsed-snapshots"
    )
    assert errored_bucket.exists is False
    assert errored_bucket.error == "bucket check failed: parsed-snapshots"
