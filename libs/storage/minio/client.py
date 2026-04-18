from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from io import BytesIO
from typing import Any

from libs.storage.settings import MinIOSettings, get_platform_settings


@dataclass(frozen=True)
class MinIORuntime:
    client_class: type[Any]
    error_class: type[Exception]


@dataclass(frozen=True)
class BucketReadinessStatus:
    bucket_name: str
    exists: bool
    error: str | None = None


@dataclass(frozen=True)
class MinIOBucketReadiness:
    is_ready: bool
    endpoint: str
    buckets: tuple[BucketReadinessStatus, ...]
    error: str | None = None


@dataclass(frozen=True)
class MinIOObjectWriteResult:
    bucket_name: str
    object_name: str
    etag: str | None = None
    version_id: str | None = None


def _load_minio_runtime() -> MinIORuntime:
    try:
        from minio import Minio
        from minio.error import S3Error
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "MinIO client dependencies are not installed. "
            "Install project runtime dependencies before using minio storage helpers."
        ) from exc

    return MinIORuntime(
        client_class=Minio,
        error_class=S3Error,
    )


def create_minio_client(settings: MinIOSettings) -> Any:
    runtime = _load_minio_runtime()
    return runtime.client_class(
        endpoint=settings.api_endpoint,
        access_key=settings.access_key,
        secret_key=settings.secret_key.get_secret_value(),
        secure=settings.secure,
        region=settings.region,
    )


class MinIOStorageClient:
    def __init__(self, client: Any, settings: MinIOSettings) -> None:
        self._client = client
        self._settings = settings

    @property
    def settings(self) -> MinIOSettings:
        return self._settings

    def bucket_exists(self, bucket_name: str) -> bool:
        return self._client.bucket_exists(bucket_name)

    def ensure_bucket(self, bucket_name: str) -> bool:
        exists = self.bucket_exists(bucket_name)
        if exists:
            return False

        self._client.make_bucket(bucket_name, location=self._settings.region)
        return True

    def ensure_buckets(self, bucket_names: tuple[str, ...] | None = None) -> list[str]:
        created_buckets: list[str] = []
        for bucket_name in bucket_names or self._settings.buckets:
            if self.ensure_bucket(bucket_name):
                created_buckets.append(bucket_name)
        return created_buckets

    def put_bytes(
        self,
        *,
        bucket_name: str,
        object_name: str,
        payload: bytes,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
    ) -> MinIOObjectWriteResult:
        result = self._client.put_object(
            bucket_name,
            object_name,
            data=BytesIO(payload),
            length=len(payload),
            content_type=content_type,
            metadata=metadata,
        )
        return MinIOObjectWriteResult(
            bucket_name=bucket_name,
            object_name=object_name,
            etag=getattr(result, "etag", None),
            version_id=getattr(result, "version_id", None),
        )


def get_minio_storage_client(
    service_name: str | None = None,
    app_env: str | None = None,
) -> MinIOStorageClient:
    settings = get_platform_settings(service_name=service_name, app_env=app_env)
    client = create_minio_client(settings.minio)
    return MinIOStorageClient(client=client, settings=settings.minio)


@cache
def get_minio_storage(
    service_name: str | None = None,
    app_env: str | None = None,
) -> MinIOStorageClient:
    return get_minio_storage_client(service_name=service_name, app_env=app_env)


def probe_minio_bucket_readiness(
    *,
    storage: MinIOStorageClient | None = None,
    settings: MinIOSettings | None = None,
    service_name: str | None = None,
    app_env: str | None = None,
) -> MinIOBucketReadiness:
    resolved_storage = storage or get_minio_storage(service_name=service_name, app_env=app_env)
    resolved_settings = settings or resolved_storage.settings
    error_class: type[Exception] = Exception
    if storage is None:
        error_class = _load_minio_runtime().error_class

    bucket_statuses: list[BucketReadinessStatus] = []
    try:
        for bucket_name in resolved_settings.buckets:
            try:
                exists = resolved_storage.bucket_exists(bucket_name)
            except error_class as exc:
                bucket_statuses.append(
                    BucketReadinessStatus(
                        bucket_name=bucket_name,
                        exists=False,
                        error=str(exc),
                    )
                )
            else:
                bucket_statuses.append(
                    BucketReadinessStatus(
                        bucket_name=bucket_name,
                        exists=exists,
                    )
                )
    except error_class as exc:
        return MinIOBucketReadiness(
            is_ready=False,
            endpoint=resolved_settings.endpoint,
            buckets=tuple(bucket_statuses),
            error=str(exc),
        )

    return MinIOBucketReadiness(
        is_ready=all(bucket.exists and bucket.error is None for bucket in bucket_statuses),
        endpoint=resolved_settings.endpoint,
        buckets=tuple(bucket_statuses),
    )
