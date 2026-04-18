from __future__ import annotations

import os
from functools import cache
from pathlib import Path
from urllib.parse import quote

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SERVICE_PORTS = {
    "scheduler": 8001,
    "parser": 8002,
    "normalizer": 8003,
    "backend": 8004,
    "frontend": 5173,
}


def _secret_value(secret: SecretStr) -> str:
    return quote(secret.get_secret_value(), safe="")


def _normalize_vhost(vhost: str) -> str:
    normalized = vhost.strip()
    if normalized in {"", "/"}:
        return "%2F"
    return quote(normalized.lstrip("/"), safe="")


class ServiceSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    environment: str
    version: str
    host: str
    port: int
    reload: bool = False

    @property
    def bind_address(self) -> str:
        return f"{self.host}:{self.port}"


class PostgresSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str
    port: int
    database: str
    user: str
    password: SecretStr
    driver: str
    application_name: str
    dsn_override: str | None = None
    connect_timeout_seconds: int
    pool_size: int
    max_overflow: int
    pool_timeout_seconds: int
    pool_recycle_seconds: int
    pool_pre_ping: bool

    @property
    def sqlalchemy_dsn(self) -> str:
        if self.dsn_override:
            return self.dsn_override

        return (
            f"postgresql+{self.driver}://{quote(self.user, safe='')}:{_secret_value(self.password)}"
            f"@{self.host}:{self.port}/{self.database}"
        )


class RabbitMQSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str
    port: int
    user: str
    password: SecretStr
    vhost: str
    management_port: int
    url_override: str | None = None

    @property
    def url(self) -> str:
        if self.url_override:
            return self.url_override

        return (
            f"amqp://{quote(self.user, safe='')}:{_secret_value(self.password)}"
            f"@{self.host}:{self.port}/{_normalize_vhost(self.vhost)}"
        )

    @property
    def management_url(self) -> str:
        return f"http://{self.host}:{self.management_port}"


class MinIOSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    endpoint_override: str | None = None
    host: str
    port: int
    access_key: str
    secret_key: SecretStr
    secure: bool
    console_port: int
    region: str
    buckets: tuple[str, ...] = (
        "raw-html",
        "raw-json",
        "parsed-snapshots",
        "llm-assist",
        "exports",
    )

    @property
    def endpoint(self) -> str:
        if self.endpoint_override:
            return self.endpoint_override.rstrip("/")

        scheme = "https" if self.secure else "http"
        return f"{scheme}://{self.host}:{self.port}"

    @property
    def console_url(self) -> str:
        scheme = "https" if self.secure else "http"
        return f"{scheme}://{self.host}:{self.console_port}"


class PlatformSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
        env_file_encoding="utf-8",
        populate_by_name=True,
    )

    app_env: str = Field(default="local", validation_alias=AliasChoices("APP_ENV"))
    app_version: str = Field(default="0.1.0", validation_alias=AliasChoices("APP_VERSION"))

    service_name: str = Field(default="app", validation_alias=AliasChoices("SERVICE_NAME"))
    service_host: str = Field(default="0.0.0.0", validation_alias=AliasChoices("SERVICE_HOST"))
    service_port: int | None = Field(default=None, validation_alias=AliasChoices("SERVICE_PORT"))
    service_reload: bool = Field(default=False, validation_alias=AliasChoices("SERVICE_RELOAD"))

    postgres_host: str = Field(default="postgres", validation_alias=AliasChoices("POSTGRES_HOST"))
    postgres_port: int = Field(default=5432, validation_alias=AliasChoices("POSTGRES_PORT"))
    postgres_db: str = Field(default="aggregator", validation_alias=AliasChoices("POSTGRES_DB"))
    postgres_user: str = Field(default="aggregator", validation_alias=AliasChoices("POSTGRES_USER"))
    postgres_password: SecretStr = Field(
        default=SecretStr("aggregator"),
        validation_alias=AliasChoices("POSTGRES_PASSWORD"),
    )
    postgres_driver: str = Field(
        default="psycopg",
        validation_alias=AliasChoices("POSTGRES_DRIVER"),
    )
    postgres_application_name: str = Field(
        default="diploma-thesis",
        validation_alias=AliasChoices("POSTGRES_APPLICATION_NAME"),
    )
    postgres_dsn_override: str | None = Field(
        default=None,
        validation_alias=AliasChoices("POSTGRES_DSN"),
    )
    postgres_connect_timeout_seconds: int = Field(
        default=5,
        validation_alias=AliasChoices("POSTGRES_CONNECT_TIMEOUT_SECONDS"),
    )
    postgres_pool_size: int = Field(
        default=5,
        validation_alias=AliasChoices("POSTGRES_POOL_SIZE"),
    )
    postgres_max_overflow: int = Field(
        default=10,
        validation_alias=AliasChoices("POSTGRES_MAX_OVERFLOW"),
    )
    postgres_pool_timeout_seconds: int = Field(
        default=30,
        validation_alias=AliasChoices("POSTGRES_POOL_TIMEOUT_SECONDS"),
    )
    postgres_pool_recycle_seconds: int = Field(
        default=1800,
        validation_alias=AliasChoices("POSTGRES_POOL_RECYCLE_SECONDS"),
    )
    postgres_pool_pre_ping: bool = Field(
        default=True,
        validation_alias=AliasChoices("POSTGRES_POOL_PRE_PING"),
    )

    rabbitmq_host: str = Field(default="rabbitmq", validation_alias=AliasChoices("RABBITMQ_HOST"))
    rabbitmq_port: int = Field(default=5672, validation_alias=AliasChoices("RABBITMQ_PORT"))
    rabbitmq_user: str = Field(
        default="aggregator",
        validation_alias=AliasChoices("RABBITMQ_USER", "RABBITMQ_DEFAULT_USER"),
    )
    rabbitmq_password: SecretStr = Field(
        default=SecretStr("aggregator"),
        validation_alias=AliasChoices("RABBITMQ_PASSWORD", "RABBITMQ_DEFAULT_PASS"),
    )
    rabbitmq_vhost: str = Field(default="/", validation_alias=AliasChoices("RABBITMQ_VHOST"))
    rabbitmq_management_port: int = Field(
        default=15672,
        validation_alias=AliasChoices("RABBITMQ_MANAGEMENT_PORT"),
    )
    rabbitmq_url_override: str | None = Field(
        default=None,
        validation_alias=AliasChoices("RABBITMQ_URL"),
    )

    minio_endpoint_override: str | None = Field(
        default=None,
        validation_alias=AliasChoices("MINIO_ENDPOINT"),
    )
    minio_host: str = Field(default="minio", validation_alias=AliasChoices("MINIO_HOST"))
    minio_port: int = Field(default=9000, validation_alias=AliasChoices("MINIO_PORT"))
    minio_access_key: str = Field(
        default="aggregator",
        validation_alias=AliasChoices("MINIO_ACCESS_KEY", "MINIO_ROOT_USER"),
    )
    minio_secret_key: SecretStr = Field(
        default=SecretStr("aggregator-secret"),
        validation_alias=AliasChoices("MINIO_SECRET_KEY", "MINIO_ROOT_PASSWORD"),
    )
    minio_secure: bool = Field(default=False, validation_alias=AliasChoices("MINIO_SECURE"))
    minio_console_port: int = Field(
        default=9001,
        validation_alias=AliasChoices("MINIO_CONSOLE_PORT"),
    )
    minio_region: str = Field(default="us-east-1", validation_alias=AliasChoices("MINIO_REGION"))

    @classmethod
    def default_env_files(
        cls,
        *,
        app_env: str | None = None,
        service_name: str | None = None,
    ) -> tuple[Path, ...]:
        resolved_app_env = app_env or os.getenv("APP_ENV", "local")
        resolved_service_name = service_name or os.getenv("SERVICE_NAME")

        candidates = [REPO_ROOT / "infra" / "env" / resolved_app_env / "app.env"]
        if resolved_service_name:
            candidates.append(
                REPO_ROOT / "infra" / "env" / resolved_app_env / f"{resolved_service_name}.env"
            )

        return tuple(path for path in candidates if path.exists())

    @classmethod
    def load(
        cls,
        *,
        service_name: str | None = None,
        app_env: str | None = None,
    ) -> PlatformSettings:
        resolved_service_name = service_name or os.getenv("SERVICE_NAME", "app")
        resolved_app_env = app_env or os.getenv("APP_ENV", "local")

        init_kwargs: dict[str, object] = {
            "service_name": resolved_service_name,
            "app_env": resolved_app_env,
        }
        env_files = cls.default_env_files(
            app_env=resolved_app_env,
            service_name=resolved_service_name,
        )
        if env_files:
            init_kwargs["_env_file"] = env_files

        return cls(**init_kwargs)

    @property
    def service(self) -> ServiceSettings:
        return ServiceSettings(
            name=self.service_name,
            environment=self.app_env,
            version=self.app_version,
            host=self.service_host,
            port=self.service_port or DEFAULT_SERVICE_PORTS.get(self.service_name, 8000),
            reload=self.service_reload,
        )

    @property
    def postgres(self) -> PostgresSettings:
        return PostgresSettings(
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
            user=self.postgres_user,
            password=self.postgres_password,
            driver=self.postgres_driver,
            application_name=self.postgres_application_name,
            dsn_override=self.postgres_dsn_override,
            connect_timeout_seconds=self.postgres_connect_timeout_seconds,
            pool_size=self.postgres_pool_size,
            max_overflow=self.postgres_max_overflow,
            pool_timeout_seconds=self.postgres_pool_timeout_seconds,
            pool_recycle_seconds=self.postgres_pool_recycle_seconds,
            pool_pre_ping=self.postgres_pool_pre_ping,
        )

    @property
    def rabbitmq(self) -> RabbitMQSettings:
        return RabbitMQSettings(
            host=self.rabbitmq_host,
            port=self.rabbitmq_port,
            user=self.rabbitmq_user,
            password=self.rabbitmq_password,
            vhost=self.rabbitmq_vhost,
            management_port=self.rabbitmq_management_port,
            url_override=self.rabbitmq_url_override,
        )

    @property
    def minio(self) -> MinIOSettings:
        return MinIOSettings(
            endpoint_override=self.minio_endpoint_override,
            host=self.minio_host,
            port=self.minio_port,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=self.minio_secure,
            console_port=self.minio_console_port,
            region=self.minio_region,
        )


@cache
def get_platform_settings(
    service_name: str | None = None,
    app_env: str | None = None,
) -> PlatformSettings:
    return PlatformSettings.load(service_name=service_name, app_env=app_env)
