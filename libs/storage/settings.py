from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PlatformSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", env_file_encoding="utf-8")

    app_env: str = Field(default="local", alias="APP_ENV")
    app_version: str = Field(default="0.1.0", alias="APP_VERSION")
    postgres_dsn: str = Field(
        default="postgresql+psycopg://aggregator:aggregator@postgres:5432/aggregator",
        alias="POSTGRES_DSN",
    )
    rabbitmq_url: str = Field(
        default="amqp://aggregator:aggregator@rabbitmq:5672/",
        alias="RABBITMQ_URL",
    )
    minio_endpoint: str = Field(default="http://minio:9000", alias="MINIO_ENDPOINT")
    minio_access_key: str = Field(default="aggregator", alias="MINIO_ROOT_USER")
    minio_secret_key: str = Field(default="aggregator-secret", alias="MINIO_ROOT_PASSWORD")
