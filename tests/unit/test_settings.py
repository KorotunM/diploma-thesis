from pathlib import Path

from libs.storage.settings import PlatformSettings, get_platform_settings


def test_platform_settings_build_connection_values_from_shared_env_file(tmp_path: Path) -> None:
    env_file = tmp_path / "app.env"
    env_file.write_text(
        "\n".join(
            [
                "APP_ENV=test",
                "APP_VERSION=0.2.0",
                "POSTGRES_HOST=db.internal",
                "POSTGRES_PORT=5433",
                "POSTGRES_DB=universities",
                "POSTGRES_USER=reader",
                "POSTGRES_PASSWORD=secret pass",
                "RABBITMQ_HOST=broker.internal",
                "RABBITMQ_PORT=5673",
                "RABBITMQ_DEFAULT_USER=publisher",
                "RABBITMQ_DEFAULT_PASS=broker pass",
                "RABBITMQ_VHOST=/events",
                "MINIO_ENDPOINT=http://object-storage:9100",
                "MINIO_ROOT_USER=minio-user",
                "MINIO_ROOT_PASSWORD=minio pass",
            ]
        ),
        encoding="utf-8",
    )

    settings = PlatformSettings(service_name="backend", _env_file=(env_file,))

    assert settings.service.environment == "test"
    assert settings.service.port == 8004
    assert settings.postgres.sqlalchemy_dsn == (
        "postgresql+psycopg://reader:secret%20pass@db.internal:5433/universities"
    )
    assert settings.postgres.connect_timeout_seconds == 5
    assert settings.postgres.pool_size == 5
    assert settings.postgres.max_overflow == 10
    assert settings.rabbitmq.url == "amqp://publisher:broker%20pass@broker.internal:5673/events"
    assert settings.rabbitmq.heartbeat_seconds == 30
    assert settings.rabbitmq.prefetch_count == 16
    assert settings.minio.endpoint == "http://object-storage:9100"


def test_platform_settings_loads_shared_and_service_specific_env_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    env_root = tmp_path / "infra" / "env" / "review"
    env_root.mkdir(parents=True)

    (env_root / "app.env").write_text(
        "\n".join(
            [
                "APP_VERSION=0.3.0",
                "SERVICE_PORT=8010",
                "POSTGRES_DB=shared_db",
            ]
        ),
        encoding="utf-8",
    )
    (env_root / "scheduler.env").write_text(
        "\n".join(
            [
                "SERVICE_PORT=9010",
                "POSTGRES_DB=scheduler_db",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("libs.storage.settings.REPO_ROOT", tmp_path)
    get_platform_settings.cache_clear()

    settings = get_platform_settings(service_name="scheduler", app_env="review")

    assert settings.service.name == "scheduler"
    assert settings.service.port == 9010
    assert settings.postgres.database == "scheduler_db"
    assert settings.app_version == "0.3.0"

    get_platform_settings.cache_clear()
