from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import cache
from time import perf_counter
from typing import Any

from libs.storage.settings import PlatformSettings, PostgresSettings, get_platform_settings


@dataclass(frozen=True)
class SQLAlchemyRuntime:
    create_engine: Callable[..., Any]
    sessionmaker: Callable[..., Any]
    text: Callable[[str], Any]
    database_error: type[Exception]


@dataclass(frozen=True)
class PostgresConnectivityStatus:
    is_available: bool
    latency_ms: float
    host: str
    port: int
    database: str
    error: str | None = None


def _load_sqlalchemy_runtime() -> SQLAlchemyRuntime:
    try:
        from sqlalchemy import create_engine, text
        from sqlalchemy.exc import SQLAlchemyError
        from sqlalchemy.orm import sessionmaker
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "SQLAlchemy PostgreSQL dependencies are not installed. "
            "Install project runtime dependencies before using postgres storage helpers."
        ) from exc

    return SQLAlchemyRuntime(
        create_engine=create_engine,
        sessionmaker=sessionmaker,
        text=text,
        database_error=SQLAlchemyError,
    )


def build_postgres_engine_options(settings: PostgresSettings) -> dict[str, Any]:
    return {
        "pool_pre_ping": settings.pool_pre_ping,
        "pool_size": settings.pool_size,
        "max_overflow": settings.max_overflow,
        "pool_timeout": settings.pool_timeout_seconds,
        "pool_recycle": settings.pool_recycle_seconds,
        "connect_args": {
            "application_name": settings.application_name,
            "connect_timeout": settings.connect_timeout_seconds,
        },
    }


def create_postgres_engine(settings: PostgresSettings) -> Any:
    sqlalchemy_runtime = _load_sqlalchemy_runtime()
    return sqlalchemy_runtime.create_engine(
        settings.sqlalchemy_dsn,
        **build_postgres_engine_options(settings),
    )


def create_postgres_session_factory(
    engine: Any,
    *,
    autoflush: bool = False,
    expire_on_commit: bool = False,
) -> Any:
    sqlalchemy_runtime = _load_sqlalchemy_runtime()
    return sqlalchemy_runtime.sessionmaker(
        bind=engine,
        autoflush=autoflush,
        expire_on_commit=expire_on_commit,
    )


@cache
def get_postgres_engine(
    service_name: str | None = None,
    app_env: str | None = None,
) -> Any:
    settings = get_platform_settings(service_name=service_name, app_env=app_env)
    return create_postgres_engine(settings.postgres)


@cache
def get_postgres_session_factory(
    service_name: str | None = None,
    app_env: str | None = None,
) -> Any:
    engine = get_postgres_engine(service_name=service_name, app_env=app_env)
    return create_postgres_session_factory(engine)


def probe_postgres_connectivity(
    *,
    engine: Any | None = None,
    settings: PostgresSettings | None = None,
    platform_settings: PlatformSettings | None = None,
    service_name: str | None = None,
    app_env: str | None = None,
) -> PostgresConnectivityStatus:
    resolved_platform = platform_settings or get_platform_settings(
        service_name=service_name,
        app_env=app_env,
    )
    resolved_settings = settings or resolved_platform.postgres
    resolved_engine = engine or create_postgres_engine(resolved_settings)
    sqlalchemy_runtime = _load_sqlalchemy_runtime()

    started_at = perf_counter()
    try:
        with resolved_engine.connect() as connection:
            connection.execute(sqlalchemy_runtime.text("SELECT 1"))
    except sqlalchemy_runtime.database_error as exc:
        return PostgresConnectivityStatus(
            is_available=False,
            latency_ms=(perf_counter() - started_at) * 1000,
            host=resolved_settings.host,
            port=resolved_settings.port,
            database=resolved_settings.database,
            error=str(exc),
        )

    return PostgresConnectivityStatus(
        is_available=True,
        latency_ms=(perf_counter() - started_at) * 1000,
        host=resolved_settings.host,
        port=resolved_settings.port,
        database=resolved_settings.database,
    )
