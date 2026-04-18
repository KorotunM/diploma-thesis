from libs.storage.postgres.engine import (
    build_postgres_engine_options,
    create_postgres_engine,
    create_postgres_session_factory,
    probe_postgres_connectivity,
)
from libs.storage.settings import PlatformSettings


class FakeDatabaseError(Exception):
    pass


class FakeConnection:
    def __init__(self, *, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.executed: list[str] = []

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, statement: str) -> None:
        self.executed.append(statement)
        if self.should_fail:
            raise FakeDatabaseError("postgres unavailable")


class FakeEngine:
    def __init__(self, *, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.connections: list[FakeConnection] = []

    def connect(self) -> FakeConnection:
        connection = FakeConnection(should_fail=self.should_fail)
        self.connections.append(connection)
        return connection


class FakeSessionFactory:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs


class FakeRuntime:
    def __init__(self) -> None:
        self.engine_calls: list[tuple[str, dict]] = []
        self.session_calls: list[dict] = []

    def create_engine(self, dsn: str, **kwargs):
        self.engine_calls.append((dsn, kwargs))
        return FakeEngine()

    def sessionmaker(self, **kwargs):
        self.session_calls.append(kwargs)
        return FakeSessionFactory(**kwargs)

    @staticmethod
    def text(statement: str) -> str:
        return statement

    database_error = FakeDatabaseError


def test_build_postgres_engine_options_uses_pool_and_connectivity_settings() -> None:
    settings = PlatformSettings(service_name="backend").postgres

    options = build_postgres_engine_options(settings)

    assert options == {
        "pool_pre_ping": True,
        "pool_size": 5,
        "max_overflow": 10,
        "pool_timeout": 30,
        "pool_recycle": 1800,
        "connect_args": {
            "application_name": "diploma-thesis",
            "connect_timeout": 5,
        },
    }


def test_create_postgres_engine_builds_engine_from_settings(monkeypatch) -> None:
    runtime = FakeRuntime()
    monkeypatch.setattr("libs.storage.postgres.engine._load_sqlalchemy_runtime", lambda: runtime)

    settings = PlatformSettings(service_name="backend").postgres
    engine = create_postgres_engine(settings)

    assert isinstance(engine, FakeEngine)
    assert runtime.engine_calls == [
        (
            "postgresql+psycopg://aggregator:aggregator@postgres:5432/aggregator",
            {
                "pool_pre_ping": True,
                "pool_size": 5,
                "max_overflow": 10,
                "pool_timeout": 30,
                "pool_recycle": 1800,
                "connect_args": {
                    "application_name": "diploma-thesis",
                    "connect_timeout": 5,
                },
            },
        )
    ]


def test_create_postgres_session_factory_uses_explicit_session_options(monkeypatch) -> None:
    runtime = FakeRuntime()
    monkeypatch.setattr("libs.storage.postgres.engine._load_sqlalchemy_runtime", lambda: runtime)

    engine = FakeEngine()
    session_factory = create_postgres_session_factory(
        engine,
        autoflush=True,
        expire_on_commit=True,
    )

    assert isinstance(session_factory, FakeSessionFactory)
    assert runtime.session_calls == [
        {
            "bind": engine,
            "autoflush": True,
            "expire_on_commit": True,
        }
    ]


def test_probe_postgres_connectivity_returns_success_status(monkeypatch) -> None:
    runtime = FakeRuntime()
    monkeypatch.setattr("libs.storage.postgres.engine._load_sqlalchemy_runtime", lambda: runtime)

    settings = PlatformSettings(service_name="backend").postgres
    engine = FakeEngine()
    status = probe_postgres_connectivity(engine=engine, settings=settings)

    assert status.is_available is True
    assert status.host == "postgres"
    assert status.database == "aggregator"
    assert status.error is None
    assert engine.connections[0].executed == ["SELECT 1"]


def test_probe_postgres_connectivity_returns_failure_status(monkeypatch) -> None:
    runtime = FakeRuntime()
    monkeypatch.setattr("libs.storage.postgres.engine._load_sqlalchemy_runtime", lambda: runtime)

    settings = PlatformSettings(service_name="backend").postgres
    engine = FakeEngine(should_fail=True)
    status = probe_postgres_connectivity(engine=engine, settings=settings)

    assert status.is_available is False
    assert status.host == "postgres"
    assert status.port == 5432
    assert status.database == "aggregator"
    assert status.error == "postgres unavailable"
