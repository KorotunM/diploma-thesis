from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_compose_wires_scheduler_parser_and_normalizer_worker_services() -> None:
    compose = (REPO_ROOT / "infra" / "docker-compose" / "docker-compose.yml").read_text(
        encoding="utf-8"
    )
    override = (
        REPO_ROOT / "infra" / "docker-compose" / "docker-compose.override.yml"
    ).read_text(encoding="utf-8")

    assert "scheduler-worker:" in compose
    assert "parser-worker:" in compose
    assert "normalizer-worker:" in compose
    assert "command: python -m apps.scheduler.app.worker" in compose
    assert "command: python -m apps.parser.app.worker" in compose
    assert "command: python -m apps.normalizer.app.worker" in compose
    assert "scheduler-worker:" in override
    assert "parser-worker:" in override
    assert "normalizer-worker:" in override
    assert "command: python -m apps.scheduler.app.worker" in override
    assert "command: python -m apps.parser.app.worker" in override
    assert "command: python -m apps.normalizer.app.worker" in override
    assert 'pg_isready -U $$POSTGRES_USER -d $$POSTGRES_DB' in compose
    assert 'rabbitmq-diagnostics", "-q", "ping' in compose
    assert "condition: service_healthy" in compose


def test_parser_image_installs_worker_dependencies_for_live_queue_runtime() -> None:
    dockerfile = (REPO_ROOT / "apps" / "parser" / "Dockerfile").read_text(encoding="utf-8")

    assert ".[parser,worker]" in dockerfile


def test_scheduler_image_installs_worker_dependencies_for_live_queue_runtime() -> None:
    dockerfile = (REPO_ROOT / "apps" / "scheduler" / "Dockerfile").read_text(encoding="utf-8")

    assert ".[worker]" in dockerfile


def test_minio_bootstrap_waits_until_server_accepts_connections() -> None:
    bootstrap_script = (REPO_ROOT / "infra" / "minio" / "bootstrap.sh").read_text(
        encoding="utf-8"
    )

    assert 'until mc alias set local "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"; do' in bootstrap_script
    assert "sleep 2" in bootstrap_script
