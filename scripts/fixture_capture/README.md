# Fixture Capture

Скрипты фиксации raw-страниц и API-ответов для regression-наборов.

MVP workflow:

- `python -m scripts.fixture_capture --bundle-name mvp-demo --output-dir tests/fixtures/mvp_bundle --official-raw-artifact-id <uuid> --aggregator-raw-artifact-id <uuid> --ranking-raw-artifact-id <uuid>`

Что делает capture:

- читает три сохранённых `ingestion.raw_artifact` из Postgres;
- загружает соответствующие payload из MinIO;
- валидирует `sha256`;
- пишет локальный bundle из трёх файлов и `manifest.json`.
