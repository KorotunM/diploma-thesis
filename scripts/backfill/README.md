# Backfill

Скрипты массового заполнения новых полей и пересборки delivery-проекций.

MVP workflow:

- `python -m scripts.backfill tests/fixtures/mvp_bundle/manifest.json`

Что делает backfill:

- читает fixture bundle manifest;
- синхронизирует source registry и endpoint registry для трёх MVP source family;
- импортирует raw fixtures обратно в MinIO/Postgres;
- запускает parser replay и normalizer replay, чтобы пересобрать `parsed_document`, claims, facts и delivery projections.
