# Replay

Скрипты повторной обработки для пересчёта данных после изменения parser/normalizer logic.

Базовый workflow:

- `python -m scripts.replay parse <raw_artifact_id>`
- `python -m scripts.replay normalize <parsed_document_id> --normalizer-version normalizer.0.1.0`
- `python -m scripts.replay full <raw_artifact_id> --normalizer-version normalizer.0.1.0`

Принципы:

- parse replay читает raw payload из MinIO по уже сохранённому `ingestion.raw_artifact`;
- parser adapter повторно запускается без сетевого fetch;
- если `parsing.parsed_document` для пары `(raw_artifact_id, parser_version)` уже существует, workflow переиспользует его и не дублирует fragments;
- normalize replay строит claims, merge, resolved facts, `delivery.university_card` и `delivery.university_search_doc` из сохранённого `parsed_document`.
