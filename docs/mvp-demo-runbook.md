# MVP Demo Runbook

Этот runbook фиксирует воспроизводимый локальный demo-сценарий для текущего MVP:
поднять стек, засеять данные из fixture bundle, проверить API/UI и быстро локализовать
типовые проблемы.

## Что покрывает demo

Demo bundle уже лежит в репозитории:

- `tests/fixtures/mvp_bundle/manifest.json`

Он содержит три MVP source family:

- `msu-official` — authoritative source для canonical fields;
- `study-aggregator` — secondary source для aliases и supporting fields;
- `qs-world-ranking` — ranking source для structured rating facts.

После seeded backfill ожидается такой поток данных:

`fixture bundle -> source registry -> raw_artifact -> parsed_document/extracted_fragment -> claim/claim_evidence -> core.university/resolved_fact/card_version -> delivery.university_card + delivery.university_search_doc -> backend API -> frontend`

## Предварительные условия

- `Docker` и `docker compose`
- `Python 3.12`
- `Node.js 22+`

Для локального запуска host-side scripts нужен установленный проект с extras:

```powershell
py -3 -m pip install -e .[dev,worker,parser]
```

## 1. Поднять локальный стек

Из корня репозитория:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml up --build
```

Полезные URL после старта:

- frontend: `http://localhost:5173`
- backend: `http://localhost:8004`
- scheduler: `http://localhost:8001`
- parser: `http://localhost:8002`
- normalizer: `http://localhost:8003`
- RabbitMQ UI: `http://localhost:15672`
- MinIO console: `http://localhost:9001`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

Локальные credentials из `infra/env/local/app.env`:

- RabbitMQ: `aggregator / aggregator`
- MinIO: `aggregator / aggregator-secret`
- Grafana: `admin / admin`

## 2. Проверить, что сервисы живы

Минимальная проверка:

```powershell
Invoke-RestMethod http://localhost:8001/healthz
Invoke-RestMethod http://localhost:8002/healthz
Invoke-RestMethod http://localhost:8003/healthz
Invoke-RestMethod http://localhost:8004/healthz
```

Ожидается `ok`-состояние либо структура health-зависимостей без transport errors.

## 3. Засеять MVP bundle в локальную базу

Важно: Python scripts по умолчанию читают `infra/env/local/app.env`, где стоят
container hostnames (`postgres`, `minio`). При запуске с хоста их нужно переопределить
на `localhost`.

Рекомендуемый способ для PowerShell:

```powershell
$env:APP_ENV = "local"
$env:POSTGRES_DSN = "postgresql+psycopg://aggregator:aggregator@localhost:5432/aggregator"
$env:MINIO_ENDPOINT = "http://localhost:9000"
py -3 -m scripts.backfill tests/fixtures/mvp_bundle/manifest.json
```

Что делает backfill:

1. Синхронизирует `ingestion.source` и `ingestion.source_endpoint` для трёх MVP sources.
2. Импортирует fixture payloads в MinIO и `ingestion.raw_artifact`.
3. Запускает parser replay для каждого imported raw artifact.
4. Запускает normalizer replay и пересобирает:
   - `normalize.claim`
   - `normalize.claim_evidence`
   - `core.university`
   - `core.resolved_fact`
   - `core.card_version`
   - `delivery.university_card`
   - `delivery.university_search_doc`

Скрипт печатает JSON-результат. Сохрани из него:

- `items[*].university_id`
- `items[*].parsed_document_id`
- `items[*].raw_artifact_id`

Для demo важнее всего `university_id`: его можно вставить в card lookup на frontend
или использовать в backend API.

## 4. Проверить seeded API path

### Search

```powershell
Invoke-RestMethod "http://localhost:8004/api/v1/search?query=Example%20University" | ConvertTo-Json -Depth 6
```

Альтернативный запрос:

```powershell
Invoke-RestMethod "http://localhost:8004/api/v1/search?query=example.edu" | ConvertTo-Json -Depth 6
```

Ожидается как минимум один hit с:

- `canonical_name`
- `university_id`
- `match_signals`

### Card

Подставь `university_id` из результата search или из вывода backfill:

```powershell
Invoke-RestMethod "http://localhost:8004/api/v1/universities/<university_id>" | ConvertTo-Json -Depth 8
```

Ожидается:

- каноническое имя;
- location и website;
- `field_attribution`;
- `source_rationale`.

### Provenance

```powershell
Invoke-RestMethod "http://localhost:8004/api/v1/universities/<university_id>/provenance" | ConvertTo-Json -Depth 10
```

Ожидается полная цепочка:

- `raw_artifacts`
- `parsed_documents`
- `claims`
- `claim_evidence`
- `resolved_facts`
- `delivery_projection`

### Freshness overview

```powershell
Invoke-RestMethod "http://localhost:8001/admin/v1/freshness" | ConvertTo-Json -Depth 6
```

После seeded backfill registry уже должен содержать три активных source entries.

## 5. Проверить frontend demo path

Открой `http://localhost:5173`.

Рекомендуемый happy path:

1. В блоке `Home` убедись, что pipeline показывает живые сервисы.
2. В блоке `Search` выполни запрос `Example University` или `example.edu`.
3. Скопируй `university_id` из search result card.
4. Вставь его в `University card`.
5. Убедись, что `Evidence Drawer` автоматически построил provenance trace для того же `university_id`.

Что должно быть видно в UI:

- `Home`: live pipeline summary и source freshness;
- `Search`: реальные search hits из `delivery.university_search_doc`;
- `University card`: payload из `delivery.university_card`;
- `Evidence Drawer`: связка `raw -> parsed -> claim -> evidence -> fact -> card`.

## 6. Replay и пересборка после изменений логики

Если менялась parser или normalizer logic, не нужно заново делать весь backfill.

Для replay с хоста снова нужны те же env overrides:

```powershell
$env:APP_ENV = "local"
$env:POSTGRES_DSN = "postgresql+psycopg://aggregator:aggregator@localhost:5432/aggregator"
$env:MINIO_ENDPOINT = "http://localhost:9000"
```

Полезные команды:

```powershell
py -3 -m scripts.replay parse <raw_artifact_id>
py -3 -m scripts.replay normalize <parsed_document_id> --normalizer-version normalizer.0.1.0
py -3 -m scripts.replay full <raw_artifact_id> --normalizer-version normalizer.0.1.0
```

Когда использовать:

- `parse` — изменилась adapter/extraction logic;
- `normalize` — изменилась matching/resolution/projection logic;
- `full` — нужно пересобрать обе стадии от raw artifact.

## 7. Observability во время demo

Prometheus и Grafana уже wired в compose stack.

Проверки:

- метрики сервисов: `http://localhost:8001/metrics`, `:8002/metrics`, `:8003/metrics`, `:8004/metrics`
- Prometheus targets: `http://localhost:9090/targets`
- Grafana dashboard: `Pipeline Health / Pipeline Health Overview`

Если dashboard пустой, сначала создай немного трафика:

- открой frontend home;
- сделай search-запрос;
- запроси card и provenance;
- при необходимости выполни manual backfill/replay.

## 8. Troubleshooting

### `could not translate host name "postgres"` или connect timeout к PostgreSQL

Причина:

- host-side script взял контейнерное имя из `infra/env/local/app.env`.

Что делать:

```powershell
$env:POSTGRES_DSN = "postgresql+psycopg://aggregator:aggregator@localhost:5432/aggregator"
```

### `Connection refused` к `minio:9000` или ошибки MinIO client

Причина:

- скрипт запущен с хоста без override для MinIO endpoint.

Что делать:

```powershell
$env:MINIO_ENDPOINT = "http://localhost:9000"
```

### Search возвращает `0` результатов после `backfill`

Проверь последовательно:

1. `py -3 -m scripts.backfill ...` завершился без exception.
2. `GET /api/v1/search?query=Example%20University` действительно обращается к `localhost:8004`.
3. `GET /api/v1/universities/<university_id>` работает по `university_id` из search output, а не по `raw_artifact_id`.

### Карточка или provenance дают `404`

Обычно это одно из двух:

- используется не `university_id`, а другой ID слоя;
- backfill/replay прервался до стадии `delivery.university_card`.

Что делать:

1. Возьми `university_id` из backfill JSON или из backend search response.
2. При необходимости перезапусти:

```powershell
py -3 -m scripts.replay full <raw_artifact_id> --normalizer-version normalizer.0.1.0
```

### Home page показывает degraded services

Проверь:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml ps
docker compose -f infra/docker-compose/docker-compose.yml logs scheduler
docker compose -f infra/docker-compose/docker-compose.yml logs parser
docker compose -f infra/docker-compose/docker-compose.yml logs normalizer
docker compose -f infra/docker-compose/docker-compose.yml logs backend
```

Отдельно полезно открыть:

- `http://localhost:8001/healthz`
- `http://localhost:8002/healthz`
- `http://localhost:8003/healthz`
- `http://localhost:8004/healthz`

### Grafana dashboard пустой

Проверь:

1. `http://localhost:9090/targets` — targets должны быть `UP`.
2. `http://localhost:8001/metrics` и остальные `/metrics` реально отдают payload.
3. После старта demo был создан traffic: search/card/provenance/backfill/replay.

## 9. Полный reset demo-данных

Если нужен чистый прогон с нуля:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml down -v
docker compose -f infra/docker-compose/docker-compose.yml up --build
```

После этого снова выполни шаг `Засеять MVP bundle в локальную базу`.
