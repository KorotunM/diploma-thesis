# Live MVP Runbook

Этот runbook фиксирует текущий рабочий локальный сценарий для live MVP после завершения commit 20.

Главный путь теперь такой:

`source bootstrap -> discovery -> manual crawl -> parser worker -> normalize worker -> backend search/card/provenance`

`backfill` и `replay` остаются вспомогательными инструментами для регрессии и отладки, но не считаются основным demo-путём.

## Что входит в live MVP

- `tabiturient-aggregator`
  - discovery source: `aggregator.tabiturient.sitemap_xml`
  - entity source: `aggregator.tabiturient.university_html`
- `tabiturient-globalrating`
  - ranking source: `ranking.tabiturient.globalrating_html`
- `kubsu-official`
  - landing page: `official_site.kubsu.abiturient_html`
  - programs page: `official_site.kubsu.programs_html`

Что сознательно не входит в live MVP:

- `official_site.kubsu.places_pdf`
- любой runtime-flow, завязанный на PDF merge

## Предварительные условия

- `Docker` и `docker compose`
- `Python 3.12`
- локально установлен проект с extras:

```powershell
py -3 -m pip install -e .[dev,worker,parser]
```

## 1. Поднять стек

Из корня репозитория:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml up --build
```

После старта должны быть живы:

- `scheduler` на `http://localhost:8001`
- `parser` на `http://localhost:8002`
- `normalizer` на `http://localhost:8003`
- `backend` на `http://localhost:8004`
- `frontend` на `http://localhost:5173`
- `parser-worker`
- `normalizer-worker`

Быстрая проверка:

```powershell
Invoke-RestMethod http://localhost:8001/healthz
Invoke-RestMethod http://localhost:8002/healthz
Invoke-RestMethod http://localhost:8003/healthz
Invoke-RestMethod http://localhost:8004/healthz
```

## 2. Засеять source registry

```powershell
py -3 -m scripts.source_bootstrap
```

Ожидаемый результат:

- в `ingestion.source` есть `kubsu-official`, `tabiturient-aggregator`, `tabiturient-globalrating`
- в `ingestion.source_endpoint` есть стартовые endpoint'ы для KubSU и Tabiturient sitemap/globalrating

Проверить можно так:

```powershell
Invoke-RestMethod "http://localhost:8001/admin/v1/sources?limit=20&offset=0" | ConvertTo-Json -Depth 8
```

## 3. Выполнить discovery для Tabiturient

```powershell
$body = @{
  source_key = "tabiturient-aggregator"
  dry_run = $false
} | ConvertTo-Json

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8001/admin/v1/discovery/materialize-jobs" `
  -ContentType "application/json" `
  -Body $body | ConvertTo-Json -Depth 8
```

Ожидаемый результат:

- scheduler читает `https://tabiturient.ru/map/sitemap.php`
- materialize'ит только root-страницы формата `/vuzu/<slug>`
- не создает `about`, `proxodnoi`, query-string и дубликаты

Проверка materialized endpoint'ов:

```powershell
Invoke-RestMethod "http://localhost:8001/admin/v1/sources/tabiturient-aggregator/endpoints?limit=200&offset=0" | ConvertTo-Json -Depth 8
```

## 4. Выбрать endpoint для crawl

Для KubSU authoritative flow:

```powershell
Invoke-RestMethod "http://localhost:8001/admin/v1/sources/kubsu-official/endpoints?limit=20&offset=0" | ConvertTo-Json -Depth 8
```

Для discovered Tabiturient pages:

```powershell
Invoke-RestMethod "http://localhost:8001/admin/v1/sources/tabiturient-aggregator/endpoints?limit=200&offset=0" | ConvertTo-Json -Depth 8
```

Из ответа нужен `endpoint_id`.

## 5. Создать manual crawl job

Пример для KubSU abiturient page:

```powershell
$crawlRunId = [guid]::NewGuid().ToString()
$endpointId = "<endpoint_id>"

$body = @{
  crawl_run_id = $crawlRunId
  source_key = "kubsu-official"
  endpoint_id = $endpointId
  priority = "high"
  metadata = @{
    requested_by = "demo-runbook"
  }
} | ConvertTo-Json -Depth 6

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8001/admin/v1/crawl-jobs" `
  -ContentType "application/json" `
  -Body $body | ConvertTo-Json -Depth 8
```

Ожидаемый результат:

- scheduler публикует `crawl.request.v1` в `parser.high`
- `parser-worker` сохраняет `raw_artifact`, строит `parsed_document` и `extracted_fragment`
- `parser-worker` публикует `parse.completed.v1`
- `normalizer-worker` строит `claim`, `claim_evidence`, `core.university`, `resolved_fact`, `card_version`
- backend начинает видеть обновленный `delivery.university_card` и `delivery.university_search_doc`

## 6. Проверить backend flow

### Search

KubSU:

```powershell
Invoke-RestMethod "http://localhost:8004/api/v1/search?query=%D0%9A%D1%83%D0%B1%D0%B0%D0%BD%D1%81%D0%BA%D0%B8%D0%B9&page=1&page_size=10" | ConvertTo-Json -Depth 8
```

Tabiturient discovered page:

```powershell
Invoke-RestMethod "http://localhost:8004/api/v1/search?query=%D0%90%D0%BB%D1%82%D0%B0%D0%B9%D1%81%D0%BA%D0%B8%D0%B9&page=1&page_size=10" | ConvertTo-Json -Depth 8
```

### Card

Подставь `university_id` из search:

```powershell
Invoke-RestMethod "http://localhost:8004/api/v1/universities/<university_id>" | ConvertTo-Json -Depth 12
```

Для KubSU в текущем MVP ожидается:

- canonical name
- `contacts.website`
- `contacts.emails`
- `contacts.phones`
- при прогоне programs page дополнительно `admission.programs`

### Provenance

```powershell
Invoke-RestMethod "http://localhost:8004/api/v1/universities/<university_id>/provenance" | ConvertTo-Json -Depth 12
```

Ожидается цепочка:

- `raw_artifacts`
- `parsed_documents`
- `claims`
- `claim_evidence`
- `resolved_facts`
- `delivery_projection`

## 7. Проверить frontend

Открой `http://localhost:5173`.

Минимальный путь:

1. На `Home` убедиться, что сервисы живы.
2. На `Search` выполнить запрос по KubSU или discovered Tabiturient вузу.
3. Использовать найденный `university_id` для страницы карточки.
4. Открыть evidence/provenance drawer.

## 8. Когда использовать backfill и replay

Они остаются полезными, но не являются live MVP sign-off path.

Использовать:

- `scripts.backfill` для seeded regression/demo bundle
- `scripts.replay parse <raw_artifact_id>` после изменения parser logic
- `scripts.replay normalize <parsed_document_id>` после изменения normalizer logic
- `scripts.replay full <raw_artifact_id>` если нужно пересобрать обе стадии

## 9. Известные ограничения текущего release

- PDF extraction выключен из MVP и не участвует в merge
- scheduler пока не запускает автономный periodic loop, используется manual crawl path
- frontend-путь все еще опирается на явный `university_id` для открытия карточки
- discovery применяется только к `tabiturient-aggregator`

## 10. Release sign-off

Текущий live MVP считаем воспроизводимым, если одновременно выполняется все ниже:

- `docker compose up` поднимает API и worker-процессы
- `py -3 -m scripts.source_bootstrap` засеивает source registry
- `POST /admin/v1/discovery/materialize-jobs` materialize'ит Tabiturient primary pages
- `POST /admin/v1/crawl-jobs` запускает реальный async path без replay
- backend search/card/provenance показывают результат после worker processing
- e2e тест `tests/e2e/test_live_mvp_bootstrap_to_provenance_flow.py` проходит

Сервис	URL
Frontend	http://localhost:5173
Scheduler API	http://localhost:8001
Backend API	http://localhost:8004
RabbitMQ UI	http://localhost:15672 (aggregator/aggregator)
MinIO Console	http://localhost:9001 (aggregator/aggregator-secret)
Grafana	http://localhost:3000 (admin/admin)