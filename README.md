# diploma-thesis

Фундамент проекта собран вокруг архитектуры из `deep-research-report.md`: агрегатор вузов строится как `evidence-first` data pipeline, где первичны raw-артефакты, извлеченные утверждения, provenance и версии обработки, а пользовательская карточка вуза является только проекцией.

## Что уже заложено

- Монорепозиторий с сервисами `scheduler`, `parser`, `normalizer`, `backend` и каркасом `frontend`.
- Общие Python-контракты событий и базовые доменные модели для `Claim`, `ResolvedFact`, `UniversityCard`.
- JSON Schema для ключевых событий и канонической карточки.
- SQL-фундамент под схемы `ops`, `ingestion`, `parsing`, `normalize`, `core`, `delivery`.
- Локальная инфраструктура через Docker Compose: `PostgreSQL`, `RabbitMQ`, `MinIO`, `Prometheus`, `Grafana`.
- Стартовые health/metrics endpoints для каждого backend-сервиса.

## Архитектурные принципы

- Источник истины: `raw -> parsed -> claims -> resolved facts -> delivery projection`.
- Любой внешний факт хранится как отдельное утверждение с источником, временем, версией парсера и трассировкой происхождения.
- Каноническая карточка вуза вычисляется, а не редактируется напрямую.
- Логика парсинга и нормализации должна быть replayable и versioned.
- Event contracts и canonical schemas живут отдельно от конкретных сервисов.
- LLM допускается только как assist-слой для неоднозначных кейсов и не пишет напрямую в resolved facts.

## Структура

```text
apps/
  scheduler/
  parser/
  normalizer/
  backend/
  frontend/
libs/
  contracts/
  domain/
  storage/
  source_sdk/
  observability/
  quality/
schemas/
  canonical/
  events/
  openapi/
  sql/
infra/
  docker-compose/
  rabbitmq/
  minio/
  prometheus/
  grafana/
  env/
docs/
tests/
```

## Быстрый старт

1. Поднимите инфраструктуру:

   ```powershell
   docker compose -f infra/docker-compose/docker-compose.yml up --build
   ```

2. Локальный запуск Python-сервисов без Docker:

   ```powershell
   py -3 -m pip install -e .[dev,worker,parser]
   $env:PYTHONPATH='.'
   uvicorn apps.scheduler.app.main:app --reload --port 8001
   uvicorn apps.parser.app.main:app --reload --port 8002
   uvicorn apps.normalizer.app.main:app --reload --port 8003
   uvicorn apps.backend.app.main:app --reload --port 8004
   ```

3. Фронтенд:

   ```powershell
   Set-Location apps/frontend
   npm install
   npm run dev
   ```

## Следующий практический шаг

Следующим этапом стоит реализовать первую сквозную цепочку:

1. `scheduler` публикует реальный `crawl.request.v1`.
2. `parser` поднимает первый adapter для официального сайта вуза и пишет raw + parsed snapshot.
3. `normalizer` создает первую `UniversityCard` из одного authoritative source.
4. `backend` отдает карточку и provenance.

Детали решений вынесены в [docs/architecture-foundation.md](docs/architecture-foundation.md) и [docs/roadmap.md](docs/roadmap.md).
