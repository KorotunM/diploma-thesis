# diploma-thesis

Проект строится как `evidence-first` data pipeline для агрегации данных о вузах: сначала сохраняются raw-артефакты и извлечённые утверждения, затем они нормализуются в facts, а пользовательская карточка вуза является только delivery-проекцией.

## Что уже есть

- Монорепозиторий с сервисами `scheduler`, `parser`, `normalizer`, `backend`, `frontend`.
- Общие event-контракты и доменные модели.
- SQL-схемы `ops`, `ingestion`, `parsing`, `normalize`, `core`, `delivery`.
- Локальная инфраструктура через Docker Compose: `Postgres`, `RabbitMQ`, `MinIO`, `Prometheus`, `Grafana`.
- Worker-процессы для live queue processing.
- Frontend с поиском, карточкой вуза, мониторингом пайплайна и панелью доказательств.

## Архитектурный принцип

Основная цепочка такая:

`raw -> parsed -> claims -> resolved facts -> delivery projection`

Любой внешний факт должен быть трассируемым до источника, времени захвата, версии парсера и evidence.

## Структура репозитория

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
  observability/
  quality/
schemas/
  canonical/
  events/
  sql/
infra/
  docker-compose/
  env/
docs/
tests/
```

## Быстрый старт

### Полный запуск через Docker Compose

```bash
docker compose \
  -p diploma-thesis \
  -f infra/docker-compose/docker-compose.yml \
  -f infra/docker-compose/docker-compose.override.yml \
  up --build
```

### Локальный запуск Python-сервисов без Docker

```bash
python -m pip install -e '.[dev,worker,parser]'
export PYTHONPATH='.'
uvicorn apps.scheduler.app.main:app --reload --port 8001
uvicorn apps.parser.app.main:app --reload --port 8002
uvicorn apps.normalizer.app.main:app --reload --port 8003
uvicorn apps.backend.app.main:app --reload --port 8004
```

### Frontend

```bash
cd apps/frontend
npm install
npm run dev
```

## Как сделать дамп Postgres и забрать его в проект

Текущие локальные параметры БД:

- host: `postgres`
- port: `5432`
- db: `aggregator`
- user: `aggregator`
- password: `aggregator`

Создать dump внутри контейнера:

```bash
docker compose \
  -p docker-compose \
  -f infra/docker-compose/docker-compose.yml \
  -f infra/docker-compose/docker-compose.override.yml \
  exec -T postgres sh -lc 'PGPASSWORD=aggregator pg_dump -U aggregator -d aggregator -Fc -f /tmp/diploma-thesis.dump'
```

Скопировать dump в проект:

```bash
mkdir -p backups
docker compose \
  -p docker-compose \
  -f infra/docker-compose/docker-compose.yml \
  -f infra/docker-compose/docker-compose.override.yml \
  cp postgres:/tmp/diploma-thesis.dump ./backups/diploma-thesis.dump
```

## Полезные замечания

- Если compose-стек запущен с другим `project name`, замени `-p diploma-thesis` на своё значение.
- Если нужен plain SQL, замени `-Fc -f /tmp/diploma-thesis.dump` на `-f /tmp/diploma-thesis.sql`.
- Для восстановления из custom dump нужен `pg_restore`.

Подробности по архитектуре и MVP-сценарию смотри в `docs/`.
