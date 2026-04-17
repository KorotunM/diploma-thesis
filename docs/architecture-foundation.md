# Architecture Foundation

Этот документ фиксирует, как `deep-research-report.md` переводится в стартовую реализацию репозитория.

## Зафиксированные решения

### 1. Модель данных

- Проект строится как `evidence-first`, а не как `source -> overwrite university row`.
- Каноническая карточка вычисляется из цепочки:
  `RawArtifact -> ParsedDocument -> Claim -> ResolvedFact -> UniversityCard`.
- Каждая стадия имеет собственную версию обработки и собственное хранилище.

### 2. Границы сервисов

- `scheduler`: планирование обходов, ручной запуск, freshness/SLA, публикация crawl jobs.
- `parser`: source adapters, fetch, raw storage, extraction, intermediate mapping.
- `normalizer`: field normalization, matching, clustering, truth resolution, projections.
- `backend`: read/write API поверх delivery и административных представлений.
- `frontend`: пользовательский поиск, карточка вуза, provenance drawer, admin/review UI.

### 3. Инфраструктурные опоры

- `PostgreSQL`: системная, каноническая и delivery БД.
- `RabbitMQ`: событийная и job-шина между сервисами.
- `MinIO`: неизменяемое raw/parsed/assist storage.

### 4. Базовые схемы PostgreSQL

- `ops`
- `ingestion`
- `parsing`
- `normalize`
- `core`
- `delivery`

### 5. Контракты как отдельный слой

- События лежат в `libs/contracts/events` и дублируются как JSON Schema в `schemas/events`.
- Каноническая карточка лежит в `libs/domain/university` и `schemas/canonical`.
- OpenAPI-файлы находятся отдельно от сервисного кода в `schemas/openapi`.

## Что намеренно не сделано сейчас

- Реальные adapters для конкретных сайтов.
- Настоящая публикация сообщений в RabbitMQ и запись в PostgreSQL/MinIO.
- Matching, clustering, truth discovery и replay jobs.
- Полноценный review workflow.

Это сознательно: текущий этап создает стабильную основу, чтобы дальнейшая реализация шла по уже зафиксированным границам и контрактам.
