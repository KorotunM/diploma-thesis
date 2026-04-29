# MVP Roadmap: 12 Days

## От чего отталкиваемся

Состояние репозитория на текущий момент:

- архитектура `evidence-first` уже зафиксирована в `deep-research-report.md` и `docs/architecture-foundation.md`;
- есть каркасы сервисов `scheduler`, `parser`, `normalizer`, `backend`, `frontend`;
- есть event contracts, доменные модели, SQL-схемы и локальная инфраструктура;
- реальный pipeline пока не реализован: сервисы работают как stub/demo, UI тоже каркасный.

Это значит, что ближайшие 12 дней надо тратить не на пересборку архитектуры, а на доведение существующего foundation до первого работающего MVP.

## Цель MVP на 12-й день

К концу 12-го дня проект должен уметь:

- вручную запускать обход источника через `scheduler`;
- публиковать и обрабатывать реальные `crawl.request.v1`, `parse.completed.v1`, `normalize.request.v1`, `card.updated.v1`;
- сохранять raw-артефакты в `MinIO` и метаданные в `PostgreSQL`;
- строить `parsed_document`, `claim`, `claim_evidence`, `resolved_fact`, `card_version`;
- собирать одну каноническую карточку вуза из трех типов источников:
  - официальный сайт;
  - один агрегатор;
  - один рейтинговый источник;
- выполнять базовое сопоставление вузов:
  - exact match по домену;
  - exact match по каноническому названию;
  - trigram fallback для серых случаев;
- отдавать через `backend`:
  - поиск;
  - карточку вуза;
  - provenance/evidence;
- показывать это во `frontend` на живых API;
- иметь минимальный replay/regression контур и базовую эксплуатационную диагностику.

## Что не входит в этот MVP

- embeddings;
- LLM-assist;
- полноценный review workflow с отдельной UI-панелью;
- широкий каталог источников;
- сложная оркестрация DAG-уровня;
- production-grade auth и multi-tenant режим.

## Механики, которые нужно вводить строго последовательно

### 1. Общая конфигурация и инфраструктурные клиенты

Сначала нужно убрать разрыв между архитектурой и кодом: единые settings, доступ к PostgreSQL, RabbitMQ и MinIO, health-check и базовые connection wrappers.

Без этого дальнейшие шаги будут плодить локальные заглушки и несовместимые точки интеграции.

### 2. Scheduler как источник реальных crawl jobs

`scheduler` должен перестать быть только echo-endpoint и стать местом, где живут:

- реестр источников;
- endpoints;
- crawl policy;
- запуск `pipeline_run`;
- публикация задач в нужную очередь по приоритету.

### 3. Parser как реальный ingestion-слой

После запуска job нужно сразу получить:

- fetch;
- запись raw в MinIO;
- запись `raw_artifact` в БД;
- построение `parsed_document`;
- выделение `extracted_fragment`;
- публикацию `parse.completed.v1`.

### 4. Первый vertical slice только с authoritative source

До multi-source логики сначала нужен один работающий контур:

`scheduler -> parser -> normalizer -> backend -> frontend`

Если этого нет, то multi-source merge только усложнит отладку.

### 5. Claim/evidence/provenance как базовая единица правды

Следующий обязательный слой:

- `claim`;
- `claim_evidence`;
- `resolved_fact`;
- `card_version`;
- `delivery.university_card`.

Именно здесь проект перестает быть scraper-демо и становится evidence-based aggregator.

### 6. Multi-source consolidation

Только после single-source карточки имеет смысл добавлять:

- агрегатор;
- рейтинг;
- trust tiers;
- field policy matrix;
- правила выбора победившего значения;
- серые кейсы для review queue.

### 7. Search и delivery projections

Поиск нельзя делать прямо поверх сырой канонической модели. Нужен отдельный delivery/read-model слой:

- `delivery.university_card`;
- `delivery.university_search_doc`;
- фильтры;
- paging;
- API-ручки под UI.

### 8. Replay, regression и эксплуатационный слой

Когда MVP уже ходит end-to-end, нужно добить:

- replay;
- fixture capture;
- regression tests;
- retries/DLQ;
- freshness;
- dashboards.

Без этого демонстрация будет возможна, но развитие после MVP станет хрупким.

## Распределение коммитов

Ограничение соблюдается:

- срок: `12` дней;
- минимум коммитов: `60`;
- разброс по дням: от `2` до `7`;
- количество коммитов по дням не одинаковое.

Плановый расклад:

| День | Коммитов | Фокус |
| --- | ---: | --- |
| 1 | 4 | Общие settings и infra-клиенты |
| 2 | 5 | Scheduler и публикация crawl jobs |
| 3 | 6 | Parser ingestion foundation |
| 4 | 4 | Первый authoritative vertical slice |
| 5 | 7 | Claims, facts, card projection, backend read path |
| 6 | 5 | Frontend на живых API |
| 7 | 6 | Второй источник и merge policy |
| 8 | 4 | Рейтинговый источник и matching fallback |
| 9 | 7 | Поиск, фильтры, paging |
| 10 | 3 | Replay, fixture capture, regression |
| 11 | 5 | DLQ, freshness, metrics, dashboards |
| 12 | 4 | Полировка MVP, runbook, финальная проверка |
| Итого | 60 | MVP |

## План по дням

### День 1. Стабилизировать foundation под реальную интеграцию -- ВЫПОЛНЕН

Цель дня:

- убрать инфраструктурные stub-разрывы;
- завести единый способ подключения к Postgres, RabbitMQ и MinIO;
- подготовить кодовую базу к реальным side effects.

Ключевые механики:

- `libs/storage/postgres`;
- `libs/storage/rabbitmq`;
- `libs/storage/minio`;
- `libs/storage/settings.py`;
- базовые readiness/health проверки.

Definition of done:

- каждый backend-сервис поднимается с реальными settings;
- можно проверить доступность Postgres/RabbitMQ/MinIO из кода;
- нет необходимости хардкодить подключение прямо в `app/main.py`.

Коммиты дня:

1. `chore(config): unify shared settings for postgres rabbitmq minio and service env loading`
2. `feat(storage): add postgres engine session factory and connectivity probe`
3. `feat(storage): add rabbitmq publisher-consumer helpers aligned with declared topology`
4. `feat(storage): add minio client wrapper and bucket readiness checks`

### День 2. Превратить scheduler в реальный вход в pipeline

Цель дня:

- добавить реестр источников и endpoint-ов;
- сделать ручной запуск crawl job;
- писать `pipeline_run` и публиковать `crawl.request.v1`.

Ключевые механики:

- `ingestion.source`;
- `ingestion.source_endpoint`;
- `ops.pipeline_run`;
- admin API в `scheduler`;
- routing в `parser.high` и `parser.bulk`.

Definition of done:

- пользователь может завести источник и endpoint;
- ручной вызов админ-ручки создает `pipeline_run`;
- событие попадает в RabbitMQ с корректным payload.

Коммиты дня:

1. `feat(scheduler): add source registry read-write API backed by ingestion.source`
2. `feat(scheduler): add endpoint registry and crawl policy validation`
3. `feat(scheduler): persist pipeline_run lifecycle for manual trigger requests`
4. `feat(scheduler): publish crawl.request.v1 into parser queues with priority routing`
5. `test(scheduler): add smoke coverage for trigger endpoint and emitted event contract`

### День 3. Поднять ingestion-слой parser

Цель дня:

- научить parser принимать реальный `crawl.request`;
- скачать страницу;
- сохранить raw-артефакт;
- начать строить parse persistence.

Ключевые механики:

- `libs/source_sdk/base_adapter.py`;
- `libs/source_sdk/fetchers/http.py`;
- `ingestion.raw_artifact`;
- content-addressed storage в MinIO;
- idempotent обработка job.

Definition of done:

- parser достает HTML/JSON по URL;
- сохраняет raw в MinIO;
- пишет запись в `ingestion.raw_artifact`;
- готовит базу для дальнейшего `parsed_document`.

Коммиты дня:

1. `feat(source-sdk): define adapter contract fetch context and parser execution model`
2. `feat(parser): implement http fetcher with status headers etag and last-modified capture`
3. `feat(parser): store raw payload in minio using sha256-based object keys`
4. `feat(parser): persist ingestion.raw_artifact metadata after successful fetch`
5. `feat(parser): add crawl.request consumer with idempotent raw artifact handling`
6. `test(parser): add fixture-driven ingestion integration harness`

### День 4. Собрать первый authoritative vertical slice

Цель дня:

- реализовать первый adapter для официального сайта;
- сохранить parsed snapshot;
- замкнуть первое событие `parse.completed.v1`.

Ключевые механики:

- adapter `official_sites`;
- `parsing.parsed_document`;
- `parsing.extracted_fragment`;
- публикация `parse.completed.v1`;
- подготовка `normalize.request.v1`.

Definition of done:

- по одному официальному источнику проходит полный путь от job до parse completed;
- extracted fragments уже содержат базовые поля карточки;
- normalizer получает корректное входное событие.

Коммиты дня:

1. `feat(parser): add official-site adapter for canonical name location website and contacts`
2. `feat(parser): persist parsed_document and extracted_fragment records for official source`
3. `feat(parser): emit parse.completed.v1 after raw and parsed persistence`
4. `test(integration): verify first vertical slice from scheduler trigger to normalize request`

### День 5. Реализовать canonical path: claims -> facts -> card

Цель дня:

- отказаться от stub-card;
- перейти к реальной сборке карточки из parsed data;
- отдать карточку и provenance из `backend`.

Ключевые механики:

- `normalize.claim`;
- `normalize.claim_evidence`;
- `core.university`;
- `core.resolved_fact`;
- `core.card_version`;
- `delivery.university_card`.

Definition of done:

- single-source authoritative карточка строится в БД;
- `backend` возвращает карточку не из sample, а из delivery projection;
- provenance-цепочка собрана из реальных сущностей.

Коммиты дня:

1. `feat(normalizer): build claim records from extracted fragments`
2. `feat(normalizer): persist claim_evidence and provenance links to raw artifacts`
3. `feat(normalizer): bootstrap core.university for single-source authoritative records`
4. `feat(normalizer): generate resolved_fact entries for canonical fields`
5. `feat(normalizer): create card_version and delivery.university_card projection`
6. `feat(backend): serve university card from delivery projection instead of sample stub`
7. `feat(backend): stitch provenance endpoint from raw parsed claim fact and card version data`

### День 6. Привязать frontend к живому backend - остановились на дне 6

Цель дня:

- заменить статические панели реальным запросом к API;
- сделать первую демонстрационную пользовательскую траекторию.

Ключевые механики:

- фронтендовый API client;
- загрузка home/search/card;
- drawer доказательств;
- состояние loading/error/empty.

Definition of done:

- фронтенд больше не живет на захардкоженных примерах;
- можно найти вуз, открыть карточку и увидеть источники поля;
- UI остается тонким, но уже рабочим.

Коммиты дня:

1. `feat(frontend): add backend api client and runtime environment wiring`
2. `feat(frontend): render home page with live pipeline and source freshness summary`
3. `feat(frontend): connect search page to backend query endpoint`
4. `feat(frontend): connect university card page to live card payload`
5. `feat(frontend): render evidence drawer from provenance and field attribution data`

### День 7. Добавить второй источник и первую консолидацию

Цель дня:

- ввести второй тип источника;
- научить normalizer объединять значения по одному вузу;
- ввести доверие к источнику как правило, а не как ad hoc if/else.

Ключевые механики:

- adapter `aggregators`;
- source trust tier;
- field policy matrix;
- merge по вузу поверх official + aggregator;
- атрибуция поля в карточке.

Definition of done:

- один вуз можно собрать из двух источников;
- official source побеждает там, где он authoritative;
- карточка хранит объяснение, почему выбрано именно это значение.

Коммиты дня:

1. `feat(parser): add aggregator adapter for secondary university source`
2. `feat(parser): map aggregator payload into parsed fragments compatible with normalizer`
3. `feat(normalizer): add source trust tiers and field resolution policy matrix`
4. `feat(normalizer): merge official and aggregator claims into one university entity`
5. `feat(backend): expose per-field attribution and source rationale in card response`
6. `test(integration): cover dual-source merge scenario with deterministic winner selection`

### День 8. Добавить рейтинг и базовый matching fallback

Цель дня:

- ввести третий тип источника;
- закрыть первый реальный конфликт данных;
- добавить базовое сопоставление и серую зону.

Ключевые механики:

- adapter `rankings`;
- ranking claims;
- exact match по домену;
- exact match по каноническому имени;
- trigram fallback;
- `review.required.v1` для ambiguous cases.

Definition of done:

- рейтинговые данные приезжают в ту же каноническую модель;
- вуз сопоставляется не только по домену, но и по имени;
- для сомнительных совпадений поднимается review event, а не silent merge.

Коммиты дня:

1. `feat(parser): add ranking adapter for first external ranking provider`
2. `feat(normalizer): persist rating claims and resolve structured ranking facts`
3. `feat(normalizer): implement exact matching by domain and canonical name`
4. `feat(normalizer): add trigram fallback and emit review.required.v1 for gray-zone matches`

### День 9. Реализовать поиск, фильтры и paging

Цель дня:

- сделать фронтовый сценарий поиска реальным;
- добавить отдельную search-проекцию;
- подготовить UI к работе не с одной карточкой, а с каталогом.

Ключевые механики:

- `delivery.university_search_doc`;
- full-text + trigram search в PostgreSQL;
- фильтры по городу, стране, типу источника;
- pagination;
- query-state sync во frontend.

Definition of done:

- backend ищет не по sample-объекту, а по projection table;
- frontend поддерживает фильтры и paging;
- путь `поиск -> карточка -> evidence` полностью рабочий.

Коммиты дня:

1. `feat(db): add delivery.university_search_doc projection table and indexes`
2. `feat(normalizer): refresh search projection whenever card version is rebuilt`
3. `feat(backend): implement full-text and trigram university search service`
4. `feat(backend): add filters for city country source type and paging`
5. `feat(frontend): render result list with pagination and empty states`
6. `feat(frontend): add filter panel and url-synced query state`
7. `test(e2e): cover search to card to evidence happy path`

### День 10. Сделать MVP replayable

Цель дня:

- перестать зависеть только от live сайтов;
- обеспечить повторный прогон pipeline;
- зафиксировать regression baseline для трех MVP-источников.

Ключевые механики:

- `scripts/replay`;
- `scripts/fixture_capture`;
- `scripts/backfill`;
- captured raw fixtures;
- regression test набор.

Definition of done:

- можно повторно собрать карточку из сохраненных raw/parsed данных;
- есть локально воспроизводимые fixtures;
- базовая регрессия ловит поломки адаптеров и normalizer rules.

Коммиты дня:

1. `feat(scripts): add replay workflow for parse and normalize from stored artifacts`
2. `feat(scripts): add fixture capture and backfill flow for three MVP sources`
3. `test(regression): add captured raw-page regression suite for adapters and normalization rules`

### День 11. Добить операционную пригодность MVP - Реализуем его сейчас

Цель дня:

- сделать поведение pipeline диагностируемым при ошибках;
- видеть свежесть и деградацию источников;
- иметь минимальный наблюдаемый runtime.

Ключевые механики:

- DLQ и retry policy;
- freshness jobs;
- domain metrics;
- Prometheus/Grafana;
- failure-path tests.

Definition of done:

- ошибки парсинга и нормализации не теряются;
- stale sources видны отдельно;
- есть метрики по run, fetch, parse, normalize, card build;
- можно показать хотя бы стартовые dashboard panels.

Коммиты дня:

1. `feat(infra): configure retry and dead-letter behavior for parser normalize and delivery flows`
2. `feat(scheduler): add freshness checks and stale-source monitoring jobs`
3. `feat(observability): publish crawl parse normalize and card-build domain metrics`
4. `feat(observability): add starter prometheus and grafana dashboards for pipeline health`
5. `test(integration): cover retry and dead-letter behavior on controlled worker failures`

### День 12. Зафиксировать MVP как демонстрируемый продукт

Цель дня:

- убрать остаточные demo-артефакты;
- подготовить воспроизводимую демонстрацию;
- зафиксировать, что является MVP, а что уходит после него.

Ключевые механики:

- UX-полировка;
- demo runbook;
- финальный smoke;
- backlog after MVP.

Definition of done:

- `docker compose up` и локальный запуск приводят к воспроизводимому demo-сценарию;
- документация отражает реальный путь запуска;
- MVP границы больше не размыты.

Коммиты дня:

1. `feat(frontend): polish loading error and empty states and remove remaining hardcoded demo placeholders`
2. `docs(mvp): add local demo runbook seeded data flow and troubleshooting notes`
3. `test(e2e): add final smoke script for compose-up to frontend demo path`
4. `chore(release): freeze mvp scope and document immediate post-mvp backlog`

## Критический путь реализации

Если времени начнет не хватать, нельзя ломать этот порядок:

1. День 1-5 обязательны целиком.
2. День 6 обязателен хотя бы в минимальном виде, иначе MVP нельзя показать.
3. День 7-9 обязательны для заявленного MVP, потому что без них не будет multi-source aggregation и реального поиска.
4. День 10-11 можно урезать по глубине, но не выкидывать полностью.
5. День 12 нельзя превращать в “допилим потом”: это день упаковки и фиксации результата.

## Главные риски

### 1. Слишком рано уйти в сложный matching

До завершения single-source и dual-source deterministic rules нельзя тратить время на embeddings и LLM.

### 2. Писать поиск напрямую поверх canonical storage

Для MVP это кажется быстрее, но потом сломает API-стабильность и производительность. Нужен отдельный delivery projection.

### 3. Делать UI раньше backend-read моделей

Тогда фронтенд снова обрастет mock-данными и придется второй раз переписывать страницы.

### 4. Пропустить replay/regression

Для такого проекта без replayability любое изменение parser/normalizer будет опасным и трудно объяснимым.

## Итог

Этот roadmap рассчитан на текущее состояние репозитория, а не на абстрактный greenfield. За 12 дней он доводит проект от foundation/stub-слоя до MVP, где уже есть:

- три типа источников;
- реальный событийный pipeline;
- каноническая карточка;
- provenance;
- поиск;
- рабочий UI;
- минимальная операционная устойчивость.

Общий план дает `60` коммитов с завершенными функциональными точками и сохраняет правильную последовательность внедрения механик из `deep-research-report.md`.
