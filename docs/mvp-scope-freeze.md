# MVP Scope Freeze

Этот документ фиксирует границы текущего MVP и запрещает незаметное расширение
объёма до завершения локального demo-сценария.

Связанный runbook:

- [MVP Demo Runbook](./mvp-demo-runbook.md)

## Цель текущего MVP

Показать воспроизводимый `evidence-first` pipeline для карточки университета, где:

- данные поднимаются локально через `docker compose`;
- три MVP source family импортируются через fixture bundle;
- pipeline строит `raw -> parsed -> claims -> resolved facts -> delivery projections`;
- frontend показывает поиск, карточку и provenance trace без demo-заглушек.

## Замороженный пользовательский сценарий

MVP считается завершённым в рамках следующего сценария:

1. Поднять локальный стек.
2. Выполнить `scripts.backfill` на `tests/fixtures/mvp_bundle/manifest.json`.
3. Найти seeded университет через backend search или frontend search.
4. Открыть карточку университета.
5. Проверить provenance/evidence chain.
6. При изменении parser/normalizer logic воспроизвести пересборку через `scripts.replay`.

Если изменение не улучшает именно этот сценарий, оно не должно попадать в MVP.

## Что входит в MVP

### Источники

- `official_site.default`
- `aggregator.default`
- `ranking.default`

### Data pipeline

- source registry и endpoint registry;
- manual/seeded ingest path;
- raw artifact storage в MinIO;
- parsed document и extracted fragments;
- claim и claim evidence persistence;
- authoritative bootstrap и dual-source merge;
- resolved facts для canonical fields;
- versioned `delivery.university_card`;
- versioned `delivery.university_search_doc`.

### Delivery surfaces

- backend search API;
- backend university card API;
- backend provenance API;
- frontend home/search/card/evidence flow;
- URL-synced search state и shared `university_id` selection.

### Эксплуатационный минимум

- replay workflow;
- fixture capture/backfill workflow;
- regression suite на captured MVP bundle;
- retry/DLQ topology для parser/normalize/delivery lanes;
- базовые domain metrics;
- starter Prometheus/Grafana assets;
- local demo runbook;
- final compose-up smoke script.

## Что сознательно не входит в MVP

### По источникам и парсингу

- новые source family сверх `official`, `aggregator`, `ranking`;
- browser-rendered crawling как обязательный путь;
- anti-bot обходы, rotating proxies, CAPTCHA handling;
- массовый production crawl по реальным сайтам.

### По нормализации

- fuzzy gray-zone matching как обязательный merge path;
- review queue как основной операционный процесс;
- ручная moderation UI;
- LLM-assisted extraction или resolution в критическом пути.

### По delivery и продукту

- дополнительные entity types кроме университетской карточки;
- полноценный кабинет оператора;
- экспортные форматы, публичный каталог, auth/multi-tenant;
- расширение карточки большим числом полей без нового evidence contract.

### По эксплуатации

- production deployment automation;
- secrets management beyond local env;
- SLO/SLA hardening;
- disaster recovery и backup policy;
- performance tuning под большие объёмы данных.

## Scope Guardrails

До post-MVP backlog запрещено:

- добавлять новый источник только потому, что он “похож на текущие”;
- расширять canonical schema без нового доказуемого source path;
- добавлять UI-страницы вне search/card/provenance;
- подменять воспроизводимый seeded demo “ручными” данными в БД;
- переводить команду на следующий этап, если текущий demo-сценарий ломается.

Любая новая задача должна отвечать хотя бы одному из вопросов:

- делает ли она текущий demo надёжнее;
- убирает ли она ambiguity в существующем pipeline;
- закрывает ли она явный operational gap уже реализованного MVP.

Если ответов нет, задача уходит в post-MVP backlog.

## Immediate Post-MVP Backlog

Приоритет 1:

- `gray-zone` matching с trigram fallback и `review.required.v1`;
- review inbox/read-model для спорных merge-решений;
- отдельный normalizer consume path от parser events до card rebuild без ручного replay.

Приоритет 2:

- реальный scheduled crawl lifecycle с policy-driven refresh;
- больше ranking и aggregator adapters;
- richer field set для карточки: контакты, aliases, рейтинги, дополнительные institutional facts.

Приоритет 3:

- browser rendering fallback для JS-heavy sources;
- observability hardening: alerting rules, retention, more domain dashboards;
- production-safe deploy/run profiles и секреты.

Приоритет 4:

- операторский UI для review и source triage;
- exports/API consumers beyond current frontend demo;
- более сложные matching strategies и clustering.

## MVP Sign-off Criteria

MVP считаем зафиксированным, если одновременно выполняется всё ниже:

- `docker compose up` поднимает рабочий локальный стек;
- `scripts.backfill` воспроизводимо засевает demo-данные;
- smoke script проходит путь frontend shell -> search -> card -> provenance;
- документация соответствует фактическому локальному запуску;
- новые задачи больше не меняют границы MVP, а идут только в post-MVP backlog.
