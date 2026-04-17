# Практическая архитектура агрегатора данных о вузах

## Исполнительная концепция

Для такой системы правильна не модель `parser -> update university_row`, а модель **evidence-first**: каждый внешний факт о вузе сначала фиксируется как отдельное утверждение с источником, временем получения, версией парсера и трассировкой происхождения, а уже затем из набора утверждений строится каноническая карточка. Такой подход хорошо согласуется и с PROV-моделью provenance, где важны сущности, действия и агенты, и с практикой end-to-end entity resolution, где консолидация обычно разбивается на blocking, matching и clustering, и с truth discovery, где конфликтующие сведения разрешаются через оценку надежности источников, а не простое “большинство голосов”. citeturn8view8turn10view6turn10view0

Практически это означает пять отдельных сервисов поверх трех базовых инфраструктурных опор: **PostgreSQL** как системная и каноническая БД, **MinIO** как неизменяемое хранилище raw-артефактов и промежуточных снимков, и **RabbitMQ** как транспорт асинхронных задач между Scheduler, Parser и Normalizer. У RabbitMQ для этого есть важные свойства, которые действительно нужны пайплайну: manual acknowledgements, publisher confirms, dead-letter exchanges и replicated quorum queues; у MinIO есть versioning и object retention; у PostgreSQL — полнотекстовый поиск, trigram similarity, JSONB и векторный поиск через pgvector, чего достаточно для MVP без обязательного ввода отдельного search engine. citeturn8view6turn8view7turn8view5turn8view4turn9view9turn5search18turn8view2

В целевой архитектуре поток выглядит так:

```text
External Sources
    -> Scheduler
    -> RabbitMQ: parser jobs
    -> Parser
        -> MinIO/raw
        -> PostgreSQL/parsing metadata
        -> RabbitMQ: normalization jobs
    -> Normalizer
        -> PostgreSQL/claims + entity matching + canonical
        -> MinIO/parsed snapshots + LLM assist artifacts
        -> PostgreSQL/delivery projections
    -> Backend API
    -> React Frontend
```

Ключевой принцип здесь в том, что **источником истины является не итоговая карточка**, а связка `raw -> parsed -> claims -> resolved facts -> card projection`. Тогда система умеет не только собирать и показывать данные, но и воспроизводимо их пересчитывать при изменении логики парсинга, правил нормализации или модели сопоставления сущностей. citeturn8view8turn8view4

## Слои данных и поток обработки

Я бы проектировал систему как пять явных слоев данных, соответствующих вашему требованию `ingestion -> parsing -> normalization -> canonical storage -> delivery`. В `ingestion` хранятся реестр источников, правила обхода, задания, запуски и метаданные скачанных артефактов; сами body-данные лежат в MinIO. В `parsing` появляются структурированные фрагменты, привязанные к raw-артефактам и версиям parser plugins. В `normalization` живут claims, candidate pairs, match edges, cluster decisions, source priors и review cases. В `canonical storage` хранятся сущности вуза и разрешенные факты по версиям. В `delivery` лежат денормализованные read-модели для API и UI. PostgreSQL поддерживает и partitioning, и materialized views, но для пользовательской карточки я бы делал именно отдельные projection tables, а materialized views оставил бы для служебной аналитики и административных отчетов. citeturn9view7turn9view8

На практике это удобно разложить по схемам PostgreSQL:

```text
ops          -- schedules, runs, queue audit, job states, errors
ingestion    -- source registry, crawl targets, raw artifact metadata
parsing      -- parsed_document, extracted_fragment, parser_issue
normalize    -- claim, claim_value, entity_candidate, entity_edge, cluster, review_case
core         -- university, alias, resolved_fact, card_version
delivery     -- university_card, search_doc, filter_facets, admin_views
```

Для хранения гибких source-specific payloads и результатных “расширений” полезно использовать `jsonb`; для поиска — `tsvector/tsquery`, ranking и trigram similarity; для гибридного семантического сопоставления — pgvector, который хранит vectors прямо рядом с остальными данными и поддерживает exact и approximate nearest neighbor search. Это позволяет в MVP держать и транзакционную модель, и поиск, и candidate retrieval в одном Postgres-кластере, не раздувая ландшафт сервисов раньше времени. citeturn13view0turn13view1turn9view9turn9view10turn5search18turn8view2

RabbitMQ в этой архитектуре я бы использовал не только для “передачи задач в Parser и Normalizer”, но и для **разделения типов нагрузки**: `parser.high`, `parser.bulk`, `normalize.high`, `normalize.bulk`, а также для `card.updated` и `review.required` событий. Это практичнее, чем одна огромная очередь с попыткой регулировать все полем priority: RabbitMQ прямо отмечает, что quorum queues поддерживают только два уровня message priority и для более сложных priority-сценариев лучше использовать отдельные очереди. Обязательны manual ack, разумный prefetch, DLX на каждую рабочую очередь и идемпотентность consumers, потому что при requeue и connection loss повторные доставки являются нормальной частью модели надежности. citeturn14view0turn8view6turn8view7turn15view0

Для raw-слоя я бы сделал несколько бакетов MinIO: `raw-html`, `raw-json`, `parsed-snapshots`, `llm-assist`, `exports`. Versioning в MinIO позволяет хранить несколько версий одного объекта, а object locking и object retention дают WORM-поведение для защищенных bucket’ов; вместе это делает replay и аудит воспроизводимыми, а lifecycle rules помогают чистить неактуальные non-current versions без ручного обслуживания. citeturn8view4turn9view11

## Сервисная декомпозиция

**Scheduler** должен быть самостоятельным Python-сервисом с собственным API и собственным состоянием в PostgreSQL, а не просто cron-таблицей. Его зона ответственности — `source registry`, `crawl policy`, `job planning`, `run state`, `retry policy`, `manual trigger`, `SLA/freshness`, `pipeline health`. Для периодического планирования достаточно встроенного scheduler’а в самом сервисе, а исполнение асинхронных job-ов удобно возложить на Celery-compatible workers: в документации Celery есть и periodic scheduling через beat, и task retry, и publish retry, и started state, которые хорошо ложатся на требования к отслеживанию run lifecycle. В будущем, если задания вырастут в крупные DAG’и с зависимостями между batch-пайплайнами, этот сервис можно эволюционно заменить или подпереть внешним orchestrator’ом, не ломая contracts между сервисами. citeturn18search0turn14view2turn14view3turn9view0turn9view3turn12view0

**Parser** должен быть не одним “универсальным скриптом”, а набором source adapters внутри одного сервиса. Для каждого источника адаптер реализует минимум четыре шага: `fetch`, `store_raw`, `extract`, `map_to_intermediate`. Fetcher должен уметь брать HTML, JSON, API-responses и Markdown, а для JS-heavy сайтов — переключаться на browser rendering. Для этого в production-профиле разумно держать легковесный HTTP fetcher по умолчанию и headless browser only-on-demand; Playwright здесь удобен именно как fallback-движок, потому что делает auto-waiting и стабилизирует взаимодействие с динамическими DOM-страницами. Каждый fetched artifact должен сохраняться как content-addressed object в MinIO с `sha256`, `source_url`, `http_status`, `content_type`, `etag/last_modified`, `fetched_at`, `render_mode`, `crawl_run_id`. citeturn19view4turn8view4

**Normalizer** — главный интеллектуальный слой системы. Именно здесь должны жить словари, field normalizers, entity matching, deduplication, truth resolution, provenance stitching, confidence scoring и сборка канонической карточки. Исследования по entity resolution описывают end-to-end workflow как последовательность blocking/indexing, matching и clustering; при этом для реальных “грязных” и текстовых данных нейросетевые методы и embeddings полезны, но их надо встраивать в более широкий workflow, а не делать единственным решением. Поэтому сервис Normalizer должен быть устроен как конвейер из детерминированных шагов и ограниченного набора model-assisted шагов. citeturn10view4turn10view6turn11view2turn11view0

**Backend** разумно строить как read/write API на FastAPI. Для внешнего фронтенд-контракта нужны endpoints на поиск, фильтрацию, карточку вуза, связанные сущности, источники данных и служебные административные представления. Для внутренних нужд — API на ручной recrawl, просмотр conflicts и trace карточки до raw-артефактов. FastAPI здесь полезен тем, что дает OpenAPI-based contracts, automatic docs и dependency injection из коробки; а read-model лучше отдавать не из “сырых” normalization tables, а из delivery-проекций, заранее подготовленных Normalizer’ом. citeturn12view0turn12view1turn12view2

**Frontend** стоит строить как React-приложение, композиционно разбитое на `HomePage`, `SearchPage`, `UniversityCardPage`, `EvidenceDrawer`, `AdminRunsPage`, `ReviewQueuePage`. React по своей модели ориентирован на сборку UI из независимых компонентов, а если проект создается “с нуля”, сами React docs указывают, что для такой сборки можно стартовать с Vite; для server-state на клиенте хорошо подходит слой наподобие TanStack Query, потому что он берет на себя fetch/cache/update async-данных. Для вашей системы это важно: карточка вуза, facets, paging и evidence tabs — это именно server-state, а не локальное приложение-с-формами. citeturn19view0turn19view1turn19view2turn19view3

## Каноническая модель и механизм консолидации

Сердце решения — **claim-based canonical model**. Вместо того чтобы писать в `universities.name = ...`, система должна хранить отдельные утверждения вида “источник X говорит, что поле Y равно Z”. PROV-модель удобно ложится на такую конструкцию: raw artifact и claim — это entities, parse run и normalization run — activities, а source adapter, parser version, human reviewer и LLM-assist actor — agents. Это дает объяснимость: любую цифру в карточке можно раскрутить назад до конкретного URL, raw-снимка, parser version, normalization policy и даты принятия решения. citeturn8view8

Практический минимум сущностей я бы сделал таким:

```text
Source
SourceEndpoint
CrawlRun
RawArtifact
ParsedDocument
ExtractedFragment
Claim
ClaimEvidence
EntityCandidate
EntityMatchDecision
University
UniversityAlias
ResolvedFact
CardVersion
ReviewCase
```

Итоговая карточка в delivery слое не должна быть “главной записью”, а должна быть **проекцией** следующих типов фактов:

```json
{
  "university_id": "uuid",
  "canonical_name": { "value": "...", "confidence": 0.98, "sources": [...] },
  "aliases": ["...", "..."],
  "location": { "country": "...", "city": "...", "address": "...", "geo": null },
  "contacts": { "website": "...", "emails": [], "phones": [] },
  "institutional": { "type": "public|private", "founded_year": 0 },
  "programs": [],
  "tuition": [],
  "ratings": [],
  "dormitory": {},
  "reviews": { "summary": null, "items": [] },
  "sources": [],
  "version": { "card_version": 17, "generated_at": "..." }
}
```

Дедупликацию и сопоставление сущностей стоит делать в несколько фаз. Сначала — **field normalization**: case folding, accent folding, нормализация URL/domain, phone cleanup, country/city dictionary normalization, унификация degree/program taxonomy, разбор рейтингов в форму `provider + year + metric + rank/value`. PostgreSQL дает для этого полезные primitives: `unaccent` для diacritics, `citext` для case-insensitive matching, `pg_trgm` для similarity search, полнотекстовый поиск для длинных представлений и pgvector для embedding-based candidate retrieval. Затем — **blocking**: exact keys по домену, официальному сайту, нормализованному названию, паре `city + token signature`, аббревиатурам и другим cheap keys. После этого — pair scoring, а затем clustering только для пар, прошедших пороги уверенности. Такой многоступенчатый workflow прямо соответствует тому, как ER-литература описывает практическое сужение квадратичного пространства сравнений. citeturn10view6turn10view5turn9view5turn9view6turn5search18turn8view2turn11view2

Скоринг я бы делал гибридным. Для каждой пары кандидатов Normalizer вычисляет набор признаков: совпадение домена, совпадение или близость названия, совпадение города/страны, similarity адреса, overlap контактов, overlap программ, semantic similarity длинных текстовых полей, а также priors источников. Далее эти признаки сводятся в `match_score`, а для фактов — в `fact_score`. Для fact resolution полезна truth-discovery логика: итог зависит не только от самого значения, но и от reliability источника и согласованности с другими наблюдениями. Практическая формула может выглядеть так:  
`fact_score = source_prior × parser_confidence × recency_weight × agreement_boost × field_policy_weight × entity_match_confidence`.  
Важно, что score должен быть **field-specific**: официальный сайт должен иметь высокий вес для названия, контактов, стоимости и общежития; рейтинговое агентство — для собственного rank/value; агрегатор — средний вес для discoverability; форумы и отзывы — низкий вес для hard facts и высокий только для opinion-layer. citeturn10view0

Это означает и важное архитектурное разделение: **отзывы и форумы нельзя смешивать с твердыми фактами**. Они должны поступать в отдельный подконтур claims, где canonicalization превращает их не в “правду о вузе”, а в opinion summary: темы, тональность, recurring concerns, representative evidence links. Для hard facts эти источники могут только поднимать флаг “возможно надо перепроверить”, но не переписывать карточку напрямую. Такой дизайн снимает типичную проблему truth pollution, когда noisy user-generated content начинает размывать authoritative metadata. citeturn10view0

Embeddings и модели сопоставления здесь полезны, но в строго ограниченной роли. Исследования по entity matching показывают, что deep learning особенно помогает на текстовых и “грязных” данных, а не обязательно на структурированных записях; отдельно Ditto показывает пользу pre-trained language models и даже возможность внедрять domain knowledge в matching decisions. Из этого следует разумная практическая позиция: embeddings нужны для candidate expansion и gray-zone scoring, а LLM — только как **вспомогательный инструмент на неоднозначных кейсах**, например для сопоставления длинных текстовых описаний программ, приведения нестандартных названий общежитий к таксономии, объяснения спорных merge cases или извлечения структурированного JSON из особенно плохого HTML. Но LLM-output не должен напрямую писать в `ResolvedFact`: он должен попадать в `llm_assist_result` с `prompt_version`, `model_name`, `evidence_ids`, `assistant_confidence` и проходить rule validation либо ручной review. citeturn11view2turn11view1turn11view0

Наконец, требование повторной обработки при изменении логики означает, что система обязана быть **replayable**. Для этого нужны versioned raw snapshots в MinIO, versioned parser outputs, явные `parser_version` и `normalizer_version` на каждой записи, а карточка должна храниться по версиям `card_version` с возможностью пересчета из любого нижележащего слоя. Versioning и object locking в MinIO как раз дают фундамент для такого replay without overwrite, а PROV-подход дает объяснимую модель происхождения каждого факта. citeturn8view4turn9view11turn8view8

## Технический стек и монорепозиторий

Технологически я бы рекомендовал следующий стек. Для всех Python-сервисов — `Python 3.12`, `FastAPI`, `Pydantic`, `SQLAlchemy`, `Alembic`, `psycopg`, `Celery/Kombu`, `HTTPX`, `Playwright` как browser fallback, `RapidFuzz` для string similarity, отдельный embedding adapter, `OpenTelemetry` SDK. В PostgreSQL сразу заложить `jsonb`, `pg_trgm`, `unaccent`, `citext`, `pgvector`. FastAPI здесь оправдан OpenAPI-first моделью и автоматической документацией, Alembic — явным управлением миграциями, SQLAlchemy — нативной поддержкой PostgreSQL dialect, включая JSON/JSONB. citeturn9view3turn12view0turn9view4turn12view3turn13view0turn5search18turn9view5turn9view6turn8view2

Для наблюдаемости я бы закладывал не только service logs, но и полную telemetry line: traces, metrics и structured logs. RabbitMQ Management UI удобен для разработки и базовой диагностики, но сами RabbitMQ docs рекомендуют внешнее monitoring-решение для production и называют Prometheus + Grafana предпочтительным вариантом для long-term metric collection. OpenTelemetry Python закрывает стандартную генерацию traces/metrics/logs, а Prometheus прямо советует instrument everything на уровне сервисов и библиотек. citeturn14view1turn9view2turn9view12turn9view13

Монорепозиторий я бы разложил так:

```text
repo/
  apps/
    scheduler/
      app/
      tests/
      Dockerfile
    parser/
      app/
      adapters/
        official_sites/
        aggregators/
        rankings/
      tests/
      Dockerfile
    normalizer/
      app/
      matchers/
      resolvers/
      tests/
      Dockerfile
    backend/
      app/
      api/
      tests/
      Dockerfile
    frontend/
      src/
        pages/
        widgets/
        entities/
        features/
      public/
      Dockerfile

  libs/
    contracts/
      events/
      dto/
    domain/
      university/
      provenance/
      claims/
    storage/
      postgres/
      minio/
      rabbitmq/
    source_sdk/
      base_adapter.py
      fetchers/
      extractors/
    observability/
      logging/
      metrics/
      tracing/
    quality/
      validation/
      regression/

  schemas/
    canonical/
      university_card.schema.json
      claim.schema.json
    events/
      crawl.request.json
      parse.completed.json
      normalize.request.json
      card.updated.json
    openapi/
      backend.yaml
      scheduler-admin.yaml
    sql/
      init/
      ddl/

  migrations/
    alembic/

  infra/
    docker-compose/
      docker-compose.yml
      docker-compose.override.yml
    postgres/
      initdb/
    rabbitmq/
      definitions.json
    minio/
      bootstrap.sh
    prometheus/
      prometheus.yml
    grafana/
      dashboards/
    env/
      local/
      dev/

  scripts/
    replay/
    backfill/
    fixture_capture/

  tests/
    integration/
    e2e/
    contract/
```

В `docker-compose` для локальной среды достаточно поднять `postgres`, `rabbitmq`, `minio`, `scheduler`, `parser`, `normalizer`, `backend`, `frontend`, `prometheus`, `grafana`. Важно, чтобы **общие контракты событий и canonical schemas жили отдельно от конкретных сервисов**: это дисциплинирует message design, предотвращает “скрытые” несовместимости между сервисами и упрощает версионирование пайплайна. citeturn12view0turn9view4

## MVP и эволюция

Для MVP я бы сознательно ограничил задачу **тремя типами источников**:  
официальный сайт вуза, один HTML/JSON-агрегатор и одно рейтинговое агентство. Такой набор уже покрывает три разных профиля проблем: authoritative source, коммерческий справочник с собственной моделью полей и внешний ранжирующий источник. Этого достаточно, чтобы проверить весь сквозной pipeline: raw capture, parsing, dedup, conflict resolution, provenance и delivery. Форумы и отзывы я бы добавлял следующим шагом, потому что именно они резко повышают неоднозначность схемы и risk of noisy facts.

Сразу, в самом первом релизе, нужно заложить несколько архитектурных решений, которые потом очень дорого добавлять retroactively: **плагинный source SDK**, **неизменяемый raw-layer**, **claim-based canonical model**, **стабильные event contracts**, **версионирование parser/normalizer logic** и хотя бы минимальное отображение provenance в UI**. Если этого не сделать в начале, то при первом же изменении парсера или правил truth-resolution система потеряет воспроизводимость и объяснимость, а это противоречит самой постановке задачи. citeturn8view8turn8view4

Я бы шел по этапам так.

**Фундамент.** Поднять монорепозиторий, общие contracts, Postgres schemas, MinIO buckets, RabbitMQ topology, health endpoints и базовую telemetry. Scheduler, Parser, Normalizer, Backend и Frontend на этом этапе могут быть заглушками, но уже должны общаться через реальные payload-contracts, а не через “временные Python dict”. Это сильно снижает риск архитектурного дрейфа.

**Первая сквозная цепочка.** Реализовать один `crawl.request`, один parser adapter для официального сайта, сохранение raw в MinIO, промежуточный parsed snapshot и простейший Normalizer, который не делает сложный merge, а создает каноническую карточку из одного источника. Цель этапа — не интеллектуальная нормализация, а доказательство жизнеспособности full path `schedule -> fetch -> parse -> normalize -> serve`.

**MVP-консолидация.** Добавить еще два адаптера, ввести `Claim`, `ClaimEvidence`, `ResolvedFact`, `CardVersion`, exact matching по домену и названию, trigram fallback, field policy matrix и evidence drawer на карточке вуза. На этом этапе система уже должна уметь показать пользователю не только итоговое значение, но и “почему выбрано именно оно”.

**Поиск и фильтрация.** Включить `delivery.university_search_doc`, facets, поиск по названию, алиасам, городам, программам и сайта-м. На PostgreSQL этого достаточно через `tsvector`, `pg_trgm` и отдельные projection tables; отдельный search cluster на этом этапе обычно избыточен. citeturn9view9turn9view10turn5search18turn8view2

**Операционная зрелость.** Добавить per-queue DLQ, retry policies, freshness jobs, quality dashboards, regression suite на captured raw pages, review queue для gray-zone matches и source health score. Это именно тот этап, когда система перестает быть “демо-агрегатором” и становится production data product. RabbitMQ и Celery уже дают для этого нужные primitives: acknowledgements, retries, started state, publish retry и dead-lettering. citeturn8view6turn8view7turn14view2turn14view3turn9view0

**Следующий шаг.** После стабилизации deterministic rules имеет смысл включать embeddings для candidate expansion и difficult matching, а затем — LLM-assist для отдельных ambiguous cases. Только после этого стоит масштабировать количество источников и вводить opinion layer из форумов и отзывов. Если Scheduler со временем превратится из “умного cron + queue publisher” в тяжелый оркестратор batch/DAG-процессов, можно эволюционировать к внешнему workflow orchestrator’у: Airflow позиционируется как платформа для scheduling and monitoring batch-oriented workflows, а Prefect — как система с scheduling flow runs и runtime monitoring. Но в MVP это не обязательно; важнее сохранить стабильные service boundaries и contracts, чтобы такой переход был эволюционным, а не переписыванием системы с нуля. citeturn18search19turn18search2

Итоговая рекомендация проста: строить агрегатор вузов как **версионируемый evidence-based data pipeline**, где raw snapshot’ы неизменяемы, claims аудируемы, canonical card вычисляется, а LLM выступает только ассистентом на сложных пограничных кейсах. Такая архитектура одновременно реалистична для MVP, масштабируема для роста и соответствует вашим ключевым нефункциональным требованиям: дедупликации, нормализации, объяснимому разрешению конфликтов, provenance и повторной обработке данных. citeturn8view8turn10view0turn10view6turn8view4