# Current Gap Analysis

Дата анализа: `2026-04-30`

## Короткий вывод

Текущий код уже похож не на пустой foundation, а на **сильный MVP-каркас с рабочими доменными слоями, projection-моделью, replay/backfill/regression и UI-обвязкой**.

Но если мерить не unit/in-memory tests, а **реальную работоспособность в живом `docker compose`**, то проект пока ещё не дотянут до полностью самодостаточного end-to-end MVP.

Главная проблема сейчас не в модели данных и не в backend read API. Главная проблема в том, что:

1. **живой event pipeline не замкнут в runtime;**
2. **frontend в браузере не сможет штатно ходить в backend/scheduler из-за CORS/proxy-разрыва;**
3. **часть critical flows всё ещё демонстрируется через `backfill/replay`, а не через реальный ingest path.**

Ниже разбор по фактам.

## 1. Критичные разрывы до реально работающего MVP

### 1.1. Frontend сейчас не доведён до реальной браузерной связности

Что видно в коде:

- frontend вызывает API по абсолютным URL через `fetch`:
  - `apps/frontend/src/shared/http/client.ts`
  - `apps/frontend/src/shared/backend-api/client.ts`
  - `apps/frontend/src/shared/platform-api/client.ts`
- runtime по умолчанию указывает на `http://localhost:8001..8004`:
  - `apps/frontend/src/shared/runtime/config.ts`
- `vite.config.ts` **не содержит proxy**:
  - `apps/frontend/vite.config.ts`
- в FastAPI apps **не найден `CORSMiddleware`**:
  - `apps/backend/app/main.py`
  - `apps/scheduler/app/main.py`
  - `apps/parser/app/main.py`
  - `apps/normalizer/app/main.py`
  - `libs/observability/service_factory.py`

Почему это критично:

- frontend живёт на `http://localhost:5173`;
- backend/scheduler/parser/normalizer живут на других origin;
- браузер будет делать cross-origin `fetch`;
- без CORS или proxy UI-path не будет стабильно работать в реальном браузере.

Вывод:

- **Сейчас frontend smoke проверяет только HTML shell и backend API отдельно, но не реальную браузерную работу пользовательского UI.**

### 1.2. В compose-профиле не стартуют реальные worker-consumer процессы

Что видно:

- `scheduler` публикует `crawl.request.v1`:
  - `apps/scheduler/app/runs/service.py`
- у parser есть consumer helper:
  - `apps/parser/app/crawl_requests/consumer.py`
- но `apps/parser/Dockerfile` запускает только `uvicorn`, а не consumer loop:
  - `apps/parser/Dockerfile`
- `apps/normalizer/Dockerfile` тоже запускает только `uvicorn`:
  - `apps/normalizer/Dockerfile`
- у normalizer вообще нет отдельного consumer-модуля уровня parser:
  - `apps/normalizer/app/`

Почему это критично:

- сообщения могут публиковаться в RabbitMQ;
- но в живом compose никто не обязан:
  - читать `parser.high` / `parser.bulk`;
  - читать `normalize.high` / `normalize.bulk`;
  - запускать нормализацию после `parse.completed`.

Следствие:

- live event path может **останавливаться в очередях**, даже если topology и emitters уже реализованы.

### 1.3. Parser и normalizer root/internal HTTP endpoints всё ещё содержат stub-логику

Прямые признаки:

- parser internal endpoint возвращает stub payload:
  - `apps/parser/app/main.py`
  - `parser_version="parser.stub.0.1.0"`
  - `metadata={"note": "stub parser completed event"}`
- normalizer internal endpoints тоже stub:
  - `apps/normalizer/app/main.py`
  - `normalizer_version="normalizer.stub.0.1.0"`
  - `metadata={"note": "stub normalize request"}`
  - `CardUpdatedPayload(university_id=uuid4(), card_version=1, ...)`

Почему это важно:

- доменные сервисы внутри `apps/parser/app/crawl_requests/service.py` и normalizer-слои уже продвинутые;
- но публичная runtime-поверхность сервисов всё ещё местами живёт как демонстрационная заглушка;
- это создаёт ложное ощущение “готового сервиса”, хотя HTTP-путь не отражает реальный processing pipeline.

### 1.4. `normalize.request` и `card.updated` не доведены до реального runtime-flow

Что видно:

- `parse.completed` реально эмитится parser-слоем:
  - `apps/parser/app/parse_completed/emitter.py`
- но `normalize.request.v1` живёт только как контракт + stub HTTP path:
  - `libs/contracts/events/pipeline.py`
  - `apps/normalizer/app/main.py`
- `card.updated.v1` присутствует в контрактах и RabbitMQ topology:
  - `libs/contracts/events/pipeline.py`
  - `libs/storage/rabbitmq/topology.py`
- но реального `CardUpdatedEmitter` / публикации из production normalizer path нет.

Почему это критично:

- часть event design уже описана архитектурно;
- но реальный runtime-поток от parse до delivery events **не замкнут полностью**.

### 1.5. Scheduler пока не стал настоящим scheduler’ом

Что видно:

- есть manual trigger endpoint:
  - `POST /admin/v1/crawl-jobs`
  - `apps/scheduler/app/runs/routes.py`
- есть freshness endpoints:
  - `apps/scheduler/app/freshness/routes.py`
- `CrawlPolicy` уже хранит:
  - `schedule_enabled`
  - `interval_seconds`
  - `render_mode`
  - `apps/scheduler/app/sources/models.py`
- но в коде нет daemon/loop/cron-like execution, который бы:
  - выбирал due endpoints;
  - сам создавал crawl jobs;
  - сам публиковал schedule-triggered runs.

Что это значит:

- сейчас scheduler в основном:
  - source registry,
  - manual run publisher,
  - freshness calculator.
- **Автоматический плановый обход источников не завершён.**

## 2. Что ещё недореализовано или упрощено относительно целевого MVP

### 2.1. Demo по-прежнему сильно опирается на `backfill/replay`

Прямо зафиксировано в документации:

- `docs/mvp-demo-runbook.md`
- `tests/fixtures/mvp_bundle/manifest.json`
- `scripts/backfill`
- `scripts/replay`

Что это означает practically:

- карточка и поиск сейчас удобно демонстрируются через seeded fixtures;
- но это ещё не то же самое, что “зарегистрировали источник -> scheduler trigger -> parser -> normalizer -> search/card обновились сами”.

Иными словами:

- **demo path воспроизводим;**
- **live ingestion path до конца не доказан в compose runtime.**

### 2.2. Gray-zone matching частично реализован, но workflow вокруг него нет

Что уже есть:

- trigram matching logic:
  - `apps/normalizer/app/matching/service.py`
- `review_required` emission:
  - `apps/normalizer/app/review_required/emitter.py`
  - `apps/scheduler/app/freshness/emitter.py`

Чего не хватает:

- review inbox/read model;
- операционного consumer path для `review.required`;
- UI или API для принятия merge-решения;
- применения review outcome обратно в normalizer flow.

Вывод:

- **серое совпадение сейчас корректно “останавливается”, но не имеет законченного downstream-процесса.**

### 2.3. Frontend demo-флоу всё ещё частично ручной

Что видно:

- `SearchPage` показывает результат, но не даёт реальное действие “открыть карточку”:
  - `apps/frontend/src/pages/SearchPage.tsx`
- `UniversityCardPage` просит вручную вставить `university_id`:
  - `apps/frontend/src/pages/UniversityCardPage.tsx`

То есть:

- UX уже не stub;
- но путь `search -> click -> card -> evidence` на уровне интерфейса ещё не доведён до естественного пользовательского действия.

Это не разрушает архитектуру, но делает demo менее цельным.

### 2.4. Финальный smoke script проверяет seeded read path, а не реальный background pipeline

Что делает current smoke:

- проверяет frontend shell;
- проверяет `healthz`;
- вызывает backend search/card/provenance;
- валидирует deep-link.

Файл:

- `tests/e2e/compose_demo_smoke.py`

Чего он не проверяет:

- что scheduler trigger публикует job;
- что parser worker реально потребил сообщение;
- что normalizer worker реально построил projections;
- что очереди не стоят с необработанными сообщениями.

Следствие:

- smoke полезен;
- но он **не закрывает самый рискованный runtime-разрыв** между очередями и delivery projection.

### 2.5. Миграционный слой фактически ещё не живёт как migrations history

Что видно:

- schema создаётся bootstrap SQL:
  - `schemas/sql/ddl/010_schemas.sql`
  - `schemas/sql/ddl/020_tables.sql`
- Alembic каталог есть, но пока это только placeholder:
  - `migrations/alembic/README.md`

Что это значит:

- для fresh bootstrap локалки схема поднимается;
- но управляемая эволюция схемы через реальную migration history ещё не поставлена.

Для demo-MVP это терпимо, но для “работоспособной системы, которую продолжают развивать” это уже риск.

## 3. Упрощения, которые допустимы для demo-MVP, но не для следующего этапа

Эти пункты не обязательно блокируют текущий demo, но точно будут ограничивать следующий шаг:

- нет browser-rendered crawling fallback в runtime path, хотя `render_mode="browser"` предусмотрен в моделях;
- карточка университета всё ещё узкая по полям;
- нет real operator workflow для review/triage;
- нет production-safe миграционного процесса;
- observability есть как стартовый слой, но не как полноценная operational practice;
- `card.updated` topology есть, а реальный subscriber ecosystem вокруг delivery events отсутствует.

## 4. Что уже выглядит достаточно зрелым

Это важно, чтобы не создавать ложное ощущение “ничего не сделано”.

Уже хорошо заложены:

- source registry и endpoint registry;
- raw artifact persistence в Postgres + MinIO;
- parsed document / extracted fragment persistence;
- claims / claim evidence / resolved facts / card/search projections;
- multi-source merge логика;
- provenance read path;
- backend search/card/provenance APIs;
- replay / backfill / fixture capture;
- regression suite;
- retry / DLQ topology;
- базовые метрики и dashboard assets.

То есть архитектурный фундамент у проекта уже сильный. Основные недостачи теперь находятся не в схеме и не в domain-модели, а в **runtime orchestration и интеграционной замкнутости**.

## 5. Минимальный набор, который я бы доделал первым

### P0. Без этого я бы не называл систему реально работающей

1. Добавить **CORS или Vite proxy** для `frontend -> scheduler/backend/parser/normalizer`.
2. Запустить **реальные worker entrypoints** в compose:
   - parser consumer для `parser.high` / `parser.bulk`;
   - normalizer consumer для `normalize.high` / `normalize.bulk`.
3. Убрать или заменить **stub internal endpoints** parser/normalizer реальным orchestration path.
4. Довести **live path**:
   - `manual crawl -> parser consume -> parse.completed -> normalizer consume -> projections updated`.

### P1. Это нужно, чтобы MVP был цельным, а не только “технически показываемым”

5. Сделать из search result нормальное действие:
   - `open card` / `select university`.
6. Расширить smoke/e2e так, чтобы он проверял не только seeded read API, но и фактическую обработку job в очереди.
7. Зафиксировать, что именно является источником истины для demo:
   - live trigger,
   - backfill,
   - replay.

### P2. Это следующий слой после закрытия live runtime

8. Реальный scheduled crawl loop.
9. Review inbox/workflow для `review.required`.
10. Alembic migrations history.
11. Browser fallback для JS-heavy sources.

## 6. Итоговая оценка

Если отвечать жёстко и коротко:

- **как набор доменных компонентов проект уже очень близок к MVP;**
- **как реально самозапускающийся live pipeline в compose — ещё нет.**

Сейчас состояние лучше описывается так:

- **demoable through seeded data and controlled flows**;
- **not yet fully operational as a live end-to-end asynchronous product**.

Главные недостающие вещи до реальной работоспособности:

- CORS/proxy,
- реальные worker-consumers,
- живой normalizer consume path,
- отсутствие stub-вставок на runtime-границах,
- автоматический scheduler flow,
- более честный e2e на фоне живых очередей.
