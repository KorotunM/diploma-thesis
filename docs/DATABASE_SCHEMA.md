# Схема базы данных — Абитуриент+

## Обзор

PostgreSQL с расширениями `citext`, `pg_trgm`, `unaccent`, `vector`.  
База данных разделена на **6 схем**, каждая соответствует этапу пайплайна.

```
ops          — служебные данные (запуски, пользователи)
ingestion    — источники, эндпоинты, сырые артефакты
parsing      — результаты парсинга, извлечённые фрагменты
normalize    — клеймы и доказательная база
core         — каноническая доменная модель университета
delivery     — read-модели для Frontend/API
```

---

## Схема пайплайна и таблицы

```
Scheduler          Ingestion             Parser              Normalizer           Backend
─────────          ─────────             ──────              ──────────           ───────
source        ──►  source_endpoint  ──►  raw_artifact   ──►  claim          ──►  university_search_doc
pipeline_run       (crawl policy)        parsed_document      claim_evidence      university_card
                                         extracted_fragment   university
                                                              resolved_fact
                                                              core.program / ...
```

---

## Схема `ops` — служебные данные

### `ops.pipeline_run`

Журнал каждого запуска пайплайна (краул, replay, backfill).

| Колонка | Тип | Описание |
|---------|-----|----------|
| `run_id` | UUID PK | Идентификатор запуска (он же `crawl_run_id` в событиях) |
| `run_type` | text | `crawl` / `replay` / `backfill` |
| `status` | text | `queued` → `published` → `running` → `succeeded` / `failed` / `canceled` |
| `trigger_type` | text | `manual` / `schedule` / `replay` |
| `source_key` | text | Ключ источника, например `kubsu-official` |
| `started_at` | timestamptz | Момент создания записи |
| `finished_at` | timestamptz | Момент завершения (NULL для активных) |
| `metadata` | jsonb | `endpoint_id`, `endpoint_url`, `parser_profile`, `priority`, результат публикации в RabbitMQ |

**Используется:** Scheduler (запись при триггере), seed-скрипт (проверка идемпотентности через `status='succeeded'`), Grafana (мониторинг статусов).

---

### `ops.app_user`

Пользователи для входа в административный интерфейс.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `user_id` | text PK | UUID в виде строки |
| `username` | text UNIQUE | Логин |
| `password_hash` | text | Хеш пароля (demo: plain text `admin`) |
| `role` | text | `admin` / `viewer` |
| `created_at` | timestamptz | — |

**Используется:** Backend — эндпоинт `POST /admin/v1/login` для аутентификации.

---

## Схема `ingestion` — источники и сырые данные

### `ingestion.source`

Реестр источников данных.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `source_id` | UUID PK | — |
| `source_key` | citext UNIQUE | Стабильный идентификатор: `kubsu-official`, `tabiturient-aggregator` |
| `source_type` | text | `official_site` / `aggregator` / `ranking` |
| `trust_tier` | text | `authoritative` / `trusted` / `auxiliary` / `experimental` |
| `is_active` | boolean | Включён ли источник в обход |
| `metadata` | jsonb | Discovery rules, endpoint blueprints, история сидинга |

**Используется:** Scheduler (регистрация через `source_bootstrap`), Normalizer (проверка tier при consolidate_claims).

---

### `ingestion.source_endpoint`

Конкретные URL для обхода с политикой краула.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `endpoint_id` | UUID PK | — |
| `source_id` | UUID FK → source | — |
| `endpoint_url` | text | Полный URL страницы |
| `parser_profile` | text | Ключ экстрактора: `official_site.kubsu.abiturient_html` |
| `crawl_policy` | jsonb | `interval_seconds`, `timeout_seconds`, `allowed_content_types`, `request_headers`, `max_retries` |

**Индексы:** UNIQUE (source_id, endpoint_url).

**Используется:** Scheduler (планирование и триггер краула), seed-скрипт (список эндпоинтов для засева), Discovery service (создание новых эндпоинтов из sitemap).

---

### `ingestion.raw_artifact`

Запись о каждом скачанном сыром файле. Сам файл хранится в MinIO.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `raw_artifact_id` | UUID PK | — |
| `crawl_run_id` | UUID | Ссылка на `ops.pipeline_run.run_id` |
| `source_key` | text | — |
| `source_url` | text | URL, с которого скачан артефакт |
| `final_url` | text | URL после редиректов |
| `http_status` | int | HTTP-код ответа |
| `content_type` | text | `text/html`, `application/pdf`, ... |
| `content_length` | bigint | Размер в байтах |
| `sha256` | text | Хеш содержимого (для дедупликации) |
| `storage_bucket` | text | MinIO bucket: `raw-html`, `raw-json` |
| `storage_object_key` | text | Путь к объекту в MinIO |
| `fetched_at` | timestamptz | — |
| `metadata` | jsonb | etag, last_modified, заголовки |

**Индексы:** UNIQUE (source_key, sha256) — дедупликация одинаковых документов.

**Используется:** Parser (создание после успешного fetch), Normalizer (получение evidence), Backend provenance API.

---

## Схема `parsing` — результаты парсинга

### `parsing.parsed_document`

Метаданные одного запуска экстрактора над артефактом.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `parsed_document_id` | UUID PK | — |
| `crawl_run_id` | UUID | — |
| `raw_artifact_id` | UUID FK → raw_artifact | Исходный артефакт |
| `source_key` | text | — |
| `parser_profile` | text | Какой экстрактор использовался |
| `parser_version` | text | Версия адаптера |
| `entity_type` | text | `university` |
| `entity_hint` | text | Подсказка парсера (например, название вуза) |
| `extracted_fragment_count` | int | Количество извлечённых фрагментов |
| `parsed_at` | timestamptz | — |
| `metadata` | jsonb | Детали выполнения, execution_id |

**Индексы:** UNIQUE (raw_artifact_id, parser_version) — один экстрактор версии N над одним артефактом.

**Используется:** Parser (создание), Normalizer (получение parsed_document_id для claim).

---

### `parsing.extracted_fragment`

Один атомарный факт, извлечённый из артефакта.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `fragment_id` | UUID PK | — |
| `parsed_document_id` | UUID FK → parsed_document | — |
| `raw_artifact_id` | UUID FK → raw_artifact | — |
| `source_key` | text | — |
| `field_name` | text | `canonical_name`, `contacts.website`, `programs.budget_places`, ... |
| `value` | jsonb | Извлечённое значение (строка, список, число) |
| `value_type` | text | `str` / `list` / `int` / `float` / `bool` |
| `locator` | text | CSS-селектор или XPath, где найдено значение (для провенанса) |
| `confidence` | double | Уверенность экстрактора [0.0–1.0] |
| `metadata` | jsonb | `parser_profile`, `adapter_family`, доп. контекст |

**Используется:** Parser (создание), Normalizer (читает для построения клеймов и evidence).

---

## Схема `normalize` — клеймы и доказательства

### `normalize.claim`

Утверждение о поле конкретного университета с атрибуцией источника.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `claim_id` | UUID PK | — |
| `parsed_document_id` | UUID FK | Документ, из которого получен клейм |
| `source_key` | text | Источник утверждения |
| `field_name` | text | `canonical_name`, `location.city`, ... |
| `value_json` | jsonb | Значение клейма |
| `entity_hint` | text | Подсказка для сопоставления (название вуза) |
| `parser_version` | text | — |
| `normalizer_version` | text | — |
| `parser_confidence` | double | Доверие парсера |
| `created_at` | timestamptz | — |
| `metadata` | jsonb | Детали разрешения конфликтов |

**Логика:** По одному полю может быть много клеймов от разных источников. Нормализатор выбирает победителя по `FieldResolutionPolicyMatrix` (trust_tier + confidence + timestamp).

**Используется:** Normalizer (создание и conflict resolution), Normalizer (список клеймов университета при повторном парсинге).

---

### `normalize.claim_evidence`

Доказательная ссылка — какой артефакт подтверждает конкретный клейм.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `evidence_id` | UUID PK | — |
| `claim_id` | UUID FK → claim | — |
| `raw_artifact_id` | UUID FK → raw_artifact | Конкретный скачанный файл |
| `fragment_id` | UUID (nullable) | Конкретный фрагмент в документе |
| `source_key` | text | — |
| `source_url` | text | URL источника |
| `captured_at` | timestamptz | Момент фиксации |
| `metadata` | jsonb | — |

**Используется:** Backend (provenance API — `/api/v1/universities/{id}/provenance`), Frontend (Evidence Drawer).

---

## Схема `core` — каноническая модель университета

### `core.university`

Центральная сущность системы. Один ряд = один реальный университет.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `university_id` | UUID PK | Детерминированный UUID от source_key (не случайный!) |
| `canonical_name` | citext | Официальное название |
| `canonical_domain` | citext | Домен сайта: `kubsu.ru` |
| `country_code` | text | `RU`, `US`, ... |
| `city_name` | text | `Краснодар` |
| `created_at` | timestamptz | — |
| `metadata` | jsonb | `source_key`, `trust_tier`, `source_snapshots`, история слияний |

**Индексы:** btree (canonical_name, canonical_domain), GIN trigram (canonical_name) — для нечёткого поиска при матчинге.

**Используется:** Normalizer (создание/обновление), Normalizer matching (поиск по domain/trigram), Backend (поиск, карточка).

---

### `core.university_alias`

Альтернативные названия университета.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `alias_id` | UUID PK | — |
| `university_id` | UUID FK → university | — |
| `alias_name` | citext | Например: `КубГУ`, `Kuban State University` |
| `alias_kind` | text | `display` / `abbreviation` / `historical` |

**Используется:** Normalizer (запись), Backend search (включается в tsvector поиска).

---

### `core.resolved_fact`

Победивший клейм по каждому полю — итог conflict resolution.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `resolved_fact_id` | UUID PK | — |
| `university_id` | UUID FK → university | — |
| `field_name` | text | `canonical_name`, `contacts.website`, ... |
| `value_json` | jsonb | Финальное значение |
| `fact_score` | double | Итоговый score после разрешения |
| `resolution_policy` | text | Политика: `authoritative_anchor_exact_match_merge`, ... |
| `card_version` | int | Версия карточки, к которой относится факт |
| `resolved_at` | timestamptz | — |
| `metadata` | jsonb | claim_ids, evidence_ids, источники |

**Используется:** Normalizer (создание после разрешения), Normalizer card projection (формирование card_json).

---

### `core.card_version`

Счётчик версий карточки университета.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `university_id` | UUID FK | — |
| `card_version` | int | Монотонно растущий номер |
| `normalizer_version` | text | Версия нормализатора, создавшего версию |
| `generated_at` | timestamptz | — |

PK: (university_id, card_version).

**Используется:** Normalizer (bump при каждом обновлении), delivery-таблицы (FK constraint).

---

### `core.faculty`

Факультеты/институты университета.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `faculty_id` | UUID PK | — |
| `university_id` | UUID FK → university | — |
| `name` | citext | `Физико-технический факультет` |
| `slug` | text | `fiztech` |
| `metadata` | jsonb | — |

UNIQUE: (university_id, slug).

---

### `core.program`

Направления подготовки (специальности).

| Колонка | Тип | Описание |
|---------|-----|----------|
| `program_id` | UUID PK | — |
| `university_id` | UUID FK | — |
| `faculty_id` | UUID FK nullable → faculty | — |
| `code` | citext | ФГОС-код: `01.03.02` |
| `name` | citext | `Прикладная математика и информатика` |
| `level` | text | `bachelor` / `master` / `specialist` / `phd` |
| `form` | text | `full_time` / `part_time` / `distance` / `mixed` |
| `duration_years` | int | Срок обучения |
| `language` | text | `ru` |
| `metadata` | jsonb | — |

UNIQUE: (university_id, code, level, form).  
**Индексы:** GIN trigram (name), btree (code), btree (level, form).

---

### `core.admission_year`

Данные приёма по конкретному году для программы.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `program_id` | UUID FK → program | — |
| `year` | int | Год приёма: `2026` |
| `budget_seats` | int | Бюджетных мест |
| `paid_seats` | int | Платных мест |
| `min_score` | int | Минимальный проходной балл |
| `tuition_cost_rub` | numeric(12,2) | Стоимость обучения |
| `metadata` | jsonb | — |

PK: (program_id, year). CHECK: year BETWEEN 1900 AND 2100.

---

### `core.admission_exam`

Обязательные и вступительные экзамены для программы/года.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `program_id` | UUID FK | — |
| `year` | int | — |
| `subject` | text | `Математика`, `Русский язык`, `Физика` |
| `min_score` | int | Минимальный балл по предмету |
| `is_required` | boolean | Обязательный или по выбору |

PK: (program_id, year, subject). CASCADE DELETE от admission_year.

---

### `core.legal_info`

Юридическая информация об организации.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `university_id` | UUID PK FK | 1:1 с university |
| `inn` | text | ИНН |
| `ogrn` | text | ОГРН |
| `accreditation_status` | text | Статус аккредитации |
| `accreditation_valid_until` | date | Срок действия |
| `founded_year` | int | Год основания |
| `institution_type` | text | `federal` / `regional` / `private` |

---

### `core.location_detail`

Расширенная геолокация.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `university_id` | UUID PK FK | 1:1 с university |
| `region_code` | text | Код региона РФ |
| `region_name` | text | `Краснодарский край` |
| `full_address` | text | Полный адрес |
| `latitude` | numeric(9,6) | Широта |
| `longitude` | numeric(9,6) | Долгота |

---

### `core.statistics_yearly`

Статистика вуза по годам.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `university_id` | UUID FK | — |
| `year` | int | — |
| `students_count` | int | Число студентов |
| `faculty_staff_count` | int | Число ППС |

PK: (university_id, year).

---

## Схема `delivery` — read-модели для API

### `delivery.university_card`

Денормализованная JSON-карточка университета — финальный результат пайплайна.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `university_id` | UUID | — |
| `card_version` | int | — |
| `card_json` | jsonb | Полная карточка: имя, контакты, программы, рейтинги, провенанс |
| `search_text` | tsvector | Полнотекстовый индекс (для legacy поиска) |
| `generated_at` | timestamptz | — |

PK: (university_id, card_version).

> **Важно:** никогда не писать напрямую. Только нормализатор через `UniversityCardProjectionService`.

---

### `delivery.university_search_doc`

Плоская read-модель для поиска по API. Обновляется синхронно с карточкой.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `university_id` | UUID | — |
| `card_version` | int | FK → core.card_version |
| `canonical_name` | text | — |
| `canonical_name_normalized` | text | Транслитерированное/нормализованное |
| `website_url` | text | — |
| `website_domain` | citext | — |
| `country_code` | text | — |
| `city_name` | text | — |
| `aliases` | text[] | Массив псевдонимов |
| `search_document` | jsonb | Дополнительные поля для поиска |
| `search_text` | tsvector | Взвешенный полнотекстовый индекс |
| `metadata` | jsonb | source_type, source_snapshots (для фильтрации) |
| `generated_at` | timestamptz | — |

**Индексы:** GIN (search_text), GIN trigram (canonical_name), btree (country_code, city_name), btree (website_domain).

**Используется:** Backend `GET /api/v1/search` — именно эта таблица читается при поиске. Поиск комбинирует `tsvector` (ts_rank) + trigram (similarity) с весами 0.7 / 0.3.

---

## Диаграмма связей

```
ops.pipeline_run ──────────────────────────────────────────────┐
                                                                │ crawl_run_id
ingestion.source ──► ingestion.source_endpoint                 │
                                                                ▼
                              ingestion.raw_artifact ◄──────────┘
                                      │
                                      ▼
                         parsing.parsed_document
                                      │
                                      ▼
                         parsing.extracted_fragment
                                      │
                                      ▼
                           normalize.claim ──► normalize.claim_evidence
                                      │                │
                                      │                └──► ingestion.raw_artifact
                                      ▼
                            core.university ◄────────────────────────────┐
                                  │                                       │
                        ┌─────────┼──────────┬──────────┬──────────┐     │
                        ▼         ▼          ▼          ▼          ▼     │
               university_alias  faculty   legal_info  location  stats  resolved_fact
                                   │                                      │
                                   └──► program ──► admission_year        │
                                              └──► admission_exam         │
                                                                          │
                                    core.card_version ◄───────────────────┘
                                            │
                              ┌─────────────┴──────────────┐
                              ▼                             ▼
                delivery.university_card   delivery.university_search_doc
                              │
                              ▼
                         Backend API ──► Frontend
```

---

## Этапы пайплайна и таблицы

| Этап | Сервис | Читает | Пишет |
|------|--------|--------|-------|
| 1. Регистрация источников | Scheduler | — | `ingestion.source`, `ingestion.source_endpoint` |
| 2. Планирование краула | Scheduler Worker | `ingestion.source_endpoint` | `ops.pipeline_run` |
| 3. Скачивание | Parser Worker | `ops.pipeline_run` | `ingestion.raw_artifact` (+ MinIO) |
| 4. Извлечение данных | Parser Worker | `ingestion.raw_artifact` | `parsing.parsed_document`, `parsing.extracted_fragment` |
| 5. Построение клеймов | Normalizer Worker | `parsing.extracted_fragment` | `normalize.claim`, `normalize.claim_evidence` |
| 6. Bootstrap университета | Normalizer Worker | `ingestion.source`, `normalize.claim`, `core.university` | `core.university`, `core.university_alias` |
| 7. Разрешение конфликтов | Normalizer Worker | `normalize.claim` (все источники) | `core.resolved_fact` |
| 8. Проекция карточки | Normalizer Worker | `core.resolved_fact` | `core.card_version`, `delivery.university_card`, `delivery.university_search_doc`, `core.program`, `core.admission_year` |
| 9. Поиск | Backend | `delivery.university_search_doc`, `core.university` | — |
| 10. Карточка + провенанс | Backend | `delivery.university_card`, `normalize.claim_evidence`, `ingestion.raw_artifact` | — |
| 11. Аутентификация | Backend | `ops.app_user` | — |

---

## Расширения PostgreSQL

| Расширение | Использование |
|------------|---------------|
| `citext` | Case-insensitive поля: canonical_name, canonical_domain, source_key — уникальность без учёта регистра |
| `pg_trgm` | GIN trigram индексы для нечёткого поиска (`similarity()`, `%` оператор) |
| `unaccent` | Нормализация текста (убирает диакритику) при поиске |
| `vector` | pgvector — инфраструктура готова для embedding-поиска (не используется в MVP) |
