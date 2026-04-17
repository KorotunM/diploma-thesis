# Roadmap

## Phase 0. Foundation

- Зафиксировать структуру монорепозитория.
- Описать event contracts и canonical schemas.
- Подготовить SQL-схемы и локальную инфраструктуру.
- Создать health/metrics-ready заглушки сервисов.

## Phase 1. First Vertical Slice

- Один authoritative source adapter.
- `crawl.request.v1 -> parse.completed.v1 -> card.updated.v1`.
- Raw snapshot в MinIO.
- Parsed snapshot в MinIO.
- Первая delivery-карточка из одного источника.

## Phase 2. MVP Consolidation

- Второй и третий тип источников.
- `Claim`, `ClaimEvidence`, `ResolvedFact`, `CardVersion`.
- Exact matching по домену и названию.
- Trigram fallback.
- Evidence drawer в UI.

## Phase 3. Search And Filters

- `delivery.university_search_doc`.
- Поиск по названию, алиасам, городам, программам.
- Facets и paging.

## Phase 4. Operational Maturity

- DLQ и retry policies.
- Freshness monitoring.
- Regression suite на captured raw pages.
- Review queue и gray-zone matches.

## Phase 5. Advanced Matching

- Embeddings для candidate expansion.
- LLM-assist для ambiguous cases.
- Отдельный opinion layer для форумов и отзывов.
