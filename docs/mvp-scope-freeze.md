# MVP Scope Freeze

Этот документ фиксирует границы live MVP после завершения commit 20.

Связанный runbook:

- [Live MVP Runbook](./mvp-demo-runbook.md)

## Цель текущего MVP

Показать воспроизводимый `evidence-first` pipeline для карточки университета:

- `scheduler -> parser -> normalizer -> delivery -> backend -> frontend`
- с живым queue-processing через worker entrypoints
- с доказуемым provenance trail до уровня поля
- без обязательного `backfill/replay` в критическом demo-пути

## Замороженный live source set

### `tabiturient-aggregator`

- discovery profile: `aggregator.tabiturient.sitemap_xml`
- entity profile: `aggregator.tabiturient.university_html`
- trust tier: `trusted`
- роль: secondary source

### `tabiturient-globalrating`

- ranking profile: `ranking.tabiturient.globalrating_html`
- trust tier: `trusted`
- роль: ranking source

### `kubsu-official`

- landing profile: `official_site.kubsu.abiturient_html`
- programs profile: `official_site.kubsu.programs_html`
- trust tier: `authoritative`
- роль: authoritative source

### Исключено из MVP

- `official_site.kubsu.places_pdf`
- любой merge path, требующий PDF-derived claims

## Freeze Extraction Rules

### `aggregator.tabiturient.university_html`

Источник:

- только страницы, materialized из `https://tabiturient.ru/map/sitemap.php`
- только root URL формата `/vuzu/<slug>`

Исключаем:

- `/about`
- `/proxodnoi`
- query-string variants
- любые secondary subpages

Поля, которые реально извлекаются в текущем MVP:

- `canonical_name`
- `aliases`
- `contacts.website`

Обязательная provenance metadata:

- `provider_name = Tabiturient`
- `source_field`
- `external_id`
- `adapter_family`
- `parser_profile`

### `ranking.tabiturient.globalrating_html`

Источник:

- `https://tabiturient.ru/globalrating/`

Поля, которые реально извлекаются:

- `canonical_name`
- `aliases`
- `ratings.provider`
- `ratings.year`
- `ratings.metric`
- `ratings.rank`
- `ratings.value`
- `ratings.category`
- `ratings.change.direction`
- `ratings.change.delta`

Обязательная metadata:

- `provider_key`
- `provider_name`
- `rating_item_key`
- `record_group_key`
- `external_id`
- `rank_display`
- `scale`

### `official_site.kubsu.abiturient_html`

Источник:

- `https://www.kubsu.ru/ru/abiturient`

Поля, которые реально извлекаются:

- `canonical_name`
- `contacts.website`
- `contacts.emails`
- `contacts.phones`

Правило:

- profile-specific selectors обязательны
- generic `official_site.default` не считается заменой для этого профиля

### `official_site.kubsu.programs_html`

Источник:

- `https://www.kubsu.ru/ru/node/44875`

Форма:

- row-based extraction
- каждая строка программы идет отдельным `record_group_key`
- `faculty` берется из section row с `colspan`
- `year` берется из header блока passing-score column

Поля, которые реально извлекаются:

- `programs.faculty`
- `programs.code`
- `programs.name`
- `programs.budget_places`
- `programs.passing_score`
- `programs.year`

Дополнительная metadata:

- `record_group_key`
- `entity_type = admission_program`
- `faculty`
- `program_code`
- `program_year`
- `source_field`

### `official_site.kubsu.places_pdf`

Состояние:

- profile сохранен в registry
- extraction и merge не входят в live MVP release

Правило:

- не использовать как обязательную часть demo flow
- любые задачи по PDF автоматически уходят в post-MVP backlog

## Freeze Merge Rules

- authoritative KubSU fields выигрывают у secondary source claims
- ranking claims не конкурируют за canonical fields и уходят в `ratings`
- structured program facts строятся только из `official_site.kubsu.programs_html`
- contacts/email/phone admission section строится только из authoritative HTML
- provenance для delivery card должен оставаться field-level

## Что входит в MVP

### Runtime

- source bootstrap через `scripts.source_bootstrap`
- Tabiturient sitemap discovery через scheduler admin route
- manual crawl publishing через scheduler
- parser worker consume path
- normalizer worker consume path
- delivery card/search projections

### Product surface

- backend `search`
- backend `card`
- backend `provenance`
- frontend home/search/card/evidence flow

### Engineering minimum

- regression suite
- replay workflow
- compose stack с worker processes
- базовая observability

## Что сознательно не входит в MVP

- PDF extraction
- scheduled autonomous refresh loop
- gray-zone review inbox и operator workflow
- LLM-assisted extraction или merge
- расширение карточки beyond current evidence contract
- production deploy automation

## Unresolved Post-MVP Backlog

### Priority 1

- `official_site.kubsu.places_pdf` extraction
- deterministic merge `programs_html + places_pdf`
- scheduled crawl lifecycle поверх `crawl_policy`

### Priority 2

- UI-path `search -> open card` без ручного ввода `university_id`
- richer KubSU admission sections beyond current field set
- расширение authoritative coverage на новые official sources

### Priority 3

- gray-zone matching workflow с review inbox
- subscriber ecosystem вокруг `card.updated`
- richer ranking source set beyond Tabiturient

### Priority 4

- browser-render fallback
- production migrations history
- alerting, retention, ops hardening

## Scope Guardrails

До post-MVP backlog запрещено:

- включать PDF обратно в sign-off path
- добавлять новые source family без отдельного extraction contract
- расширять schema карточки без доказуемого source path
- подменять live queue flow ручным `replay` и считать это release-ready runtime

Новая задача остается в MVP только если она:

- усиливает текущий live path
- убирает ambiguity в существующем merge/provenance
- исправляет конкретный runtime gap уже зафиксированного release
