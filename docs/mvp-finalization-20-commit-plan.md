# MVP Finalization Plan

План ниже рассчитан на **20 коммитов суммарно** на **30 апреля, 1 мая и 2 мая 2026 года**.

Принципы:
- сначала фиксируем `test/spec`, потом `feat`;
- extraction rules описываются явно по source family и parser profile;
- в MVP не закладываем LLM как обязательную зависимость;
- если для коммита нужен вход от тебя, это отмечено отдельно.

## Assumptions

- MVP остаётся evidence-first pipeline: `scheduler -> parser -> normalizer -> delivery -> backend -> frontend`.
- На выходе нужен **живой runtime**, а не только `backfill/replay`.
- Источники текущего live MVP:
  - `tabiturient-aggregator`
  - `tabiturient-globalrating`
  - `kubsu-official`
- Базовый card/provenance/search flow уже есть и дальше дорабатывается, а не строится с нуля.

## Day 13 — 2026-04-30

### Commit 1
`test(source-registry): add live MVP source seed contract for kubsu and tabiturient`

Что делаем:
- фиксируем expected source records и endpoint blueprints;
- проверяем `source_key`, `source_type`, `trust_tier`, `parser_profile`, `crawl_policy`.

Нужно от тебя:
- ничего.

### Commit 2
`feat(scripts): add live MVP source bootstrap command for kubsu and tabiturient endpoints`

Что делаем:
- добавляем script/bootstrap для записи live-source catalog в `ingestion.source` и `ingestion.source_endpoint`;
- заводим все endpoint’ы для `kubsu` и стартовые endpoint’ы для `tabiturient`.

Нужно от тебя:
- ничего.

### Commit 3
`test(discovery): add tabiturient sitemap discovery coverage for primary university pages only`

Что делаем:
- тестами закрепляем правила отбора из sitemap;
- берём только `/vuzu/<slug>`;
- исключаем `about`, `proxodnoi`, `dod`, `obsh`, query-string variants и дубликаты.

Нужно от тебя:
- ничего.

### Commit 4
`feat(scheduler): materialize tabiturient university endpoints from sitemap discovery`

Что делаем:
- job читает `https://tabiturient.ru/map/sitemap.php`;
- создаёт/обновляет endpoint records для discovered pages;
- не плодит дубликаты;
- помечает discovery metadata.

Нужно от тебя:
- ничего.

### Commit 5
`test(parser): add fixture-driven tabiturient university extractor specs for identity and contacts`

Что делаем:
- сначала пишем fixtures и extractor tests под `aggregator.tabiturient.university_html`;
- формализуем extraction rules и expected fragments.

Нужно от тебя:
- **да, нужно**:
  - точный список полей для карточки Tabiturient university page;
  - достаточно ли на MVP:
    - `canonical_name`
    - `aliases`
    - `location.city`
    - `contacts.website`
    - `contacts.phones`
    - `contacts.emails`
  - или нужно больше.

### Commit 6
`feat(parser): implement tabiturient university HTML extractor for primary university pages`

Что делаем:
- реализуем extractor под реальный HTML Tabiturient;
- сохраняем source-specific locators и evidence metadata;
- работаем строго по primary page, без secondary subpages.

Нужно от тебя:
- только ответ по полям из commit 5.

### Commit 7 
`test(parser): add tabiturient globalrating extractor specs for structured ranking rows`

Что делаем:
- пишем tests и fixtures под `ranking.tabiturient.globalrating_html`;
- фиксируем expected rating fragments и grouping rules.

Нужно от тебя:
- **да, нужно**:
  - что именно считаем rating payload на MVP:
    - только общий ранг;
    - или все доступные показатели со страницы;
    - если все, то какие именно поля должны выходить в normalized form.

## Day 14 — 2026-05-01

### Commit 8
`feat(parser): implement tabiturient globalrating HTML extractor and ranking row mapping`

Что делаем:
- реализуем extractor для `https://tabiturient.ru/globalrating/`;
- строим fragments совместимые с existing normalizer rating flow;
- сохраняем ranking provider metadata.

Нужно от тебя:
- ответ по expected ranking fields из commit 7.

### Commit 9 - МЫ СЕЙЧАС НА ЭТОМ КОММИТЕ
`test(parser): add KubSU abiturient page extractor specs for canonical and admission contact fields`

Что делаем:
- сначала пишем tests для `official_site.kubsu.abiturient_html`;
- закрепляем, какие поля именно считаем authoritative с landing page.

Нужно от тебя:
- желательно подтвердить, что с этой страницы на MVP парсим только:
  - `canonical_name`
  - `contacts.website`
  - `contacts.emails`
  - `contacts.phones`
- если нужны ещё поля, перечисли их.

### Commit 10
`feat(parser): implement KubSU abiturient extractor profile with source-specific selectors`

Что делаем:
- добавляем profile-specific extractor rules для страницы абитуриента;
- не ломаем generic official extractor;
- metadata помечаем как authoritative.

Нужно от тебя:
- только подтверждение полей из commit 9.

### Commit 11
`test(parser): add KubSU programs-page extraction specs for faculties directions passing scores and budget places`

Что делаем:
- тестами закрепляем extraction contract для `https://www.kubsu.ru/ru/node/44875`;
- определяем форму данных для программных записей.

Нужно от тебя:
- **да, нужно обязательно**:
  - точный список полей для program/admission records;
  - предлагаемый минимум:
    - `programs.code`
    - `programs.name`
    - `programs.faculty`
    - `programs.passing_score`
    - `programs.budget_places`
    - `programs.education_level`
    - `programs.study_mode`
  - если твоя целевая схема другая, нужно утвердить её до реализации.

### Commit 12
`feat(parser): implement KubSU programs-page extractor for structured admission fragments`

Что делаем:
- реализуем page-specific HTML extractor;
- строим structured fragments/intermediate records для program rows;
- сохраняем row locator и provenance.

Нужно от тебя:
- утверждённая schema program fields из commit 11.

### Commit 13
`test(parser): add KubSU PDF extraction regression specs for 2026 places document`

Что делаем:
- добавляем fixture/regression для PDF;
- фиксируем expected records и fallback behavior;
- проверяем, что parser не ломается на binary source.

Нужно от тебя:
- **да, нужно обязательно**:
  - что именно парсим из PDF;
  - нужно ли мёрджить PDF с HTML page по program code/name;
  - если в PDF структура табличная и важны все колонки, перечисли целевые поля.

### Commit 14
`feat(parser): add PDF text extraction pipeline and KubSU places parser`

Что делаем:
- добавляем dependency и parser path для PDF;
- извлекаем budget/admission facts из `2026_places_b_b.pdf`;
- готовим fragments под merge в normalizer.

Нужно от тебя:
- финальный список полей из commit 13.

## Day 15 — 2026-05-02

### Commit 15
`test(normalizer): add merge specs for official html official pdf aggregator and ranking claims`

Что делаем:
- фиксируем end-state merge rules;
- проверяем winner selection:
  - official authoritative fields побеждают;
  - aggregator даёт supporting fields;
  - ranking идёт в ratings;
  - program/admission facts собираются из official HTML + PDF.

Нужно от тебя:
- ничего, если schema program fields уже утверждена.

### Commit 16
`feat(normalizer): merge KubSU program and admission claims into delivery card sections`

Что делаем:
- normalizer начинает собирать structured sections:
  - `programs`
  - `admission`
  - `ratings`
- source evidence остаётся привязанным к field-level facts.

Нужно от тебя:
- только уже утверждённая schema program fields.

### Commit 17
`feat(backend): expose structured admissions sections and field-level provenance in card response`

Что делаем:
- backend card response расширяется живыми sections;
- provenance endpoint начинает отдавать цепочку и для admissions/program rows.

Нужно от тебя:
- ничего.

### Commit 18
`feat(runtime): add parser and normalizer worker entrypoints and wire compose live queue processing`

Что делаем:
- поднимаем реальные worker entrypoints;
- подключаем consumer loops для RabbitMQ;
- доводим compose до live flow, а не только API shell.

Нужно от тебя:
- ничего.

### Commit 19
`test(e2e): cover live flow from source bootstrap and discovery to card search and provenance`

Что делаем:
- полный e2e:
  - bootstrap sources
  - sitemap discovery
  - crawl
  - parse
  - normalize
  - search
  - card
  - provenance
- проверяем, что flow работает без ручного replay.

Нужно от тебя:
- ничего.

### Commit 20
`docs(release): freeze live MVP extraction rules runbook and unresolved post-mvp backlog`

Что делаем:
- финализируем runbook;
- отдельно фиксируем extraction rules по каждому live profile;
- документируем ограничения и post-MVP backlog.

Нужно от тебя:
- ничего.

## Extraction Rules To Freeze During These Commits

### Aggregator — `aggregator.tabiturient.university_html`

Правила:
- вход только из `https://tabiturient.ru/map/sitemap.php`;
- primary university pages только формата `/vuzu/<slug>`;
- secondary pages не используются как первичный entity source;
- каждый extracted fragment должен хранить:
  - `source_url`
  - `locator`
  - `source_field`
  - `adapter_family`
  - `parser_profile`

Нужно утвердить:
- точный состав полей для university card с этой страницы.

### Ranking — `ranking.tabiturient.globalrating_html`

Правила:
- source snapshot берётся GET-запросом со страницы рейтинга;
- ranking rows должны быть детерминированно матчены к университету;
- provider metadata обязательно сохраняется;
- rating fragments должны быть пригодны для current structured rating resolution.

Нужно утвердить:
- только общий ранг или полный набор показателей.

### Official — `official_site.kubsu.abiturient_html`

Правила:
- это authoritative source;
- generic official extractor не должен бездумно переиспользоваться там, где нужны profile-specific selectors;
- contact/identity fields должны идти с высокой confidence и чистым locator trail.

Нужно утвердить:
- только базовая карточка или ещё дополнительные поля.

### Official Programs — `official_site.kubsu.programs_html`

Правила:
- row-based extraction;
- у каждой program row должен быть stable row locator;
- факты должны быть пригодны для merge с PDF.

Нужно утвердить:
- точная schema `programs.*`.

### Official PDF — `official_site.kubsu.places_pdf`

Правила:
- PDF не должен попадать в pipeline как opaque blob-only source;
- extraction должен давать structured facts;
- merge с HTML rules должен быть детерминированный.

Нужно утвердить:
- какие колонки и какие итоговые поля считаются MVP-обязательными.

## What I Need From You Before Specific Commits

### Mandatory

До commit 5:
- список полей для `tabiturient` university page.

До commit 7:
- список ranking fields:
  - только общий ранг;
  - или полный набор ranking metrics.

До commit 11:
- финальная schema для `programs/admission` records.

До commit 13:
- финальная schema для PDF-derived fields и правило merge с HTML page.

### Optional

LLM-assisted conflict handling в MVP я бы **не включал**.

Если всё же хочешь добавить это после commit 20 или вместо части backlog, тогда от тебя потребуется:
- provider;
- env var name для API key;
- model name;
- лимиты по цене/таймауту;
- expected JSON response schema;
- список ситуаций, когда LLM вообще разрешено вызывать.

## Recommended Sequence Of Answers From You

Чтобы не стопорить реализацию, лучше дать ответы одним сообщением в таком порядке:

1. Поля для `tabiturient` university page.
2. Поля для `tabiturient/globalrating`.
3. Schema для `programs/admission` из страницы КубГУ.
4. Schema для PDF и правило merge с HTML.

