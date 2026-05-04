# Glossary

Domain terminology used throughout the codebase. Match these terms when writing code, comments, and PR descriptions.

## Pipeline concepts

**Adapter** — a class implementing `SourceAdapter` (`libs/source_sdk/base_adapter.py`) that knows how to fetch and extract data from one *family* of sources (official_sites, aggregators, rankings).

**Adapter family** — top-level grouping by trust model: `official_sites`, `aggregators`, `rankings`. Each family has its own subdirectory under `apps/parser/adapters/`.

**Card** — short for `UniversityCard`, the user-facing read model. Stored in `delivery.university_card`, computed by the normalizer, never edited directly.

**Card version** — monotonic integer identifying a snapshot of a university card. Increments when any resolved fact changes.

**Claim** — a typed assertion about a single field of a single entity, with source attribution and confidence. Stored in `normalize.claim`. Many claims may exist for the same field (one per source); the normalizer picks a winner.

**Claim evidence** — the link between a claim and its source artifact. Stored in `normalize.claim_evidence`. Lets the API trace any field back to a raw HTML byte.

**Crawl run** — one execution of a source crawl. Identified by `crawl_run_id`. Tracked in `ops.pipeline_run`.

**Discovery** — the process of finding new endpoint URLs to crawl, typically by parsing a sitemap. See `tabiturient_sitemap.py`.

**Endpoint** — one URL belonging to a source. Stored in `ingestion.source_endpoint` with its parser profile and crawl policy.

**Evidence chain** — the ordered sequence: `raw_artifact → extracted_fragment → claim → resolved_fact → card_field`. Every visible fact must have a complete chain.

**Fragment** — short for `ExtractedFragment`, a single atomic value extracted from one raw artifact at one location, with a locator and confidence. The "atom" of parsing.

**Locator** — a string identifying *where* in the source a fragment was extracted (e.g., `title`, `div#contacts .phone`, `pdf.page[2].row[5].budget`). Used for debugging and for diff-tracking when source HTML changes.

**Parser profile** — a string like `official_site.kubsu.abiturient_html` that selects which extractor handles a given URL. Format: `<family>.<short_source_key>.<page>_<format>`.

**Pipeline run** — a record in `ops.pipeline_run` representing one orchestrated batch of work (a crawl, a normalization pass, a card rebuild).

**Provenance** — the full evidence chain for a fact, exposed via `GET /api/v1/universities/{id}/provenance`.

**Raw artifact** — the immutable bytes fetched from an external URL. Stored in MinIO, indexed in `ingestion.raw_artifact` with sha256 deduplication.

**Resolution policy** — the rule for picking a winner when claims conflict. Examples: `majority` (most claims wins), `trust_tier` (highest tier wins), `latest_timestamp` (most recent wins).

**Resolved fact** — the winner of conflict resolution among claims for a given (university_id, field_name). Stored in `core.resolved_fact`.

**Source** — a logical data source identified by `source_key`. Has one or more endpoints and one trust tier.

**Source key** — a stable string identifier like `kubsu-official` or `vuzopedia-aggregator`. Kebab-case, lowercase.

**Trust tier** — one of `authoritative`, `trusted`, `auxiliary`, `experimental`. Drives resolution policy weights.

## University domain (added in migration `0002`)

**Faculty** — an academic unit within a university (e.g., "Faculty of Computational Mathematics and Cybernetics"). Has a name and a slug. Stored in `core.faculty`.

**Program** — a specific specialty offered by a university, identified by code (e.g., `01.03.01`), level, and form. Stored in `core.program`. May belong to a faculty.

**Program code** — the official Russian specialty classifier code, format `XX.XX.XX` (e.g., `09.03.01` for Computer Science bachelor).

**Education level** — one of `bachelor` (бакалавриат), `master` (магистратура), `specialist` (специалитет), `phd` (аспирантура).

**Form of study** — one of `full_time` (очная), `part_time` (заочная), `distance` (дистанционная), `mixed` (очно-заочная).

**Admission year** — admission statistics for a program in a specific year: budget seats, paid seats, minimum score, tuition cost. Stored in `core.admission_year`.

**Budget seats** — государственные бюджетные места. The number of state-funded slots in a program for a year.

**Paid seats** — платные места. The number of self-funded slots in a program for a year.

**Min score** — passing score (минимальный балл) — the lowest accepted total EGE score for a program in a year.

**Admission exam** — a required EGE subject for a program (`russian`, `math`, `physics`, `informatics`, `chemistry`, `biology`, `english`, `history`, `social_studies`, `literature`). Stored in `core.admission_exam`.

**Legal info** — INN, OGRN, accreditation status, founded year, institution type. Stored per university in `core.legal_info`.

**Location detail** — region code, full address, lat/lon. Stored per university in `core.location_detail`.

## Russian-language analogues (for source HTML scraping)

| English | Russian |
|---------|---------|
| University | Вуз / университет |
| Applicant | Абитуриент |
| Admission | Приём |
| Bachelor | Бакалавриат |
| Master | Магистратура |
| Specialist | Специалитет |
| PhD | Аспирантура |
| Full-time | Очная форма |
| Part-time | Заочная форма |
| Distance | Дистанционная |
| Budget seats | Бюджетные места |
| Paid seats | Платные места |
| Specialty code | Код направления |
| Min score | Минимальный балл / проходной балл |
| EGE | ЕГЭ (Единый государственный экзамен) |
| Faculty | Факультет |
| Department | Кафедра |
| Tuition | Стоимость обучения |
