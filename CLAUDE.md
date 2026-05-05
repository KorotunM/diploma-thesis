# CLAUDE.md

Project guide for AI agents (Claude Code, Cursor, Copilot). Read this first.

## What this project is

`diploma-thesis` is an **evidence-first university aggregator**. It crawls Russian university websites and aggregator portals, extracts claims, resolves conflicts between sources, and serves a canonical "university card" with full provenance — every visible fact traces back to a raw artifact in MinIO.

Core principle: `raw → parsed → claims → resolved facts → delivery projection`. The user-facing card is a *computed read model*, never edited directly.

## Architecture at a glance

```
                                 ┌──────────────┐
                                 │  Scheduler   │ POST /admin/v1/runs
                                 │  (port 8001) │ — manual trigger
                                 └──────┬───────┘
                                        │ crawl.request.v1 events
                                        ▼
                                  [RabbitMQ: parser.jobs]
                                        │
                                 ┌──────▼───────┐    raw HTML/PDF
                                 │   Parser     │────────────────► MinIO
                                 │  (port 8002) │   ingestion/raw/...
                                 └──────┬───────┘
                                        │ parse.completed.v1 events
                                        ▼
                                  [RabbitMQ: normalize.jobs]
                                        │
                                 ┌──────▼───────┐    claims +
                                 │  Normalizer  │    resolved_facts +
                                 │  (port 8003) │────► PostgreSQL
                                 └──────┬───────┘    delivery.university_card
                                        │ card.updated events
                                        ▼
                                 ┌──────────────┐
                                 │   Backend    │◄──── REST GET
                                 │  (port 8004) │      /api/v1/search
                                 └──────┬───────┘      /api/v1/universities/{id}
                                        │              /api/v1/universities/{id}/provenance
                                        ▼
                                 ┌──────────────┐
                                 │  Frontend    │
                                 │   (nginx,    │
                                 │   port 5173) │
                                 └──────────────┘
```

## Where things live

| Concept | Location |
|---------|----------|
| FastAPI services | `apps/{scheduler,parser,normalizer,backend}/app/` |
| Worker entrypoints | `apps/{scheduler,parser,normalizer}/app/worker.py` |
| React frontend | `apps/frontend/src/` |
| Event contracts (Pydantic) | `libs/contracts/events/` |
| Domain models | `libs/domain/` |
| Storage clients (Postgres/RabbitMQ/MinIO) | `libs/storage/` |
| Parser adapters | `apps/parser/adapters/{official_sites,aggregators,rankings}/` |
| Adapter SDK base classes | `libs/source_sdk/` |
| Source registry blueprints | `libs/source_catalog/mvp_live.py` |
| Source bootstrap workflow | `scripts/source_bootstrap/` |
| Demo data seed | `scripts/seed_demo_data/` |
| Alembic migrations | `migrations/alembic/` |
| Docker Compose stack | `infra/docker-compose/docker-compose.yml` |
| Local env vars (dev only) | `infra/env/local/app.env` |
| Prometheus rules + Grafana dashboards | `infra/{prometheus,grafana}/` |
| Tests | `tests/{unit,integration,e2e,contract,regression}/` |

## Critical files to read first

If you have 5 minutes and need to understand the system:

1. [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — 1-page reference
2. [`libs/contracts/events/pipeline.py`](libs/contracts/events/pipeline.py) — pipeline event contracts (the *interface* between services)
3. [`libs/source_sdk/base_adapter.py`](libs/source_sdk/base_adapter.py) — adapter contract
4. [`libs/source_catalog/mvp_live.py`](libs/source_catalog/mvp_live.py) — the source registry (which sites we crawl)
5. [`migrations/alembic/`](migrations/alembic/) — DB schema (the *shape* of all stored data)

## How to run locally

```bash
make up          # bring up the full stack
make seed        # bootstrap 8 sources + trigger one crawl per source
make test        # run unit + integration tests
make logs        # tail all service logs
make down        # tear down (volumes preserved)
make down-clean  # tear down + drop volumes
```

Without `make`:

```bash
docker compose -f infra/docker-compose/docker-compose.yml up -d --build
python -m scripts.source_bootstrap     # populate the source registry
python -m scripts.seed_demo_data       # trigger demo crawls
```

## Common tasks (recipes)

### Add a new parser source

1. Add a `SourceBlueprint` in [`libs/source_catalog/mvp_live.py`](libs/source_catalog/mvp_live.py)
2. For each endpoint, define a `parser_profile` like `official_site.<key>.<page>_html`
3. Create an extractor in `apps/parser/adapters/{family}/<key>_<page>_extractor.py` subclassing `OfficialSiteFragmentExtractor` (or aggregator/ranking equivalent)
4. Register the extractor in `apps/parser/adapters/{family}/adapter.py` — both in `_build_extractors()` and `supported_parser_profiles`
5. Add a unit test in `tests/unit/adapters/`
6. Run `python -m scripts.source_bootstrap` (or `make seed`) — it's idempotent

### Add a new field to UniversityCard

1. Update [`libs/domain/university/models.py`](libs/domain/university/models.py) (Pydantic) and the JSON Schema in [`schemas/canonical/university_card.schema.json`](schemas/canonical/university_card.schema.json)
2. Update extractor to emit a fragment with `field_name="<your_field>"`
3. Update the projection logic in `apps/normalizer/app/projection/` to write the field into the card
4. Update the contract test in `tests/contract/test_delivery_projection_schema.py`

### Add a DB migration

1. Create `migrations/alembic/<YYYYMMDD>_<NNNN>_<slug>.py` with `revision`, `down_revision`, `upgrade()`, `downgrade()`
2. Reference the previous head as `down_revision`
3. Run `make migrate` (alias for `alembic upgrade head`)
4. **Don't edit a migration after it's been merged** — write a new one

### Debug a stuck crawl

1. Check RabbitMQ management UI at http://localhost:15672 (aggregator/aggregator) — look at queue depths and DLX queues
2. Check pipeline state: `SELECT * FROM ops.pipeline_run ORDER BY started_at DESC LIMIT 20;`
3. Check raw artifacts in MinIO console at http://localhost:9001 (aggregator/aggregator-secret)
4. Tail worker logs: `docker compose logs -f parser-worker`

## Coding conventions

- **Python 3.12+, async by default** for I/O — never block the event loop
- **Pydantic v2** for all models — use `model_validate`, `model_dump`, `Field(...)`. No `dict[str, Any]` for fields you can model
- **Type hints required** — `ruff` enforces. Run `make lint` before committing
- **No `print()` for diagnostics** — use the standard `logging` module
- **No new files unless necessary** — prefer extending existing patterns
- **Don't fragment extractors per page** — one extractor class per `parser_profile`
- **Don't write directly to `delivery.university_card`** — only the normalizer's projection layer does that
- **Don't bypass adapter base class methods** — `fetch → store_raw → extract → map_to_intermediate` is the contract
- **All event payloads are immutable** — use `Field(frozen=True)` on dataclasses or `model_config = ConfigDict(frozen=True)` on Pydantic models
- **Migrations are append-only** — never edit a merged migration

## Domain glossary

See [`docs/GLOSSARY.md`](docs/GLOSSARY.md) for the full list. Key terms:

- **Claim** — an extracted assertion about a field, with source attribution and confidence. Many claims may exist for the same field.
- **Resolved fact** — the winner of conflict resolution among claims, chosen by a *resolution policy* (majority, trust_tier, timestamp).
- **Fragment** — a single extracted value from one raw artifact (the "atom" of parsing).
- **Parser profile** — a string like `official_site.kubsu.abiturient_html` that selects which extractor handles a given URL.
- **Source key** — a stable identifier like `kubsu-official` for a logical data source.
- **Trust tier** — `authoritative` (official site) > `trusted` (reputable aggregator) > `auxiliary` > `experimental`.

## Don't do this

- **Don't add fields directly to `university_card.card_json`** without an extractor + claim → the data has no provenance
- **Don't call external APIs synchronously** in request handlers — push to a worker
- **Don't share secrets across env files** — `infra/env/local/app.env` is dev-only; production secrets must come from a real secrets manager
- **Don't disable the rate limiter** in extractors — we will get IP-banned during a demo
- **Don't use `service_started` as a docker-compose dependency** — use `service_healthy` so workers don't race startup
- **Don't bypass the admin API auth middleware** — even for "internal" tools

## See also

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — high-level architecture
- [`docs/PATTERNS.md`](docs/PATTERNS.md) — recurring code patterns
- [`docs/GLOSSARY.md`](docs/GLOSSARY.md) — domain terminology
- [`docs/adr/`](docs/adr/) — architecture decision records
- [`docs/roadmap.md`](docs/roadmap.md) — what's planned
- [`docs/deep-research-report.md`](docs/deep-research-report.md) — original architectural research