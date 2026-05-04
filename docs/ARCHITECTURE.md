# Architecture (1-page reference)

## High-level data flow

```
Source registry  →  Scheduler  →  Parser  →  Normalizer  →  Backend API  →  Frontend
   (Postgres)    (FastAPI)     (FastAPI)    (FastAPI)      (FastAPI)       (nginx)
                     │             │            │
                     │             ▼            ▼
                     │          MinIO       Postgres
                     │       (raw bytes)  (claims, facts, cards)
                     ▼
              RabbitMQ (parser.jobs / normalize.jobs / delivery.events)
```

## Services

| Service | Port | Role | Data it owns |
|---------|------|------|--------------|
| `scheduler` | 8001 | Source registry CRUD, plans crawl runs, publishes `crawl.request.v1` | `ingestion.source`, `ingestion.source_endpoint`, `ops.pipeline_run` |
| `parser` | 8002 | Consumes `crawl.request`, fetches HTML/PDF, extracts fragments, emits `parse.completed.v1` | `ingestion.raw_artifact`, `parsing.parsed_document`, `parsing.extracted_fragment` |
| `normalizer` | 8003 | Consumes `parse.completed`, builds claims, resolves conflicts, projects cards, emits `card.updated` | `normalize.claim`, `normalize.claim_evidence`, `core.resolved_fact`, `core.university`, `delivery.university_card` |
| `backend` | 8004 | Read-only public API: search, university card, provenance trace | reads from `delivery.*` |
| `frontend` | 5173 (nginx) | React SPA — search, card, evidence drawer, monitoring | none (consumes backend API) |

Each FastAPI service has a sibling **worker process** (`apps/<svc>/app/worker.py`) that consumes RabbitMQ messages.

## Database schemas

```
ops/         — pipeline_run                     (orchestration state)
ingestion/   — source, source_endpoint,         (what we crawl)
               raw_artifact                     (raw bytes + sha256 + storage pointer)
parsing/     — parsed_document,                 (typed extraction output)
               extracted_fragment               (atomic value + locator + confidence)
normalize/   — claim, claim_evidence            (typed assertions with full evidence)
core/        — university, university_alias,    (entity identity)
               resolved_fact, card_version,     (the resolved truth)
               faculty, program, admission_year,(typed domain — added in 0002)
               admission_exam, legal_info,
               location_detail, statistics_yearly
delivery/    — university_card,                 (read models)
               university_search_doc            (full-text search)
```

## Event contracts (Pydantic, `libs/contracts/events/pipeline.py`)

All events have an `EventHeader { event_id, trace_id, producer, occurred_at, schema_version }` and a typed `payload`. Schema versions live in the event class name (`v1`).

- `CrawlRequestEvent` — published by scheduler, consumed by parser worker
- `ParseCompletedEvent` — published by parser, consumed by normalizer worker
- `NormalizeRequestEvent` — internal to normalizer (subdivides work)
- `CardUpdatedEvent` — published by normalizer, consumed by delivery / future analytics
- `ReviewRequiredEvent` — published when conflict resolution can't pick a winner

## RabbitMQ topology

```
parser.jobs       ─┬─ parser.high          (high-priority crawls)
                   └─ parser.bulk          (scheduled bulk crawls)
       ↓ on error
parser.retry      ─→ parser.high.retry / parser.bulk.retry  (TTL 30s)
       ↓ on persistent failure
parser.dead       ─→ parser.high.dead / parser.bulk.dead    (DLX, manual recovery)

(same shape for normalize.jobs and delivery.events)
```

All queues are **quorum** queues for HA. Workers ack only after DB commit.

## Trust model

| Tier | Sources | Resolution policy weight |
|------|---------|-------------------------|
| `authoritative` | Official university sites | Highest — wins on tie |
| `trusted` | Major aggregators (Vuzopedia, Tabiturient) | Wins over auxiliary |
| `auxiliary` | Smaller portals, social | Tiebreaker only |
| `experimental` | Unverified sources | Never wins on its own |

## Adapter contract

```python
class SourceAdapter(ABC):
    source_key: str
    adapter_version: str
    supported_parser_profiles: tuple[str, ...]

    @abstractmethod
    async def fetch(ctx) -> FetchedArtifact: ...
    @abstractmethod
    async def extract(ctx, artifact) -> Sequence[ExtractedFragment]: ...
    @abstractmethod
    async def map_to_intermediate(ctx, artifact, fragments) -> Sequence[IntermediateRecord]: ...

    # default: store raw artifact in MinIO via injected RawArtifactStore
    async def store_raw(ctx, artifact) -> FetchedArtifact: ...
```

The pipeline orchestrator in `libs/source_sdk/base_adapter.py:execute()` runs the four steps in order and emits `parse.completed.v1`.

## Observability

- **Prometheus** scrapes `/metrics` on each service (every 15s)
- **Recording rules** at `infra/prometheus/rules/pipeline-health.yml`:
  - `pipeline:crawl_jobs_published:rate5m`
  - `pipeline:parse_runs:rate5m`
  - `pipeline:card_builds:rate5m`
  - p95 latency histograms for parse/normalize/card_build
- **Grafana** dashboard: `pipeline-health-overview.json`
- **Logs** are stdout/stderr, captured by `docker compose logs`

## Authentication

- **Public API** (`backend:/api/v1/*`) — unauthenticated, rate-limited at nginx
- **Admin API** (`scheduler:/admin/v1/*`) — `Authorization: Bearer <PLATFORM_ADMIN_API_KEY>` required
- **Health endpoints** (`/healthz`, `/readyz`, `/metrics`) — always unauthenticated
- **Worker → DB / RabbitMQ / MinIO** — internal, no auth (network-isolated in compose)

For production, replace the static API key with a real OIDC issuer + JWT verification.

## Critical invariants (do not violate)

1. **Every fact has provenance.** A `delivery.university_card.card_json` field must trace to a `normalize.claim` row, which must trace to a `parsing.extracted_fragment`, which must reference a `ingestion.raw_artifact` with a sha256 hash.
2. **Cards are computed, never edited.** Only the normalizer writes to `delivery.*`.
3. **Raw artifacts are immutable.** `ingestion.raw_artifact` is append-only (deduplicated by sha256).
4. **Migrations are append-only.** Never edit a merged migration; write a new one.
5. **Events are versioned.** Breaking changes require a new schema_version (e.g., `v2`).

## See also

- [PATTERNS.md](PATTERNS.md) — recurring code patterns
- [GLOSSARY.md](GLOSSARY.md) — domain terminology
- [adr/](adr/) — architecture decision records
- [deep-research-report.md](deep-research-report.md) — original research
