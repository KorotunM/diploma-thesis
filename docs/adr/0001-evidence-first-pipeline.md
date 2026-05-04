# ADR 0001: Evidence-first pipeline

- Status: accepted
- Date: 2026-04-15

## Context

The thesis goal is a *trustworthy* university aggregator. Russian university data is contradictory across sources — official sites, aggregators, and rankings often disagree on basic facts (city, programs, scores). A naive design that overwrites fields would lose information and prevent auditing.

We need: (a) every visible fact to be traceable to a source, (b) the ability to replay extraction with new logic, (c) safe coexistence of conflicting claims, (d) human-reviewable conflict resolution.

## Decision

Adopt the **evidence-first pipeline**:

```
raw_artifact → parsed_document → extracted_fragment → claim → resolved_fact → card_field
```

Every step is persisted to its own table; nothing is overwritten. Specifically:

- `ingestion.raw_artifact` stores the bytes fetched from a URL, indexed by sha256, immutable.
- `parsing.extracted_fragment` stores atomic values extracted from a raw artifact, with a locator string and confidence.
- `normalize.claim` stores typed assertions, one per source per field, with full back-references.
- `core.resolved_fact` stores the winner per (university, field), chosen by an explicit `resolution_policy`.
- `delivery.university_card` is a *projection* — recomputable from claims at any time.

## Consequences

- Positive: full provenance API, replayable extraction, safe multi-source merging, no data loss on conflicts.
- Positive: enables versioning of parsers and resolution policies without losing history.
- Negative: ~5x more storage than a flat schema. Acceptable at MVP scale.
- Negative: every domain change requires updating the projection logic, not just the read model.

## Alternatives considered

**Flat overwriting schema** — one row per university, fields overwritten by latest source. Rejected: loses provenance, makes conflicts invisible.

**Event-sourced log** — store only events, derive everything. Rejected: too much engineering overhead for an MVP; the projection layer is enough.

**Document store (MongoDB)** — store raw blobs, query via aggregation. Rejected: claim-level queries (provenance API) become awkward; we need relational guarantees for resolution.

## References

- `docs/deep-research-report.md` — original research
- `migrations/alembic/20260503_0001_initial_university_aggregator_schema.py`
