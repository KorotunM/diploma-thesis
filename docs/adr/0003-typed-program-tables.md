# ADR 0003: Typed program/admission tables in `core` schema

- Status: accepted
- Date: 2026-05-04

## Context

The initial schema (migration `0001`) modeled programs and admissions as untyped JSON inside `delivery.university_card.card_json`. This was a pragmatic MVP choice but created problems as we add more sources:

- Can't query "all bachelor programs in Moscow under 200K rub/year" without scanning JSON
- Can't enforce uniqueness on `(university_id, program_code, level, form)`
- Can't join programs to faculties or admissions cleanly
- Aggregating across years for trend analysis requires JSON path expressions

The university domain has a clear, stable structure (programs have codes, admissions have years, exams are a fixed enum). Modeling it as JSON loses that structure.

## Decision

Add migration `0002_university_domain_tables.py` introducing typed tables in the `core` schema:

- `core.faculty` — academic units within a university
- `core.program` — specialties: code, name, level, form, duration, language
- `core.admission_year` — yearly admission stats: budget/paid seats, min score, tuition cost
- `core.admission_exam` — required EGE subjects per program-year
- `core.legal_info` — INN, OGRN, accreditation, founded year, institution type
- `core.location_detail` — region, full address, lat/lon (extends `core.university` city/country)
- `core.statistics_yearly` — student count, faculty staff count by year

The `delivery.university_card.card_json` continues to embed denormalized program/admission data for the public API (so frontend doesn't have to do joins). The normalizer's projection layer is responsible for keeping both representations consistent.

## Consequences

- Positive: structured queries become possible (search by program, admission filters, ranking changes over time).
- Positive: data integrity — uniqueness on program code, foreign keys to university and faculty.
- Positive: future LLM features (program embeddings) get a stable target for joins.
- Negative: normalizer projection gets more complex — must write to both typed tables and JSON.
- Negative: backward compatibility — older cards stored only as JSON need backfilling. Acceptable: re-projecting from claims is cheap.

## Alternatives considered

**Keep everything in card_json** — simpler but blocks query flexibility. Rejected.

**Replace card_json entirely with typed tables** — would force the frontend to do joins. Rejected: API ergonomics matter.

**Materialized views** — derive typed tables from JSON. Rejected: indirection without solving the integrity problem.

## References

- `migrations/alembic/20260504_0002_university_domain_tables.py`
- `docs/GLOSSARY.md` — domain terms used in the new tables
