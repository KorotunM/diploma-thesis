# Code patterns

Recurring patterns in this codebase. Match these when adding new code.

## Pydantic model (frozen, strict)

```python
from pydantic import BaseModel, ConfigDict, Field

class CrawlRequestPayload(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source_key: str = Field(..., min_length=1)
    endpoint_url: str = Field(..., min_length=1)
    parser_profile: str = Field(..., min_length=1)
    priority: Literal["high", "bulk"] = "bulk"
```

- `frozen=True` — immutability (event payloads must never mutate)
- `extra="forbid"` — strict validation, no silent extra keys
- `Field(..., min_length=1)` — explicit non-empty constraints

## FastAPI service skeleton

```python
# apps/<svc>/app/main.py
from libs.observability import create_service_app
from .routes import router

app = create_service_app(service_name="<svc>", description="...")
app.include_router(router)
```

`create_service_app` adds `/healthz`, `/readyz`, `/metrics` automatically.

## Worker loop

```python
# apps/<svc>/app/worker.py
from libs.storage.worker import run_resilient_worker_loop

async def main() -> None:
    settings = get_platform_settings("<svc>")
    await run_resilient_worker_loop(
        consumer_factory=lambda: build_<svc>_consumer(settings),
        on_message=process_one,
        initial_backoff_seconds=2.0,
        max_backoff_seconds=30.0,
    )

if __name__ == "__main__":
    asyncio.run(main())
```

Workers retry on transient errors (`OSError`, `OperationalError`) with exponential backoff. Permanent errors crash the process and let the supervisor (docker compose / k8s) restart.

## Adapter extractor

```python
class FooAbiturientHtmlExtractor(OfficialSiteFragmentExtractor):
    supported_parser_profiles = ("official_site.foo.abiturient_html",)

    def extract(self, *, context, artifact) -> list[ExtractedFragment]:
        content = artifact.content.decode("utf-8", errors="replace")
        fragments: list[ExtractedFragment] = []
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="canonical_name",
            value=self._extract_title(content),
            locator="title",
            confidence=0.95,
        )
        # ... more fragments
        return fragments
```

Always:
- Decode with `errors="replace"` (real-world HTML has bad bytes)
- Set explicit `confidence` per fragment
- Use `_append_fragment` helper — it skips empty values automatically
- Set `locator` to a CSS-like or regex-like string identifying *where* in the artifact

## Repository pattern

```python
# Repositories live in apps/<svc>/app/<domain>/repositories.py
class SourceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_key(self, source_key: str) -> SourceRecord | None: ...
    async def upsert(self, record: SourceRecord) -> SourceRecord: ...
```

Routes get repositories via FastAPI `Depends`, which gets the session from a request-scoped factory.

## Settings via pydantic-settings

```python
class FooSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FOO_", env_file=None)

    poll_interval_seconds: int = 30
    batch_limit: int = 100
```

Reads from environment variables with the `FOO_` prefix. Used by services to discover their config.

## Event publishing

```python
event = ParseCompletedEvent(
    header=EventHeader(
        event_id=uuid4(),
        trace_id=context.trace_id,
        producer="parser",
        occurred_at=datetime.now(UTC),
        schema_version="v1",
    ),
    payload=ParseCompletedPayload(...),
)
await publisher.publish(event, exchange="normalize.jobs", routing_key=context.priority)
```

Always include `trace_id` from upstream so the full pipeline can be correlated in logs/metrics.

## Test fixture pattern

```python
def test_extractor_pulls_canonical_name():
    artifact = FetchedArtifact(
        raw_artifact_id=uuid4(),
        source_key="foo-official",
        source_url="https://foo.example/",
        final_url="https://foo.example/",
        content=b"<html><title>Foo University</title></html>",
        content_type="text/html",
        http_status=200,
        sha256="...",
        headers={},
    )
    context = FetchContext(
        crawl_run_id=uuid4(),
        source_key="foo-official",
        endpoint_url="https://foo.example/",
        parser_profile="official_site.foo.home_html",
        # ...
    )
    fragments = FooHomeHtmlExtractor().extract(context=context, artifact=artifact)
    canonical = next(f for f in fragments if f.field_name == "canonical_name")
    assert canonical.value == "Foo University"
    assert canonical.confidence >= 0.9
```

Don't mock `httpx` or `MinIO` — give the extractor a literal `FetchedArtifact` with bytes.

## Confidence scoring (rule of thumb)

| Source | Confidence |
|--------|-----------|
| `<title>` tag on canonical site | 0.95–0.98 |
| `<h1>` or `og:site_name` | 0.88–0.94 |
| Structured table (e.g., admission programs PDF) | 0.95–1.00 |
| Free-text regex match | 0.70–0.85 |
| Inferred from URL or domain | 0.50–0.70 |

The normalizer uses these to weight conflicts.

## Logging

```python
import logging
logger = logging.getLogger(__name__)

logger.info("crawl_started", extra={"source_key": ..., "trace_id": ...})
logger.warning("parser_no_fragments", extra={"parser_profile": ..., "raw_artifact_id": ...})
logger.exception("normalize_failed", extra={"claim_id": ...})
```

Always pass `extra=` so structured-log shippers can index. No `print`.
