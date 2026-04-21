import asyncio
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from apps.parser.adapters.official_sites import OfficialSiteAdapter, OfficialSiteHtmlExtractor
from libs.source_sdk import FetchContext, FetchedArtifact, ParserExecutionStatus

FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "parser_ingestion"


class FakeFetcher:
    def __init__(self, artifact: FetchedArtifact) -> None:
        self.artifact = artifact
        self.calls: list[FetchContext] = []

    async def fetch(self, context: FetchContext) -> FetchedArtifact:
        self.calls.append(context)
        return self.artifact


class FakeRawStore:
    def __init__(self) -> None:
        self.calls: list[tuple[FetchContext, FetchedArtifact]] = []

    async def store_raw(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> FetchedArtifact:
        self.calls.append((context, artifact))
        return artifact.model_copy(
            update={
                "storage_bucket": "raw-html",
                "storage_object_key": f"{context.source_key}/{artifact.sha256}.html",
            }
        )


def build_context() -> FetchContext:
    return FetchContext(
        crawl_run_id=uuid4(),
        source_key="msu-official",
        endpoint_url="https://example.edu/admissions",
        parser_profile="official_site.default",
    )


def build_artifact(payload: bytes | None = None) -> FetchedArtifact:
    content = payload or (FIXTURE_ROOT / "official_site_admissions.html").read_bytes()
    return FetchedArtifact(
        raw_artifact_id=uuid4(),
        crawl_run_id=uuid4(),
        source_key="msu-official",
        source_url="https://example.edu/admissions",
        final_url="https://example.edu/admissions",
        http_status=200,
        content_type="text/html; charset=utf-8",
        content_length=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        fetched_at=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
        render_mode="http",
        content=content,
    )


def test_official_site_html_extractor_reads_canonical_fields_from_fixture() -> None:
    context = build_context()
    artifact = build_artifact()

    fragments = OfficialSiteHtmlExtractor().extract(context=context, artifact=artifact)
    by_field = {fragment.field_name: fragment for fragment in fragments}

    assert by_field["canonical_name"].value == "Example University"
    assert by_field["canonical_name"].locator == "h1"
    assert by_field["location.city"].value == "Moscow"
    assert by_field["contacts.website"].value == "https://example.edu"
    assert by_field["contacts.emails"].value == ["admissions@example.edu"]
    assert "contacts.phones" not in by_field
    assert all(fragment.raw_artifact_id == artifact.raw_artifact_id for fragment in fragments)
    assert all(fragment.source_key == "msu-official" for fragment in fragments)


def test_official_site_html_extractor_handles_address_and_phone_hints() -> None:
    payload = b"""
    <html>
      <body>
        <h1 class="university-name">Example Technical University</h1>
        <p class="address">1 University Ave</p>
        <section class="contacts">
          <a href="tel:+7 495 123-45-67">Call</a>
          <a href="mailto:info@example.edu">Email</a>
        </section>
      </body>
    </html>
    """
    context = build_context()
    artifact = build_artifact(payload)

    fragments = OfficialSiteHtmlExtractor().extract(context=context, artifact=artifact)
    by_field = {fragment.field_name: fragment for fragment in fragments}

    assert by_field["canonical_name"].value == "Example Technical University"
    assert by_field["location.address"].value == "1 University Ave"
    assert by_field["contacts.emails"].value == ["info@example.edu"]
    assert by_field["contacts.phones"].value == ["+7 495 123-45-67"]


def test_official_site_adapter_maps_fragments_to_intermediate_claims() -> None:
    context = build_context()
    artifact = build_artifact()
    adapter = OfficialSiteAdapter(fetcher=FakeFetcher(artifact))
    fragments = asyncio.run(adapter.extract(context, artifact))

    records = asyncio.run(adapter.map_to_intermediate(context, artifact, fragments))

    assert adapter.can_handle(context) is True
    aggregator_context = context.model_copy(update={"parser_profile": "aggregator.default"})
    assert adapter.can_handle(aggregator_context) is False
    assert len(records) == 1
    record = records[0]
    assert record.source_key == "msu-official"
    assert record.entity_type == "university"
    assert record.entity_hint == "Example University"
    assert record.metadata["adapter_key"] == "official_sites:0.1.0"
    claims_by_field = {claim["field_name"]: claim for claim in record.claims}
    assert claims_by_field["canonical_name"]["value"] == "Example University"
    assert claims_by_field["canonical_name"]["value_type"] == "str"
    assert claims_by_field["canonical_name"]["parser_version"] == "0.1.0"
    assert claims_by_field["contacts.emails"]["value"] == ["admissions@example.edu"]
    assert claims_by_field["contacts.emails"]["value_type"] == "list"
    assert claims_by_field["location.city"]["raw_artifact_id"] == str(artifact.raw_artifact_id)


def test_official_site_adapter_executes_fetch_store_extract_and_map() -> None:
    context = build_context()
    artifact = build_artifact()
    fetcher = FakeFetcher(artifact)
    raw_store = FakeRawStore()
    adapter = OfficialSiteAdapter(fetcher=fetcher, raw_store=raw_store)

    result = asyncio.run(adapter.execute(context))

    assert fetcher.calls == [context]
    assert raw_store.calls == [(context, artifact)]
    assert result.status == ParserExecutionStatus.SUCCEEDED
    assert result.adapter_key == "official_sites:0.1.0"
    assert result.artifact is not None
    assert result.artifact.storage_bucket == "raw-html"
    assert result.extracted_fragments == 4
    assert result.intermediate_records[0].entity_hint == "Example University"
