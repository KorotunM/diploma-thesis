import asyncio
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from apps.parser.adapters.aggregators import (
    AggregatorAdapter,
    AggregatorPayloadExtractor,
    TabiturientUniversityHtmlExtractor,
)
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
                "storage_bucket": "raw-json",
                "storage_object_key": f"{context.source_key}/{artifact.sha256}.json",
            }
        )


def build_context() -> FetchContext:
    return FetchContext(
        crawl_run_id=uuid4(),
        source_key="study-aggregator",
        endpoint_url="https://aggregator.example/universities/example-university",
        parser_profile="aggregator.default",
    )


def build_tabiturient_context() -> FetchContext:
    return FetchContext(
        crawl_run_id=uuid4(),
        source_key="tabiturient-aggregator",
        endpoint_url="https://tabiturient.ru/vuzu/altgaki",
        parser_profile="aggregator.tabiturient.university_html",
    )


def build_artifact(payload: bytes | None = None) -> FetchedArtifact:
    content = payload or (FIXTURE_ROOT / "aggregator_university_profile.json").read_bytes()
    return FetchedArtifact(
        raw_artifact_id=uuid4(),
        crawl_run_id=uuid4(),
        source_key="study-aggregator",
        source_url="https://aggregator.example/universities/example-university",
        final_url="https://aggregator.example/universities/example-university",
        http_status=200,
        content_type="application/json",
        content_length=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        fetched_at=datetime(2026, 4, 25, 9, 0, tzinfo=UTC),
        render_mode="http",
        content=content,
    )


def build_tabiturient_artifact(payload: bytes | None = None) -> FetchedArtifact:
    content = payload or (FIXTURE_ROOT / "tabiturient_university_primary.html").read_bytes()
    return FetchedArtifact(
        raw_artifact_id=uuid4(),
        crawl_run_id=uuid4(),
        source_key="tabiturient-aggregator",
        source_url="https://tabiturient.ru/vuzu/altgaki",
        final_url="https://tabiturient.ru/vuzu/altgaki",
        http_status=200,
        content_type="text/html; charset=utf-8",
        content_length=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        fetched_at=datetime(2026, 4, 30, 12, 0, tzinfo=UTC),
        render_mode="http",
        content=content,
    )


def test_aggregator_payload_extractor_reads_secondary_university_fields() -> None:
    context = build_context()
    artifact = build_artifact()

    fragments = AggregatorPayloadExtractor().extract(context=context, artifact=artifact)
    by_field = {fragment.field_name: fragment for fragment in fragments}

    assert by_field["canonical_name"].value == "Example University"
    assert by_field["aliases"].value == ["EU", "Example U"]
    assert by_field["location.city"].value == "Moscow"
    assert by_field["location.country_code"].value == "RU"
    assert by_field["contacts.website"].value == "https://example.edu"
    assert by_field["contacts.emails"].value == ["admissions@example.edu"]
    assert by_field["contacts.phones"].value == ["+7 495 123-45-67"]
    assert by_field["canonical_name"].metadata["adapter_family"] == "aggregators"
    assert by_field["canonical_name"].metadata["provider_name"] == "Study Aggregator"
    assert by_field["canonical_name"].metadata["source_field"] == "aggregator.display_name"
    assert by_field["canonical_name"].metadata["external_id"] == "agg-42"


def test_tabiturient_university_html_extractor_reads_identity_and_contacts_from_fixture() -> None:
    context = build_tabiturient_context()
    artifact = build_tabiturient_artifact()

    fragments = TabiturientUniversityHtmlExtractor().extract(
        context=context,
        artifact=artifact,
    )
    by_field = {fragment.field_name: fragment for fragment in fragments}

    assert by_field["canonical_name"].value == "Алтайский государственный институт культуры"
    assert by_field["aliases"].value == ["АГИК"]
    assert by_field["contacts.website"].value == "https://agik22.ru"
    assert by_field["canonical_name"].metadata["provider_name"] == "Tabiturient"
    assert by_field["canonical_name"].metadata["source_field"] == "tabiturient.itemprop.name"
    assert by_field["canonical_name"].metadata["external_id"] == "altgaki"
    assert by_field["contacts.website"].locator == 'a[itemprop="sameAs"]'


def test_aggregator_adapter_maps_fragments_to_secondary_intermediate_claims() -> None:
    context = build_context()
    artifact = build_artifact()
    adapter = AggregatorAdapter(fetcher=FakeFetcher(artifact))
    fragments = asyncio.run(adapter.extract(context, artifact))

    records = asyncio.run(adapter.map_to_intermediate(context, artifact, fragments))

    assert adapter.can_handle(context) is True
    official_context = context.model_copy(update={"parser_profile": "official_site.default"})
    assert adapter.can_handle(official_context) is False
    assert len(records) == 1
    record = records[0]
    assert record.source_key == "study-aggregator"
    assert record.entity_type == "university"
    assert record.entity_hint == "Example University"
    assert record.metadata["adapter_key"] == "aggregators:0.1.0"
    assert record.metadata["source_kind"] == "secondary"
    claims_by_field = {claim["field_name"]: claim for claim in record.claims}
    assert claims_by_field["canonical_name"]["value"] == "Example University"
    assert claims_by_field["canonical_name"]["value_type"] == "str"
    assert claims_by_field["contacts.emails"]["value_type"] == "list"
    assert claims_by_field["contacts.website"]["raw_artifact_id"] == str(
        artifact.raw_artifact_id
    )
    assert (
        claims_by_field["canonical_name"]["metadata"]["source_field"]
        == "aggregator.display_name"
    )


def test_aggregator_adapter_maps_tabiturient_primary_page_fragments_to_intermediate_claims(
) -> None:
    context = build_tabiturient_context()
    artifact = build_tabiturient_artifact()
    adapter = AggregatorAdapter(fetcher=FakeFetcher(artifact))
    fragments = asyncio.run(adapter.extract(context, artifact))

    records = asyncio.run(adapter.map_to_intermediate(context, artifact, fragments))

    assert adapter.can_handle(context) is True
    assert len(records) == 1
    record = records[0]
    assert record.source_key == "tabiturient-aggregator"
    assert record.entity_type == "university"
    assert record.entity_hint == "Алтайский государственный институт культуры"
    claims_by_field = {claim["field_name"]: claim for claim in record.claims}
    assert claims_by_field["aliases"]["value"] == ["АГИК"]
    assert claims_by_field["contacts.website"]["value"] == "https://agik22.ru"
    assert claims_by_field["contacts.website"]["metadata"]["provider_name"] == "Tabiturient"


def test_aggregator_adapter_executes_fetch_store_extract_and_map() -> None:
    context = build_context()
    artifact = build_artifact()
    fetcher = FakeFetcher(artifact)
    raw_store = FakeRawStore()
    adapter = AggregatorAdapter(fetcher=fetcher, raw_store=raw_store)

    result = asyncio.run(adapter.execute(context))

    assert fetcher.calls == [context]
    assert raw_store.calls == [(context, artifact)]
    assert result.status == ParserExecutionStatus.SUCCEEDED
    assert result.adapter_key == "aggregators:0.1.0"
    assert result.artifact is not None
    assert result.artifact.storage_bucket == "raw-json"
    assert result.extracted_fragments == 7
    assert result.intermediate_records[0].entity_hint == "Example University"


def test_aggregator_adapter_executes_tabiturient_html_extract_and_map() -> None:
    context = build_tabiturient_context()
    artifact = build_tabiturient_artifact()
    fetcher = FakeFetcher(artifact)
    raw_store = FakeRawStore()
    adapter = AggregatorAdapter(fetcher=fetcher, raw_store=raw_store)

    result = asyncio.run(adapter.execute(context))

    assert fetcher.calls == [context]
    assert raw_store.calls == [(context, artifact)]
    assert result.status == ParserExecutionStatus.SUCCEEDED
    assert result.adapter_key == "aggregators:0.1.0"
    assert result.artifact is not None
    assert result.artifact.storage_bucket == "raw-json"
    assert result.extracted_fragments == 3
    assert (
        result.intermediate_records[0].entity_hint
        == "Алтайский государственный институт культуры"
    )
