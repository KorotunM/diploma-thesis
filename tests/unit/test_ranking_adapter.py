import asyncio
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from apps.parser.adapters.rankings import (
    RankingAdapter,
    RankingPayloadExtractor,
    TabiturientGlobalRatingHtmlExtractor,
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
        source_key="qs-world-ranking",
        endpoint_url="https://rankings.example.com/universities/example-university",
        parser_profile="ranking.default",
    )


def build_artifact(payload: bytes | None = None) -> FetchedArtifact:
    content = payload or (FIXTURE_ROOT / "ranking_provider_university_profile.json").read_bytes()
    return FetchedArtifact(
        raw_artifact_id=uuid4(),
        crawl_run_id=uuid4(),
        source_key="qs-world-ranking",
        source_url="https://rankings.example.com/universities/example-university",
        final_url="https://rankings.example.com/universities/example-university",
        http_status=200,
        content_type="application/json",
        content_length=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        fetched_at=datetime(2026, 4, 27, 9, 0, tzinfo=UTC),
        render_mode="http",
        content=content,
    )


def build_tabiturient_context() -> FetchContext:
    return FetchContext(
        crawl_run_id=uuid4(),
        source_key="tabiturient-globalrating",
        endpoint_url="https://tabiturient.ru/globalrating/",
        parser_profile="ranking.tabiturient.globalrating_html",
    )


def build_tabiturient_artifact(payload: bytes | None = None) -> FetchedArtifact:
    content = payload or (FIXTURE_ROOT / "tabiturient_globalrating_page.html").read_bytes()
    return FetchedArtifact(
        raw_artifact_id=uuid4(),
        crawl_run_id=uuid4(),
        source_key="tabiturient-globalrating",
        source_url="https://tabiturient.ru/globalrating/",
        final_url="https://tabiturient.ru/globalrating/",
        http_status=200,
        content_type="text/html; charset=utf-8",
        content_length=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        fetched_at=datetime(2026, 4, 30, 11, 0, tzinfo=UTC),
        render_mode="http",
        content=content,
    )


def test_ranking_payload_extractor_reads_provider_and_rating_fragments() -> None:
    context = build_context()
    artifact = build_artifact()

    fragments = RankingPayloadExtractor().extract(context=context, artifact=artifact)
    by_field = {fragment.field_name: fragment for fragment in fragments}

    assert by_field["canonical_name"].value == "Example University"
    assert by_field["contacts.website"].value == "https://example.edu"
    assert by_field["location.country_code"].value == "RU"
    assert by_field["ratings.provider"].value == "QS World University Rankings"
    assert by_field["ratings.year"].value == 2026
    assert by_field["ratings.metric"].value == "world_overall"
    assert by_field["ratings.value"].value == "151"
    assert by_field["ratings.provider"].metadata["provider_key"] == "qs-world"
    assert by_field["ratings.provider"].metadata["rating_item_key"] == (
        "qs-world:2026:world_overall:example-university"
    )
    assert by_field["ratings.value"].metadata["rank_display"] == "#151"
    assert by_field["ratings.value"].metadata["scale"] == "global"
    assert by_field["ratings.value"].metadata["adapter_family"] == "rankings"
    assert by_field["ratings.value"].metadata["record_group_key"] == (
        "qs-world:2026:world_overall:example-university"
    )


def test_tabiturient_globalrating_extractor_reads_structured_ranking_rows() -> None:
    context = build_tabiturient_context()
    artifact = build_tabiturient_artifact()

    fragments = TabiturientGlobalRatingHtmlExtractor().extract(
        context=context,
        artifact=artifact,
    )

    grouped: dict[str, dict[str, object]] = {}
    for fragment in fragments:
        group_key = fragment.metadata["record_group_key"]
        grouped.setdefault(group_key, {})[fragment.field_name] = fragment

    assert len(grouped) == 3

    mfti = grouped["tabiturient-globalrating:2026:russia_overall:7"]
    assert mfti["canonical_name"].value == (
        "Московский Физико-Технический Институт (государственный университет)"
    )
    assert mfti["aliases"].value == ["МФТИ"]
    assert mfti["ratings.provider"].value == "Tabiturient"
    assert mfti["ratings.year"].value == 2026
    assert mfti["ratings.metric"].value == "russia_overall"
    assert mfti["ratings.rank"].value == 1
    assert mfti["ratings.value"].value == "158.75"
    assert mfti["ratings.category"].value == "A+"
    assert mfti["ratings.change.direction"].value == "up"
    assert mfti["ratings.change.delta"].value == 1
    assert mfti["ratings.value"].metadata["external_id"] == "7"
    assert mfti["ratings.value"].metadata["rank_display"] == "#1"
    assert mfti["ratings.value"].metadata["scale"] == "russia"

    hse = grouped["tabiturient-globalrating:2026:russia_overall:2"]
    assert hse["aliases"].value == ["ВШЭ"]
    assert hse["ratings.rank"].value == 2
    assert hse["ratings.change.direction"].value == "down"
    assert hse["ratings.change.delta"].value == 1

    mgimo = grouped["tabiturient-globalrating:2026:russia_overall:4"]
    assert mgimo["aliases"].value == ["МГИМО"]
    assert mgimo["ratings.rank"].value == 3
    assert mgimo["ratings.change.direction"].value == "same"
    assert mgimo["ratings.change.delta"].value == 0


def test_ranking_adapter_maps_fragments_to_rating_intermediate_record() -> None:
    context = build_context()
    artifact = build_artifact()
    adapter = RankingAdapter(fetcher=FakeFetcher(artifact))
    fragments = asyncio.run(adapter.extract(context, artifact))

    records = asyncio.run(adapter.map_to_intermediate(context, artifact, fragments))

    assert adapter.can_handle(context) is True
    aggregator_context = context.model_copy(update={"parser_profile": "aggregator.default"})
    assert adapter.can_handle(aggregator_context) is False
    assert len(records) == 1
    record = records[0]
    assert record.source_key == "qs-world-ranking"
    assert record.entity_type == "university"
    assert record.entity_hint == "Example University"
    assert record.metadata["adapter_key"] == "rankings:0.1.0"
    assert record.metadata["source_kind"] == "ranking"
    assert record.metadata["provider_name"] == "QS World University Rankings"
    claims_by_field = {claim["field_name"]: claim for claim in record.claims}
    assert claims_by_field["ratings.year"]["value"] == 2026
    assert claims_by_field["ratings.year"]["value_type"] == "int"
    assert claims_by_field["ratings.value"]["metadata"]["rating_item_key"] == (
        "qs-world:2026:world_overall:example-university"
    )
    assert claims_by_field["contacts.website"]["raw_artifact_id"] == str(
        artifact.raw_artifact_id
    )


def test_ranking_adapter_groups_tabiturient_page_rows_into_records() -> None:
    context = build_tabiturient_context()
    artifact = build_tabiturient_artifact()
    adapter = RankingAdapter(fetcher=FakeFetcher(artifact))
    fragments = asyncio.run(adapter.extract(context, artifact))

    records = asyncio.run(adapter.map_to_intermediate(context, artifact, fragments))

    assert adapter.can_handle(context) is True
    assert len(records) == 3

    records_by_group = {
        record.metadata["record_group_key"]: record
        for record in records
    }

    mfti = records_by_group["tabiturient-globalrating:2026:russia_overall:7"]
    assert mfti.entity_hint == (
        "Московский Физико-Технический Институт (государственный университет)"
    )
    mfti_claims = {claim["field_name"]: claim for claim in mfti.claims}
    assert mfti_claims["ratings.provider"]["value"] == "Tabiturient"
    assert mfti_claims["ratings.rank"]["value"] == 1
    assert mfti_claims["ratings.value"]["value"] == "158.75"
    assert mfti_claims["ratings.category"]["value"] == "A+"
    assert mfti_claims["ratings.change.direction"]["value"] == "up"
    assert mfti_claims["ratings.change.delta"]["value"] == 1
    assert mfti_claims["ratings.change.delta"]["value_type"] == "int"
    assert mfti_claims["ratings.rank"]["metadata"]["rating_item_key"] == (
        "tabiturient-globalrating:2026:russia_overall:7"
    )

    mgimo = records_by_group["tabiturient-globalrating:2026:russia_overall:4"]
    mgimo_claims = {claim["field_name"]: claim for claim in mgimo.claims}
    assert mgimo_claims["ratings.change.direction"]["value"] == "same"
    assert mgimo_claims["ratings.change.delta"]["value"] == 0


def test_ranking_adapter_executes_fetch_store_extract_and_map() -> None:
    context = build_context()
    artifact = build_artifact()
    fetcher = FakeFetcher(artifact)
    raw_store = FakeRawStore()
    adapter = RankingAdapter(fetcher=fetcher, raw_store=raw_store)

    result = asyncio.run(adapter.execute(context))

    assert fetcher.calls == [context]
    assert raw_store.calls == [(context, artifact)]
    assert result.status == ParserExecutionStatus.SUCCEEDED
    assert result.adapter_key == "rankings:0.1.0"
    assert result.artifact is not None
    assert result.artifact.storage_bucket == "raw-json"
    assert result.extracted_fragments == 7
    assert result.intermediate_records[0].entity_hint == "Example University"


def test_ranking_adapter_executes_tabiturient_globalrating_pipeline() -> None:
    context = build_tabiturient_context()
    artifact = build_tabiturient_artifact()
    fetcher = FakeFetcher(artifact)
    raw_store = FakeRawStore()
    adapter = RankingAdapter(fetcher=fetcher, raw_store=raw_store)

    result = asyncio.run(adapter.execute(context))

    assert fetcher.calls == [context]
    assert raw_store.calls == [(context, artifact)]
    assert result.status == ParserExecutionStatus.SUCCEEDED
    assert result.adapter_key == "rankings:0.1.0"
    assert result.artifact is not None
    assert result.artifact.storage_bucket == "raw-json"
    assert result.extracted_fragments == 30
    assert len(result.intermediate_records) == 3
    group_keys = {
        record.metadata["record_group_key"]
        for record in result.intermediate_records
    }
    assert group_keys == {
        "tabiturient-globalrating:2026:russia_overall:7",
        "tabiturient-globalrating:2026:russia_overall:2",
        "tabiturient-globalrating:2026:russia_overall:4",
    }
