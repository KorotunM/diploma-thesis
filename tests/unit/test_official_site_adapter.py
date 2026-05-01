import asyncio
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from apps.parser.adapters.official_sites import (
    KubSUAbiturientHtmlExtractor,
    KubSUProgramsHtmlExtractor,
    OfficialSiteAdapter,
    OfficialSiteHtmlExtractor,
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


def build_kubsu_context() -> FetchContext:
    return FetchContext(
        crawl_run_id=uuid4(),
        source_key="kubsu-official",
        endpoint_url="https://www.kubsu.ru/ru/abiturient",
        parser_profile="official_site.kubsu.abiturient_html",
    )


def build_kubsu_programs_context() -> FetchContext:
    return FetchContext(
        crawl_run_id=uuid4(),
        source_key="kubsu-official",
        endpoint_url="https://www.kubsu.ru/ru/node/44875",
        parser_profile="official_site.kubsu.programs_html",
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


def build_kubsu_artifact(payload: bytes | None = None) -> FetchedArtifact:
    content = payload or (FIXTURE_ROOT / "kubsu_abiturient_page.html").read_bytes()
    return FetchedArtifact(
        raw_artifact_id=uuid4(),
        crawl_run_id=uuid4(),
        source_key="kubsu-official",
        source_url="https://www.kubsu.ru/ru/abiturient",
        final_url="https://www.kubsu.ru/ru/abiturient",
        http_status=200,
        content_type="text/html; charset=utf-8",
        content_length=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        fetched_at=datetime(2026, 5, 1, 10, 0, tzinfo=UTC),
        render_mode="http",
        content=content,
    )


def build_kubsu_programs_artifact(payload: bytes | None = None) -> FetchedArtifact:
    content = payload or (FIXTURE_ROOT / "kubsu_programs_page.html").read_bytes()
    return FetchedArtifact(
        raw_artifact_id=uuid4(),
        crawl_run_id=uuid4(),
        source_key="kubsu-official",
        source_url="https://www.kubsu.ru/ru/node/44875",
        final_url="https://www.kubsu.ru/ru/node/44875",
        http_status=200,
        content_type="text/html; charset=utf-8",
        content_length=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        fetched_at=datetime(2026, 5, 1, 11, 0, tzinfo=UTC),
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


def test_kubsu_abiturient_html_extractor_reads_canonical_and_admission_contacts() -> None:
    context = build_kubsu_context()
    artifact = build_kubsu_artifact()

    fragments = KubSUAbiturientHtmlExtractor().extract(
        context=context,
        artifact=artifact,
    )
    by_field = {fragment.field_name: fragment for fragment in fragments}

    assert by_field["canonical_name"].value == "Кубанский государственный университет"
    assert by_field["canonical_name"].locator == "title"
    assert by_field["contacts.website"].value == "https://www.kubsu.ru"
    assert by_field["contacts.website"].locator == 'link[rel="canonical"]|endpoint_host'
    assert by_field["contacts.emails"].value == ["abitur@kubsu.ru"]
    assert by_field["contacts.emails"].locator == 'div#block-block-8 .icons.email'
    assert by_field["contacts.phones"].value == ["+7 (861) 219-95-30"]
    assert by_field["contacts.phones"].locator == 'div#block-block-8 .icons.phone'
    assert all(fragment.source_key == "kubsu-official" for fragment in fragments)
    assert (
        by_field["contacts.emails"].metadata["source_field"]
        == "footer.admission_email"
    )


def test_kubsu_programs_html_extractor_reads_structured_program_rows() -> None:
    context = build_kubsu_programs_context()
    artifact = build_kubsu_programs_artifact()

    fragments = KubSUProgramsHtmlExtractor().extract(
        context=context,
        artifact=artifact,
    )

    grouped: dict[str, dict[str, object]] = {}
    for fragment in fragments:
        group_key = fragment.metadata["record_group_key"]
        grouped.setdefault(group_key, {})[fragment.field_name] = fragment

    assert len(grouped) == 3

    geology = grouped["институт-географии-геологии-туризма-и-сервиса:05.03.01:0"]
    assert geology["programs.faculty"].value == "Институт географии, геологии, туризма и сервиса"
    assert geology["programs.code"].value == "05.03.01"
    assert geology["programs.name"].value == "Геология"
    assert geology["programs.budget_places"].value == 25
    assert geology["programs.passing_score"].value == 182
    assert geology["programs.year"].value == 2025

    hotel = grouped["институт-географии-геологии-туризма-и-сервиса:43.03.03:1"]
    assert hotel["programs.name"].value == "Гостиничное дело"
    assert hotel["programs.budget_places"].value == 24
    assert hotel["programs.passing_score"].value == 216

    math = grouped["факультет-математики-и-компьютерных-наук:01.03.01:2"]
    assert math["programs.faculty"].value == "Факультет математики и компьютерных наук"
    assert math["programs.code"].value == "01.03.01"
    assert math["programs.name"].value == "Математика"
    assert math["programs.year"].locator == "table.programs.header.passing_year"


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


def test_official_site_adapter_maps_kubsu_profile_fragments_to_intermediate_claims() -> None:
    context = build_kubsu_context()
    artifact = build_kubsu_artifact()
    adapter = OfficialSiteAdapter(fetcher=FakeFetcher(artifact))
    fragments = asyncio.run(adapter.extract(context, artifact))

    records = asyncio.run(adapter.map_to_intermediate(context, artifact, fragments))

    assert adapter.can_handle(context) is True
    assert len(records) == 1
    record = records[0]
    assert record.source_key == "kubsu-official"
    assert record.entity_type == "university"
    assert record.entity_hint == "Кубанский государственный университет"
    assert record.metadata["parser_profile"] == "official_site.kubsu.abiturient_html"
    claims_by_field = {claim["field_name"]: claim for claim in record.claims}
    assert claims_by_field["contacts.website"]["value"] == "https://www.kubsu.ru"
    assert claims_by_field["contacts.phones"]["value"] == ["+7 (861) 219-95-30"]
    assert claims_by_field["contacts.emails"]["value"] == ["abitur@kubsu.ru"]


def test_official_site_adapter_groups_kubsu_program_rows_into_intermediate_records() -> None:
    context = build_kubsu_programs_context()
    artifact = build_kubsu_programs_artifact()
    adapter = OfficialSiteAdapter(fetcher=FakeFetcher(artifact))
    fragments = asyncio.run(adapter.extract(context, artifact))

    records = asyncio.run(adapter.map_to_intermediate(context, artifact, fragments))

    assert adapter.can_handle(context) is True
    assert len(records) == 3

    records_by_group = {
        record.metadata["record_group_key"]: record
        for record in records
    }

    geology = records_by_group["институт-географии-геологии-туризма-и-сервиса:05.03.01:0"]
    assert geology.entity_type == "admission_program"
    assert geology.entity_hint == "Геология"
    geology_claims = {claim["field_name"]: claim for claim in geology.claims}
    assert geology_claims["programs.faculty"]["value"] == "Институт географии, геологии, туризма и сервиса"
    assert geology_claims["programs.code"]["value"] == "05.03.01"
    assert geology_claims["programs.name"]["value"] == "Геология"
    assert geology_claims["programs.budget_places"]["value"] == 25
    assert geology_claims["programs.passing_score"]["value"] == 182
    assert geology_claims["programs.year"]["value"] == 2025


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


def test_official_site_adapter_executes_kubsu_profile_pipeline() -> None:
    context = build_kubsu_context()
    artifact = build_kubsu_artifact()
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
    assert result.intermediate_records[0].entity_hint == "Кубанский государственный университет"


def test_official_site_adapter_executes_kubsu_programs_profile_pipeline() -> None:
    context = build_kubsu_programs_context()
    artifact = build_kubsu_programs_artifact()
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
    assert result.extracted_fragments == 18
    assert len(result.intermediate_records) == 3
