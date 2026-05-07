import asyncio
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from apps.parser.adapters.aggregators import (
    AggregatorAdapter,
    AggregatorPayloadExtractor,
    TabiturientAboutHtmlExtractor,
    TabiturientProxodnoiHtmlExtractor,
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


def build_tabiturient_about_context() -> FetchContext:
    return FetchContext(
        crawl_run_id=uuid4(),
        source_key="tabiturient-aggregator",
        endpoint_url="https://tabiturient.ru/vuzu/eltech/about/",
        parser_profile="aggregator.tabiturient.about_html",
    )


def build_tabiturient_proxodnoi_context() -> FetchContext:
    return FetchContext(
        crawl_run_id=uuid4(),
        source_key="tabiturient-aggregator",
        endpoint_url="https://tabiturient.ru/vuzu/eltech/proxodnoi",
        parser_profile="aggregator.tabiturient.proxodnoi_html",
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


def build_tabiturient_about_artifact(payload: bytes) -> FetchedArtifact:
    return FetchedArtifact(
        raw_artifact_id=uuid4(),
        crawl_run_id=uuid4(),
        source_key="tabiturient-aggregator",
        source_url="https://tabiturient.ru/vuzu/eltech/about/",
        final_url="https://tabiturient.ru/vuzu/eltech/about/",
        http_status=200,
        content_type="text/html; charset=utf-8",
        content_length=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        fetched_at=datetime(2026, 4, 30, 12, 0, tzinfo=UTC),
        render_mode="http",
        content=payload,
    )


def build_tabiturient_proxodnoi_artifact(payload: bytes) -> FetchedArtifact:
    return FetchedArtifact(
        raw_artifact_id=uuid4(),
        crawl_run_id=uuid4(),
        source_key="tabiturient-aggregator",
        source_url="https://tabiturient.ru/vuzu/eltech/proxodnoi",
        final_url="https://tabiturient.ru/vuzu/eltech/proxodnoi",
        http_status=200,
        content_type="text/html; charset=utf-8",
        content_length=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        fetched_at=datetime(2026, 4, 30, 12, 0, tzinfo=UTC),
        render_mode="http",
        content=payload,
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


def test_tabiturient_about_html_extractor_filters_program_blocks_and_reads_live_fields(
) -> None:
    html = """
    <html><body>
      <h2 itemprop="alternateName">СПбГЭТУ «ЛЭТИ»</h2>
      <h2 itemprop="name">Санкт-Петербургский государственный электротехнический университет «ЛЭТИ»</h2>
      <span itemprop="logo">https://tabiturient.ru/logovuz/eltech.png</span>
      <img src="/logovuz/eltech.png" />
      <a href="https://tabiturient.ru/city/spb">Санкт-Петербург и Ленинградская область</a>
      <span>Гос.вуз</span>
      <span>Головной</span>
      <a href="https://tabiturient.ru/globalrating">A+ категория</a>
      <a href="https://tabiturient.ru/vuzu/eltech">8.5 /10 6552 оценок</a>
      <span class="font2">
        Санкт-Петербургский Электротехнический Университет является одним из лидирующих
        учебных заведений в Восточной Европе. Основанный в 1886 году, университет
        стал важным центром технической науки и подготовки инженеров.
      </span>
      <span class="font2">
        Вопрос: Кто может посоветовать, куда поступить?
        Я набрал 240 баллов за информатику, профиль и русский.
        Подскажите, стоит ли выбирать этот вуз и какие есть отзывы?
      </span>
      <div class="p40 pm40">
        Направления подготовки бакалавриата и специальности в вузе:
        Укрупненная группа 01.00.00 01.03.02 | Бакалавриат Прикладная математика
        09.03.01 | Бакалавриат Информатика и вычислительная техника
        11.03.04 | Бакалавриат Электроника и наноэлектроника
      </div>
      <div>Рейтинг вузов по отзывам</div>
    </body></html>
    """.encode()
    context = build_tabiturient_about_context()
    artifact = build_tabiturient_about_artifact(html)

    fragments = TabiturientAboutHtmlExtractor().extract(
        context=context,
        artifact=artifact,
    )
    by_field = {fragment.field_name: fragment for fragment in fragments}

    assert by_field["description"].value.startswith(
        "Санкт-Петербургский Электротехнический Университет"
    )
    assert "Направления подготовки" not in by_field["description"].value
    assert by_field["location.city"].value == "Санкт-Петербург"
    assert "Вопрос:" not in by_field["description"].value
    assert by_field["institutional.category"].value == "A+"
    assert by_field["reviews.rating_count"].value == 6552
    assert by_field["ratings.tabiturient_user"].value == {
        "provider": "tabiturient",
        "year": 2025,
        "metric": "user_rating",
        "value": "8.5",
    }


def test_tabiturient_proxodnoi_html_extractor_reads_one_program_per_card() -> None:
    html = """
    <html><body>
      <div class="mobpaddcard">
        <span class="font2" style="text-transform:uppercase;"><b>Информатика и вычислительная техника</b></span>
        <span style="color:#8D8D8D;" class="font2">Бакалавриат | 09.03.01</span>
        <span class="font2"><b>Профиль:</b> Искусственный интеллект</span>
        <span class="font2"><b>Подразделение:</b> Факультет компьютерных технологий и информатики</span>
        <span class="font2"><b style="white-space:nowrap;">Проходные баллы:</b></span>
        <span class="font11"><b>251</b></span><center><span class="font0">очно</span></center>
      </div>
      <div class="mobpaddcard">
        <span class="font2" style="text-transform:uppercase;"><b>Программная инженерия</b></span>
        <span style="color:#8D8D8D;" class="font2">Бакалавриат | 09.03.04</span>
        <span class="font2"><b>Подразделение:</b> Факультет компьютерных технологий и информатики</span>
        <span class="font11"><b>271</b></span><center><span class="font0">очно</span></center>
      </div>
    </body></html>
    """.encode()
    context = build_tabiturient_proxodnoi_context()
    artifact = build_tabiturient_proxodnoi_artifact(html)

    fragments = TabiturientProxodnoiHtmlExtractor().extract(
        context=context,
        artifact=artifact,
    )

    assert len(fragments) == 2
    first = fragments[0]
    assert first.field_name.startswith("programs.")
    assert first.value["code"] == "09.03.01"
    assert first.value["name"] == "Информатика и вычислительная техника"
    assert first.value["faculty"] == "Факультет компьютерных технологий и информатики"
    assert first.value["passing_score"] == 251
    assert first.value["study_form"] == "full_time"
    assert first.value["level"] == "Бакалавриат"
    assert first.metadata["program_merge_key"].startswith("09.03.01:")


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
    assert record.metadata["adapter_key"] == "aggregators:0.2.0"
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
    assert result.adapter_key == "aggregators:0.2.0"
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
    assert result.adapter_key == "aggregators:0.2.0"
    assert result.artifact is not None
    assert result.artifact.storage_bucket == "raw-json"
    assert result.extracted_fragments == 3
    assert (
        result.intermediate_records[0].entity_hint
        == "Алтайский государственный институт культуры"
    )
