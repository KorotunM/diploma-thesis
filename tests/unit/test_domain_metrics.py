from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from apps.normalizer.app.cards import UniversityCardProjectionService
from apps.normalizer.app.cards.models import CardProjectionRecord, CardVersionRecord
from apps.normalizer.app.claims import ClaimEvidenceRecord, ClaimRecord
from apps.normalizer.app.facts import ResolvedFactGenerationService, ResolvedFactRecord
from apps.normalizer.app.resolution import SourceTrustTier
from apps.normalizer.app.search_docs import UniversitySearchDocRecord
from apps.normalizer.app.universities import (
    SourceAuthorityRecord,
    UniversityBootstrapResult,
    UniversityRecord,
)
from apps.parser.app.crawl_requests import CrawlRequestProcessingService
from apps.parser.app.parse_completed import ParseCompletedEmitter
from apps.parser.app.parsed_documents import (
    ExtractedFragmentRecord,
    ParsedDocumentPersistenceService,
    ParsedDocumentRecord,
)
from apps.parser.app.raw_artifacts import RawArtifactPersistenceService, RawArtifactRecord
from apps.scheduler.app.runs.models import ManualCrawlTriggerRequest, PipelineRunStatus
from apps.scheduler.app.runs.service import ManualCrawlTriggerService
from apps.scheduler.app.sources.models import CrawlPolicy, SourceEndpointRecord
from libs.contracts.events import CrawlRequestEvent, CrawlRequestPayload, EventHeader
from libs.observability import NoopDomainMetricsCollector, PrometheusDomainMetricsCollector
from libs.source_sdk import ExtractedFragment, FetchContext, FetchedArtifact


class FakeMetricsCollector:
    def __init__(self) -> None:
        self.crawl_jobs: list[dict] = []
        self.parse_runs: list[dict] = []
        self.normalize_runs: list[dict] = []
        self.card_builds: list[dict] = []

    def record_crawl_job(self, **kwargs) -> None:
        self.crawl_jobs.append(kwargs)

    def record_parse_run(self, **kwargs) -> None:
        self.parse_runs.append(kwargs)

    def record_normalize_run(self, **kwargs) -> None:
        self.normalize_runs.append(kwargs)

    def record_card_build(self, **kwargs) -> None:
        self.card_builds.append(kwargs)


class FakeEndpointRepository:
    def __init__(self, endpoint: SourceEndpointRecord | None) -> None:
        self.endpoint = endpoint

    def get(self, source_key: str, endpoint_id):
        return self.endpoint


class FakeRunRepository:
    def __init__(self) -> None:
        self.record = None

    def create(self, **kwargs):
        self.record = {
            "run_id": kwargs["run_id"],
            "run_type": kwargs["run_type"],
            "status": kwargs["status"],
            "trigger_type": kwargs["trigger_type"],
            "source_key": kwargs["source_key"],
            "started_at": datetime(2026, 4, 29, 12, 0, tzinfo=UTC),
            "finished_at": None,
            "metadata": kwargs["metadata"],
        }
        return self.record

    def transition(self, **kwargs):
        if self.record is None:
            return None
        self.record = {**self.record, "status": kwargs["status"]}
        return self.record

    def commit(self) -> None:
        return None


class FakeCrawlRequestPublisher:
    def __init__(self, *, fail_with: Exception | None = None) -> None:
        self.fail_with = fail_with

    def publish(self, payload, *, queue_name: str, headers=None):
        if self.fail_with is not None:
            raise self.fail_with
        return type(
            "PublishResult",
            (),
            {
                "queue_name": queue_name,
                "exchange_name": "parser.jobs",
                "routing_key": "high" if queue_name == "parser.high" else "bulk",
            },
        )()


class FakeFetcher:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    async def fetch(self, context: FetchContext) -> FetchedArtifact:
        return FetchedArtifact(
            raw_artifact_id=uuid4(),
            crawl_run_id=context.crawl_run_id,
            source_key=context.source_key,
            source_url=context.endpoint_url,
            final_url=context.endpoint_url,
            http_status=200,
            content_type="text/html",
            response_headers={"content-type": "text/html"},
            content_length=len(self.payload),
            sha256="a" * 64,
            fetched_at=datetime(2026, 4, 29, 12, 5, tzinfo=UTC),
            render_mode=context.render_mode,
            content=self.payload,
        )


class FakeRawStore:
    async def store_raw(self, context: FetchContext, artifact: FetchedArtifact) -> FetchedArtifact:
        return artifact.model_copy(
            update={
                "storage_bucket": "raw-html",
                "storage_object_key": "raw/path.html",
                "metadata": {},
            }
        )


class FakeRawRepository:
    def __init__(self) -> None:
        self.record = None

    def upsert_from_artifact(
        self,
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> RawArtifactRecord:
        self.record = RawArtifactRecord(
            raw_artifact_id=artifact.raw_artifact_id,
            crawl_run_id=artifact.crawl_run_id,
            source_key=context.source_key,
            source_url=context.endpoint_url,
            final_url=artifact.final_url or context.endpoint_url,
            http_status=artifact.http_status,
            content_type=artifact.content_type,
            content_length=artifact.content_length,
            sha256=artifact.sha256,
            storage_bucket=artifact.storage_bucket or "raw-html",
            storage_object_key=artifact.storage_object_key or "raw/path.html",
            etag=None,
            last_modified=None,
            fetched_at=artifact.fetched_at,
            metadata=artifact.metadata,
        )
        return self.record

    def commit(self) -> None:
        return None


class FakeParsedDocumentRepository:
    def __init__(self) -> None:
        self.document = None
        self.fragments = None

    def upsert_document(self, *, execution_result):
        artifact = execution_result.artifact
        self.document = ParsedDocumentRecord(
            parsed_document_id=uuid4(),
            crawl_run_id=artifact.crawl_run_id,
            raw_artifact_id=artifact.raw_artifact_id,
            source_key=artifact.source_key,
            parser_profile="official_site.default",
            parser_version="official.0.1.0",
            entity_type="university",
            entity_hint="Example University",
            extracted_fragment_count=1,
            parsed_at=datetime(2026, 4, 29, 12, 6, tzinfo=UTC),
            metadata={},
        )
        return self.document

    def upsert_fragments(self, *, parsed_document, fragments):
        artifact_raw_artifact_id = parsed_document.raw_artifact_id
        self.fragments = [
            ExtractedFragmentRecord(
                fragment_id=uuid4(),
                parsed_document_id=parsed_document.parsed_document_id,
                raw_artifact_id=artifact_raw_artifact_id,
                source_key=parsed_document.source_key,
                field_name="canonical_name",
                value="Example University",
                value_type="str",
                locator="h1",
                confidence=0.98,
                metadata={},
            )
        ]
        return self.fragments

    def commit(self) -> None:
        return None


class FakeSourceAdapter:
    def can_handle(self, context: FetchContext) -> bool:
        return True

    def build_execution_plan(self, context: FetchContext):
        return type(
            "Plan",
            (),
            {
                "execution_id": uuid4(),
                "adapter_key": "official",
                "adapter_version": "official.0.1.0",
            },
        )()

    async def extract(self, context: FetchContext, stored_artifact: FetchedArtifact):
        return [
            ExtractedFragment(
                raw_artifact_id=stored_artifact.raw_artifact_id,
                source_key=context.source_key,
                source_url=context.endpoint_url,
                field_name="canonical_name",
                value="Example University",
                locator="h1",
                confidence=0.98,
                metadata={},
            )
        ]

    async def map_to_intermediate(
        self,
        context: FetchContext,
        stored_artifact: FetchedArtifact,
        fragments,
    ):
        return []


class FakeParseCompletedPublisher:
    def publish(self, payload, *, queue_name: str, headers=None):
        return type(
            "PublishResult",
            (),
            {
                "queue_name": queue_name,
                "exchange_name": "normalize.jobs",
                "routing_key": "high",
            },
        )()


class FakeResolvedFactRepository:
    def upsert_resolved_facts(self, candidates):
        return [
            ResolvedFactRecord(
                resolved_fact_id=candidate.resolved_fact_id,
                university_id=candidate.university_id,
                field_name=candidate.field_name,
                value=candidate.value,
                value_type=candidate.value_type,
                fact_score=candidate.fact_score,
                resolution_policy=candidate.resolution_policy,
                selected_claim_ids=candidate.selected_claim_ids,
                selected_evidence_ids=candidate.selected_evidence_ids,
                card_version=candidate.card_version,
                resolved_at=datetime(2026, 4, 29, 12, 15, tzinfo=UTC),
                metadata=candidate.metadata,
            )
            for candidate in candidates
        ]

    def commit(self) -> None:
        return None


class FakeCardRepository:
    def upsert_card_version(self, *, university_id, card_version, normalizer_version):
        return CardVersionRecord(
            university_id=university_id,
            card_version=card_version,
            normalizer_version=normalizer_version,
            generated_at=datetime(2026, 4, 29, 12, 20, tzinfo=UTC),
        )

    def upsert_delivery_projection(self, *, card, generated_at):
        return CardProjectionRecord(
            university_id=card.university_id,
            card_version=card.version.card_version,
            card=card,
            generated_at=generated_at,
            metadata={},
        )

    def commit(self) -> None:
        return None


class FakeSearchDocService:
    def refresh_for_card(self, card):
        return UniversitySearchDocRecord(
            university_id=card.university_id,
            card_version=card.version.card_version,
            canonical_name=card.canonical_name.value or "",
            canonical_name_normalized=(card.canonical_name.value or "").lower(),
            website_url=card.contacts.website,
            website_domain="example.edu",
            country_code=card.location.country,
            city_name=card.location.city,
            aliases=[],
            search_document={},
            generated_at=datetime(2026, 4, 29, 12, 20, tzinfo=UTC),
            metadata={},
        )


def build_bootstrap_result() -> UniversityBootstrapResult:
    claims = [
        ClaimRecord(
            claim_id=uuid4(),
            parsed_document_id=uuid4(),
            source_key="msu-official",
            field_name="canonical_name",
            value="Example University",
            value_type="str",
            entity_hint="Example University",
            parser_version="official.0.1.0",
            normalizer_version="normalizer.0.1.0",
            parser_confidence=0.98,
            created_at=datetime(2026, 4, 29, 12, 10, tzinfo=UTC),
            metadata={},
        )
    ]
    evidence = [
        ClaimEvidenceRecord(
            evidence_id=uuid4(),
            claim_id=claims[0].claim_id,
            raw_artifact_id=uuid4(),
            fragment_id=uuid4(),
            source_key="msu-official",
            source_url="https://example.edu",
            captured_at=datetime(2026, 4, 29, 12, 9, tzinfo=UTC),
            metadata={},
        )
    ]
    return UniversityBootstrapResult(
        source=SourceAuthorityRecord(
            source_id=uuid4(),
            source_key="msu-official",
            source_type="official_site",
            trust_tier=SourceTrustTier.AUTHORITATIVE,
            is_active=True,
            metadata={},
        ),
        sources_used=[],
        university=UniversityRecord(
            university_id=uuid4(),
            canonical_name="Example University",
            canonical_domain="example.edu",
            country_code="RU",
            city_name="Moscow",
            created_at=datetime(2026, 4, 29, 12, 10, tzinfo=UTC),
            metadata={"bootstrap_policy": "single_source_authoritative"},
        ),
        claims_used=claims,
        evidence_used=evidence,
    )


def build_event() -> CrawlRequestEvent:
    return CrawlRequestEvent(
        header=EventHeader(producer="scheduler"),
        payload=CrawlRequestPayload(
            crawl_run_id=uuid4(),
            source_key="msu-official",
            endpoint_url="https://example.edu",
            priority="high",
            trigger="manual",
            parser_profile="official_site.default",
            requested_at=datetime(2026, 4, 29, 12, 0, tzinfo=UTC),
            metadata={},
        ),
    )


def test_manual_crawl_trigger_service_records_domain_metric() -> None:
    metrics = FakeMetricsCollector()
    endpoint_id = uuid4()
    service = ManualCrawlTriggerService(
        endpoint_repository=FakeEndpointRepository(
            SourceEndpointRecord(
                endpoint_id=endpoint_id,
                source_id=uuid4(),
                source_key="msu-official",
                endpoint_url="https://example.edu",
                parser_profile="official_site.default",
                crawl_policy=CrawlPolicy(),
            )
        ),
        run_repository=FakeRunRepository(),
        publisher=FakeCrawlRequestPublisher(),
        metrics_collector=metrics,
    )

    response = service.trigger_manual_crawl(
        ManualCrawlTriggerRequest(
            source_key="msu-official",
            endpoint_id=endpoint_id,
            priority="high",
        )
    )

    assert response.pipeline_run.status == PipelineRunStatus.PUBLISHED
    assert metrics.crawl_jobs == [
        {
            "status": "published",
            "trigger_type": "manual",
            "priority": "high",
            "parser_profile": "official_site.default",
        }
    ]


@pytest.mark.asyncio
async def test_crawl_request_processing_service_records_parse_metric() -> None:
    metrics = FakeMetricsCollector()
    service = CrawlRequestProcessingService(
        fetcher=FakeFetcher(b"<html></html>"),
        raw_artifact_service=RawArtifactPersistenceService(
            raw_store=FakeRawStore(),
            repository=FakeRawRepository(),
        ),
        parsed_document_service=ParsedDocumentPersistenceService(FakeParsedDocumentRepository()),
        source_adapters=(FakeSourceAdapter(),),
        parse_completed_emitter=ParseCompletedEmitter(publisher=FakeParseCompletedPublisher()),
        metrics_collector=metrics,
    )

    result = await service.process(build_event())

    assert result.parsed_document is not None
    assert metrics.parse_runs[0]["status"] == "parsed"
    assert metrics.parse_runs[0]["parser_profile"] == "official_site.default"
    assert metrics.parse_runs[0]["parser_version"] == "official.0.1.0"
    assert metrics.parse_runs[0]["fragment_count"] == 1
    assert metrics.parse_runs[0]["parse_completed_emitted"] is True


def test_resolved_fact_generation_service_records_normalize_metric() -> None:
    metrics = FakeMetricsCollector()
    service = ResolvedFactGenerationService(
        FakeResolvedFactRepository(),
        metrics_collector=metrics,
    )

    result = service.generate_for_bootstrap(build_bootstrap_result())

    assert len(result.facts) == 1
    assert metrics.normalize_runs[0]["status"] == "succeeded"
    assert metrics.normalize_runs[0]["parser_version"] == "official.0.1.0"
    assert metrics.normalize_runs[0]["normalizer_version"] == "normalizer.0.1.0"
    assert metrics.normalize_runs[0]["claim_count"] == 1
    assert metrics.normalize_runs[0]["evidence_count"] == 1
    assert metrics.normalize_runs[0]["resolved_fact_count"] == 1
    assert metrics.normalize_runs[0]["source_count"] == 1


def test_university_card_projection_service_records_card_build_metric() -> None:
    metrics = FakeMetricsCollector()
    bootstrap_result = build_bootstrap_result()
    fact = ResolvedFactRecord(
        resolved_fact_id=uuid4(),
        university_id=bootstrap_result.university.university_id,
        field_name="canonical_name",
        value="Example University",
        value_type="str",
        fact_score=0.98,
        resolution_policy="tiered_authority_highest_confidence",
        selected_claim_ids=[bootstrap_result.claims_used[0].claim_id],
        selected_evidence_ids=[bootstrap_result.evidence_used[0].evidence_id],
        card_version=1,
        resolved_at=datetime(2026, 4, 29, 12, 15, tzinfo=UTC),
        metadata={
            "source_key": "msu-official",
            "source_urls": ["https://example.edu"],
        },
    )
    fact_result = type(
        "FactResult",
        (),
        {"university": bootstrap_result.university, "facts": [fact]},
    )()
    service = UniversityCardProjectionService(
        FakeCardRepository(),
        search_doc_service=FakeSearchDocService(),
        metrics_collector=metrics,
    )

    result = service.create_projection(fact_result)

    assert result.card_version.card_version == 1
    assert metrics.card_builds[0]["status"] == "succeeded"
    assert metrics.card_builds[0]["normalizer_version"] == "normalizer.0.1.0"
    assert metrics.card_builds[0]["resolved_fact_count"] == 1
    assert metrics.card_builds[0]["rating_count"] == 0
    assert metrics.card_builds[0]["search_doc_refreshed"] is True


def test_prometheus_domain_metrics_collector_registers_pipeline_metric_names() -> None:
    prometheus = pytest.importorskip("prometheus_client")
    registry = prometheus.CollectorRegistry()
    collector = PrometheusDomainMetricsCollector(registry=registry)

    collector.record_crawl_job(
        status="published",
        trigger_type="manual",
        priority="high",
        parser_profile="official_site.default",
    )
    collector.record_parse_run(
        status="parsed",
        parser_profile="official_site.default",
        parser_version="official.0.1.0",
        fragment_count=4,
        duration_seconds=0.2,
        parse_completed_emitted=True,
    )
    collector.record_normalize_run(
        status="succeeded",
        parser_version="official.0.1.0",
        normalizer_version="normalizer.0.1.0",
        claim_count=4,
        evidence_count=4,
        resolved_fact_count=4,
        source_count=1,
        rating_fact_count=0,
        duration_seconds=0.3,
    )
    collector.record_card_build(
        status="succeeded",
        normalizer_version="normalizer.0.1.0",
        resolved_fact_count=4,
        rating_count=1,
        duration_seconds=0.1,
        search_doc_refreshed=True,
    )

    metric_sample_names = {
        sample.name
        for family in registry.collect()
        for sample in family.samples
    }
    assert "pipeline_crawl_jobs_total" in metric_sample_names
    assert "pipeline_parse_runs_total" in metric_sample_names
    assert "pipeline_parse_duration_seconds_count" in metric_sample_names
    assert "pipeline_parse_fragments_per_run_count" in metric_sample_names
    assert "pipeline_normalize_runs_total" in metric_sample_names
    assert "pipeline_normalize_duration_seconds_count" in metric_sample_names
    assert "pipeline_normalize_claims_per_run_count" in metric_sample_names
    assert "pipeline_normalize_evidence_per_run_count" in metric_sample_names
    assert "pipeline_normalize_resolved_facts_per_run_count" in metric_sample_names
    assert "pipeline_card_builds_total" in metric_sample_names
    assert "pipeline_card_build_duration_seconds_count" in metric_sample_names
    assert "pipeline_card_build_resolved_facts_per_run_count" in metric_sample_names
    assert "pipeline_card_build_ratings_per_run_count" in metric_sample_names


def test_noop_domain_metrics_collector_is_safe() -> None:
    collector = NoopDomainMetricsCollector()

    collector.record_crawl_job(
        status="published",
        trigger_type="manual",
        priority="high",
        parser_profile="official_site.default",
    )
    collector.record_parse_run(
        status="parsed",
        parser_profile="official_site.default",
        parser_version="official.0.1.0",
        fragment_count=1,
        duration_seconds=0.1,
        parse_completed_emitted=True,
    )
    collector.record_normalize_run(
        status="succeeded",
        parser_version="official.0.1.0",
        normalizer_version="normalizer.0.1.0",
        claim_count=1,
        evidence_count=1,
        resolved_fact_count=1,
        source_count=1,
        rating_fact_count=0,
        duration_seconds=0.1,
    )
    collector.record_card_build(
        status="succeeded",
        normalizer_version="normalizer.0.1.0",
        resolved_fact_count=1,
        rating_count=0,
        duration_seconds=0.1,
        search_doc_refreshed=True,
    )
