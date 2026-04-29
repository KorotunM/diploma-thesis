from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from apps.normalizer.app.cards import (
    CardProjectionRecord,
    CardVersionRecord,
    UniversityCardProjectionResult,
)
from apps.normalizer.app.claims import (
    ClaimBuildResult,
    ClaimEvidenceRecord,
    ClaimRecord,
    ParsedDocumentSnapshot,
)
from apps.normalizer.app.facts import ResolvedFactBuildResult, ResolvedFactRecord
from apps.normalizer.app.resolution import SourceTrustTier
from apps.normalizer.app.search_docs import UniversitySearchDocRecord
from apps.normalizer.app.universities import (
    SourceAuthorityRecord,
    UniversityBootstrapResult,
    UniversityRecord,
)
from apps.parser.app.parsed_documents import ExtractedFragmentRecord, ParsedDocumentRecord
from apps.parser.app.raw_artifacts import RawArtifactRecord
from libs.domain.university import UniversityCard
from libs.source_sdk import ExtractedFragment, FetchContext, FetchedArtifact, IntermediateRecord
from scripts.replay.workflow import (
    NoopFetcher,
    NormalizationReplayService,
    ParserReplayService,
    ReplayWorkflowError,
    StoredArtifactReplayLoader,
)


class FakeRawArtifactRepository:
    def __init__(self, record: RawArtifactRecord | None) -> None:
        self.record = record
        self.requested_ids = []

    def get_by_id(self, raw_artifact_id):
        self.requested_ids.append(raw_artifact_id)
        return self.record


class FakeMinIOStorage:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.calls: list[tuple[str, str]] = []

    def get_bytes(self, *, bucket_name: str, object_name: str) -> bytes:
        self.calls.append((bucket_name, object_name))
        return self.payload


class FakeParsedDocumentRepository:
    def __init__(
        self,
        *,
        existing_document: ParsedDocumentRecord | None = None,
        existing_fragments: list[ExtractedFragmentRecord] | None = None,
    ) -> None:
        self.existing_document = existing_document
        self.existing_fragments = existing_fragments or []
        self.lookup_calls = []
        self.fragment_calls = []

    def get_document_by_raw_artifact_and_parser_version(
        self,
        *,
        raw_artifact_id,
        parser_version: str,
    ):
        self.lookup_calls.append((raw_artifact_id, parser_version))
        return self.existing_document

    def list_fragments_for_document(self, parsed_document_id):
        self.fragment_calls.append(parsed_document_id)
        return self.existing_fragments


class FakeParsedDocumentService:
    def __init__(
        self,
        *,
        document: ParsedDocumentRecord,
        fragments: list[ExtractedFragmentRecord],
    ) -> None:
        self.document = document
        self.fragments = fragments
        self.execution_results = []

    def persist_successful_execution(self, *, execution_result):
        self.execution_results.append(execution_result)
        return self.document, self.fragments


class FakeReplayAdapter:
    source_key = "official_sites"
    adapter_version = "0.2.0"
    supported_parser_profiles = ("official_site.default",)

    def __init__(self) -> None:
        self.extract_calls = []
        self.map_calls = []

    def can_handle(self, context: FetchContext) -> bool:
        return context.parser_profile in self.supported_parser_profiles

    def build_execution_plan(self, context: FetchContext):
        from libs.source_sdk import ParserExecutionPlan

        return ParserExecutionPlan.for_adapter(
            context=context,
            adapter_key="official_sites:0.2.0",
            adapter_version=self.adapter_version,
            metadata={"mode": "replay"},
        )

    async def extract(self, context: FetchContext, artifact: FetchedArtifact):
        self.extract_calls.append((context, artifact))
        return [
            ExtractedFragment(
                raw_artifact_id=artifact.raw_artifact_id,
                source_key=context.source_key,
                source_url=artifact.source_url,
                field_name="canonical_name",
                value="Example University",
                locator="h1",
                confidence=0.99,
            )
        ]

    async def map_to_intermediate(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
        fragments,
    ):
        self.map_calls.append((context, artifact, list(fragments)))
        return [
            IntermediateRecord(
                source_key=context.source_key,
                entity_type="university",
                entity_hint="Example University",
                claims=[{"field_name": "canonical_name", "value": "Example University"}],
                fragment_ids=[fragment.fragment_id for fragment in fragments],
                metadata={"parser_profile": context.parser_profile},
            )
        ]


class FakeClaimRepository:
    def __init__(self, parsed_document: ParsedDocumentSnapshot | None) -> None:
        self.parsed_document = parsed_document
        self.requested_ids = []

    def get_parsed_document(self, parsed_document_id):
        self.requested_ids.append(parsed_document_id)
        return self.parsed_document


class FakeClaimBuildService:
    def __init__(self, result: ClaimBuildResult) -> None:
        self.result = result
        self.payloads = []

    def build_claims_from_extracted_fragments(self, payload):
        self.payloads.append(payload)
        return self.result


class FakeBootstrapService:
    def __init__(self, result: UniversityBootstrapResult) -> None:
        self.result = result
        self.calls = []

    def consolidate_claims(self, claim_result: ClaimBuildResult):
        self.calls.append(claim_result)
        return self.result


class FakeFactService:
    def __init__(self, result: ResolvedFactBuildResult) -> None:
        self.result = result
        self.calls = []

    def generate_for_bootstrap(self, bootstrap_result: UniversityBootstrapResult):
        self.calls.append(bootstrap_result)
        return self.result


class FakeProjectionService:
    def __init__(self, result: UniversityCardProjectionResult) -> None:
        self.result = result
        self.calls = []

    def create_projection(self, fact_result: ResolvedFactBuildResult):
        self.calls.append(fact_result)
        return self.result


def build_raw_artifact_record() -> RawArtifactRecord:
    return RawArtifactRecord(
        raw_artifact_id=uuid4(),
        crawl_run_id=uuid4(),
        source_key="msu-official",
        source_url="https://example.edu/admissions",
        final_url="https://example.edu/admissions",
        http_status=200,
        content_type="text/html; charset=utf-8",
        content_length=len(b"<html>ok</html>"),
        sha256="5f6a4b6a37f4e8f2f8ef2d4e2f0a0a5922f4b6b5e3e6db9d8fb8a1a6b8d5f8f0",
        storage_bucket="raw-html",
        storage_object_key="msu-official/5f/6a/sample.html",
        etag='"etag-1"',
        last_modified="Tue, 29 Apr 2026 10:00:00 GMT",
        fetched_at=datetime(2026, 4, 29, 10, 5, tzinfo=UTC),
        metadata={
            "parser_profile": "official_site.default",
            "priority": "high",
            "trigger": "manual",
            "requested_at": "2026-04-29T10:00:00+00:00",
            "render_mode": "http",
            "response_headers": {"content-type": "text/html; charset=utf-8"},
        },
    )


def build_parsed_document_record(raw_artifact_id) -> ParsedDocumentRecord:
    return ParsedDocumentRecord(
        parsed_document_id=uuid4(),
        crawl_run_id=uuid4(),
        raw_artifact_id=raw_artifact_id,
        source_key="msu-official",
        parser_profile="official_site.default",
        parser_version="0.2.0",
        entity_type="university",
        entity_hint="Example University",
        extracted_fragment_count=1,
        parsed_at=datetime(2026, 4, 29, 10, 10, tzinfo=UTC),
        metadata={"replay_source": "stored_raw_artifact"},
    )


def build_fragment_record(parsed_document_id, raw_artifact_id) -> ExtractedFragmentRecord:
    return ExtractedFragmentRecord(
        fragment_id=uuid4(),
        parsed_document_id=parsed_document_id,
        raw_artifact_id=raw_artifact_id,
        source_key="msu-official",
        field_name="canonical_name",
        value="Example University",
        value_type="str",
        locator="h1",
        confidence=0.99,
        metadata={},
    )


@pytest.mark.asyncio
async def test_parser_replay_service_reuses_existing_parsed_document_for_same_version() -> None:
    payload = b"<html>ok</html>"
    raw_record = build_raw_artifact_record().model_copy(
        update={
            "sha256": hashlib.sha256(payload).hexdigest(),
            "content_length": len(payload),
        }
    )
    existing_document = build_parsed_document_record(raw_record.raw_artifact_id)
    existing_fragment = build_fragment_record(
        existing_document.parsed_document_id,
        raw_record.raw_artifact_id,
    )
    service = ParserReplayService(
        raw_loader=StoredArtifactReplayLoader(
            raw_artifact_repository=FakeRawArtifactRepository(raw_record),
            storage=FakeMinIOStorage(payload),
        ),
        parsed_document_repository=FakeParsedDocumentRepository(
            existing_document=existing_document,
            existing_fragments=[existing_fragment],
        ),
        parsed_document_service=FakeParsedDocumentService(
            document=existing_document,
            fragments=[existing_fragment],
        ),
        source_adapters=(FakeReplayAdapter(),),
    )

    result = await service.replay(raw_record.raw_artifact_id)

    assert result.reused_existing is True
    assert result.parsed_document == existing_document
    assert result.fragments == [existing_fragment]


@pytest.mark.asyncio
async def test_parser_replay_service_rebuilds_parsed_document_from_stored_raw() -> None:
    payload = b"<html>ok</html>"
    raw_record = build_raw_artifact_record().model_copy(
        update={
            "sha256": hashlib.sha256(payload).hexdigest(),
            "content_length": len(payload),
        }
    )
    persisted_document = build_parsed_document_record(raw_record.raw_artifact_id)
    persisted_fragment = build_fragment_record(
        persisted_document.parsed_document_id,
        raw_record.raw_artifact_id,
    )
    parsed_service = FakeParsedDocumentService(
        document=persisted_document,
        fragments=[persisted_fragment],
    )
    adapter = FakeReplayAdapter()
    service = ParserReplayService(
        raw_loader=StoredArtifactReplayLoader(
            raw_artifact_repository=FakeRawArtifactRepository(raw_record),
            storage=FakeMinIOStorage(payload),
        ),
        parsed_document_repository=FakeParsedDocumentRepository(),
        parsed_document_service=parsed_service,
        source_adapters=(adapter,),
    )

    result = await service.replay(raw_record.raw_artifact_id)

    assert result.reused_existing is False
    assert result.parsed_document == persisted_document
    assert result.fragments == [persisted_fragment]
    assert len(parsed_service.execution_results) == 1
    execution_result = parsed_service.execution_results[0]
    assert execution_result.artifact.raw_artifact_id == raw_record.raw_artifact_id
    assert execution_result.artifact.content == payload
    assert execution_result.fragments[0].field_name == "canonical_name"
    assert adapter.extract_calls
    assert adapter.map_calls


def test_normalization_replay_service_rebuilds_claims_and_projection() -> None:
    parsed_document = ParsedDocumentSnapshot(
        parsed_document_id=uuid4(),
        crawl_run_id=uuid4(),
        raw_artifact_id=uuid4(),
        source_key="msu-official",
        parser_profile="official_site.default",
        parser_version="0.2.0",
        entity_type="university",
        entity_hint="Example University",
        parsed_at=datetime(2026, 4, 29, 10, 10, tzinfo=UTC),
        metadata={},
    )
    claim = ClaimRecord(
        claim_id=uuid4(),
        parsed_document_id=parsed_document.parsed_document_id,
        source_key="msu-official",
        field_name="canonical_name",
        value="Example University",
        value_type="str",
        entity_hint="Example University",
        parser_version="0.2.0",
        normalizer_version="normalizer.0.2.0",
        parser_confidence=0.99,
        created_at=datetime(2026, 4, 29, 10, 11, tzinfo=UTC),
        metadata={},
    )
    evidence = ClaimEvidenceRecord(
        evidence_id=uuid4(),
        claim_id=claim.claim_id,
        source_key="msu-official",
        source_url="https://example.edu/admissions",
        raw_artifact_id=parsed_document.raw_artifact_id,
        fragment_id=uuid4(),
        captured_at=datetime(2026, 4, 29, 10, 5, tzinfo=UTC),
        metadata={},
    )
    claim_result = ClaimBuildResult(
        parsed_document=parsed_document,
        claims=[claim],
        evidence=[evidence],
    )
    source = SourceAuthorityRecord(
        source_id=uuid4(),
        source_key="msu-official",
        source_type="official_site",
        trust_tier=SourceTrustTier.AUTHORITATIVE,
        is_active=True,
        metadata={},
    )
    university = UniversityRecord(
        university_id=uuid4(),
        canonical_name="Example University",
        canonical_domain="example.edu",
        country_code="RU",
        city_name="Moscow",
        created_at=datetime(2026, 4, 29, 10, 12, tzinfo=UTC),
        metadata={"bootstrap_policy": "single_source_authoritative"},
    )
    bootstrap_result = UniversityBootstrapResult(
        source=source,
        sources_used=[source],
        university=university,
        claims_used=[claim],
        evidence_used=[evidence],
    )
    resolved_fact = ResolvedFactRecord(
        resolved_fact_id=uuid4(),
        university_id=university.university_id,
        field_name="canonical_name",
        value="Example University",
        value_type="str",
        fact_score=0.99,
        resolution_policy="single_source_authoritative",
        selected_claim_ids=[claim.claim_id],
        selected_evidence_ids=[evidence.evidence_id],
        card_version=1,
        resolved_at=datetime(2026, 4, 29, 10, 13, tzinfo=UTC),
        metadata={"source_key": "msu-official", "source_urls": [evidence.source_url]},
    )
    fact_result = ResolvedFactBuildResult(
        university=university,
        facts=[resolved_fact],
    )
    card = UniversityCard.sample().model_copy(
        update={"university_id": university.university_id}
    )
    generated_at = datetime(2026, 4, 29, 10, 14, tzinfo=UTC)
    projection_result = UniversityCardProjectionResult(
        card_version=CardVersionRecord(
            university_id=university.university_id,
            card_version=1,
            normalizer_version="normalizer.0.2.0",
            generated_at=generated_at,
        ),
        projection=CardProjectionRecord(
            university_id=university.university_id,
            card_version=1,
            card=card,
            generated_at=generated_at,
            metadata={},
        ),
        search_doc=UniversitySearchDocRecord(
            university_id=university.university_id,
            card_version=1,
            canonical_name="Example University",
            canonical_name_normalized="example university",
            website_url="https://example.edu/admissions",
            website_domain="example.edu",
            country_code="RU",
            city_name="Moscow",
            aliases=[],
            search_document={},
            generated_at=generated_at,
            metadata={},
        ),
    )

    claim_repository = FakeClaimRepository(parsed_document)
    claim_service = FakeClaimBuildService(claim_result)
    bootstrap_service = FakeBootstrapService(bootstrap_result)
    fact_service = FakeFactService(fact_result)
    projection_service = FakeProjectionService(projection_result)
    service = NormalizationReplayService(
        claim_repository=claim_repository,
        claim_build_service=claim_service,
        university_bootstrap_service=bootstrap_service,
        resolved_fact_generation_service=fact_service,
        university_card_projection_service=projection_service,
        normalizer_version="normalizer.0.2.0",
    )

    result = service.replay(parsed_document.parsed_document_id)

    assert result.projection_result == projection_result
    assert claim_repository.requested_ids == [parsed_document.parsed_document_id]
    assert claim_service.payloads[0].parsed_document_id == parsed_document.parsed_document_id
    assert claim_service.payloads[0].source_key == "msu-official"
    assert claim_service.payloads[0].parser_version == "0.2.0"
    assert claim_service.payloads[0].normalizer_version == "normalizer.0.2.0"
    assert claim_service.payloads[0].metadata["trigger"] == "replay"
    assert bootstrap_service.calls == [claim_result]
    assert fact_service.calls == [bootstrap_result]
    assert projection_service.calls == [fact_result]


def test_stored_artifact_replay_loader_rejects_sha_mismatch() -> None:
    raw_record = build_raw_artifact_record()
    loader = StoredArtifactReplayLoader(
        raw_artifact_repository=FakeRawArtifactRepository(raw_record),
        storage=FakeMinIOStorage(b"bad payload"),
    )

    with pytest.raises(ReplayWorkflowError, match="sha256 mismatch"):
        loader.load(raw_record.raw_artifact_id)


def test_noop_fetcher_raises_replay_error() -> None:
    context = FetchContext(
        crawl_run_id=uuid4(),
        source_key="msu-official",
        endpoint_url="https://example.edu/admissions",
        parser_profile="official_site.default",
    )

    with pytest.raises(ReplayWorkflowError, match="should not be called"):
        asyncio.run(NoopFetcher().fetch(context))
