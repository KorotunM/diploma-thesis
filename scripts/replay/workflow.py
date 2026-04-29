from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from apps.normalizer.app.cards import (
    UniversityCardProjectionResult,
    UniversityCardProjectionService,
)
from apps.normalizer.app.claims import (
    ClaimBuildRepository,
    ClaimBuildResult,
    ClaimBuildService,
    ParsedDocumentSnapshot,
)
from apps.normalizer.app.facts import (
    ResolvedFactBuildResult,
    ResolvedFactGenerationService,
)
from apps.normalizer.app.universities import (
    UniversityBootstrapResult,
    UniversityBootstrapService,
)
from apps.parser.adapters.aggregators import AggregatorAdapter
from apps.parser.adapters.official_sites import OfficialSiteAdapter
from apps.parser.adapters.rankings import RankingAdapter
from apps.parser.app.parsed_documents import (
    ExtractedFragmentRecord,
    ParsedDocumentPersistenceService,
    ParsedDocumentRecord,
    ParsedDocumentRepository,
)
from apps.parser.app.raw_artifacts import RawArtifactRecord, RawArtifactRepository
from libs.contracts.events import NormalizeRequestPayload
from libs.source_sdk import (
    FetchContext,
    FetchedArtifact,
    ParserExecutionResult,
    ParserExecutionStatus,
    RawFetcher,
    SourceAdapter,
)
from libs.storage import MinIOStorageClient, get_minio_storage, get_postgres_session_factory


class ReplayWorkflowError(ValueError):
    pass


class NoopFetcher(RawFetcher):
    async def fetch(self, context: FetchContext) -> FetchedArtifact:
        raise ReplayWorkflowError(
            f"Replay adapter fetch should not be called for stored artifact {context.source_key}."
        )


class ParserReplayResult:
    def __init__(
        self,
        *,
        raw_artifact: RawArtifactRecord,
        parsed_document: ParsedDocumentRecord,
        fragments: list[ExtractedFragmentRecord],
        reused_existing: bool,
    ) -> None:
        self.raw_artifact = raw_artifact
        self.parsed_document = parsed_document
        self.fragments = fragments
        self.reused_existing = reused_existing

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_artifact_id": str(self.raw_artifact.raw_artifact_id),
            "parsed_document_id": str(self.parsed_document.parsed_document_id),
            "source_key": self.parsed_document.source_key,
            "parser_version": self.parsed_document.parser_version,
            "fragment_count": len(self.fragments),
            "reused_existing": self.reused_existing,
        }


class NormalizationReplayResult:
    def __init__(
        self,
        *,
        parsed_document: ParsedDocumentSnapshot,
        claim_result: ClaimBuildResult,
        bootstrap_result: UniversityBootstrapResult,
        fact_result: ResolvedFactBuildResult,
        projection_result: UniversityCardProjectionResult,
    ) -> None:
        self.parsed_document = parsed_document
        self.claim_result = claim_result
        self.bootstrap_result = bootstrap_result
        self.fact_result = fact_result
        self.projection_result = projection_result

    def to_dict(self) -> dict[str, Any]:
        return {
            "parsed_document_id": str(self.parsed_document.parsed_document_id),
            "source_key": self.parsed_document.source_key,
            "parser_version": self.parsed_document.parser_version,
            "normalizer_version": self.projection_result.card_version.normalizer_version,
            "claim_count": len(self.claim_result.claims),
            "evidence_count": len(self.claim_result.evidence),
            "university_id": str(self.bootstrap_result.university.university_id),
            "card_version": self.projection_result.card_version.card_version,
            "resolved_fact_count": len(self.fact_result.facts),
        }


class StoredArtifactReplayLoader:
    def __init__(
        self,
        *,
        raw_artifact_repository: RawArtifactRepository,
        storage: MinIOStorageClient,
    ) -> None:
        self._raw_artifact_repository = raw_artifact_repository
        self._storage = storage

    def load(
        self,
        raw_artifact_id: UUID,
    ) -> tuple[FetchContext, FetchedArtifact, RawArtifactRecord]:
        record = self._raw_artifact_repository.get_by_id(raw_artifact_id)
        if record is None:
            raise ReplayWorkflowError(f"Raw artifact {raw_artifact_id} was not found.")

        payload = self._storage.get_bytes(
            bucket_name=record.storage_bucket,
            object_name=record.storage_object_key,
        )
        actual_sha256 = hashlib.sha256(payload).hexdigest()
        if actual_sha256 != record.sha256:
            raise ReplayWorkflowError(
                "Stored raw artifact sha256 mismatch: "
                f"expected {record.sha256}, got {actual_sha256}."
            )

        context = self._build_context(record)
        artifact = self._build_artifact(record=record, payload=payload)
        return context, artifact, record

    @staticmethod
    def _build_context(record: RawArtifactRecord) -> FetchContext:
        metadata = record.metadata
        return FetchContext(
            crawl_run_id=record.crawl_run_id,
            source_key=record.source_key,
            endpoint_url=record.source_url,
            priority=StoredArtifactReplayLoader._priority_from_metadata(metadata),
            trigger="replay",
            parser_profile=StoredArtifactReplayLoader._string_metadata(
                metadata,
                "parser_profile",
                default="default",
            ),
            requested_at=StoredArtifactReplayLoader._datetime_metadata(
                metadata,
                "requested_at",
                default=record.fetched_at,
            ),
            render_mode=StoredArtifactReplayLoader._render_mode(metadata),
            metadata={
                **metadata,
                "replay_source": "stored_raw_artifact",
                "original_trigger": metadata.get("trigger"),
            },
        )

    @staticmethod
    def _build_artifact(
        *,
        record: RawArtifactRecord,
        payload: bytes,
    ) -> FetchedArtifact:
        response_headers = record.metadata.get("response_headers")
        return FetchedArtifact(
            raw_artifact_id=record.raw_artifact_id,
            crawl_run_id=record.crawl_run_id,
            source_key=record.source_key,
            source_url=record.source_url,
            final_url=record.final_url,
            http_status=record.http_status,
            content_type=record.content_type,
            response_headers=(
                response_headers if isinstance(response_headers, dict) else {}
            ),
            content_length=record.content_length,
            sha256=record.sha256,
            fetched_at=record.fetched_at,
            render_mode=StoredArtifactReplayLoader._render_mode(record.metadata),
            etag=record.etag,
            last_modified=record.last_modified,
            storage_bucket=record.storage_bucket,
            storage_object_key=record.storage_object_key,
            content=payload,
            metadata={
                **record.metadata,
                "replayed_from_raw_artifact_id": str(record.raw_artifact_id),
            },
        )

    @staticmethod
    def _string_metadata(
        metadata: dict[str, Any],
        key: str,
        *,
        default: str,
    ) -> str:
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        return default

    @staticmethod
    def _datetime_metadata(
        metadata: dict[str, Any],
        key: str,
        *,
        default: datetime,
    ) -> datetime:
        value = metadata.get(key)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                return default
        return default

    @staticmethod
    def _priority_from_metadata(
        metadata: dict[str, Any],
    ) -> Literal["high", "bulk"]:
        value = metadata.get("priority")
        if value == "high":
            return "high"
        return "bulk"

    @staticmethod
    def _render_mode(metadata: dict[str, Any]) -> Literal["http", "browser", "auto"]:
        value = metadata.get("render_mode")
        if value in {"http", "browser", "auto"}:
            return value
        return "http"


class ParserReplayService:
    def __init__(
        self,
        *,
        raw_loader: StoredArtifactReplayLoader,
        parsed_document_repository: ParsedDocumentRepository,
        parsed_document_service: ParsedDocumentPersistenceService,
        source_adapters: tuple[SourceAdapter, ...],
    ) -> None:
        self._raw_loader = raw_loader
        self._parsed_document_repository = parsed_document_repository
        self._parsed_document_service = parsed_document_service
        self._source_adapters = source_adapters

    async def replay(self, raw_artifact_id: UUID) -> ParserReplayResult:
        context, artifact, raw_record = self._raw_loader.load(raw_artifact_id)
        adapter = self._resolve_adapter(context)
        existing_document = (
            self._parsed_document_repository.get_document_by_raw_artifact_and_parser_version(
                raw_artifact_id=raw_artifact_id,
                parser_version=adapter.adapter_version,
            )
        )
        if existing_document is not None:
            return ParserReplayResult(
                raw_artifact=raw_record,
                parsed_document=existing_document,
                fragments=self._parsed_document_repository.list_fragments_for_document(
                    existing_document.parsed_document_id
                ),
                reused_existing=True,
            )

        plan = adapter.build_execution_plan(context)
        fragments = list(await adapter.extract(context, artifact))
        intermediate_records = list(
            await adapter.map_to_intermediate(context, artifact, fragments)
        )
        execution_result = ParserExecutionResult(
            execution_id=plan.execution_id,
            crawl_run_id=context.crawl_run_id,
            status=ParserExecutionStatus.SUCCEEDED,
            adapter_key=plan.adapter_key,
            adapter_version=plan.adapter_version,
            artifact=artifact,
            fragments=fragments,
            intermediate_records=intermediate_records,
            started_at=plan.created_at,
            completed_at=datetime.now(plan.created_at.tzinfo),
            metadata={
                **plan.metadata,
                "replay_source": "stored_raw_artifact",
            },
        )
        parsed_document, persisted_fragments = (
            self._parsed_document_service.persist_successful_execution(
                execution_result=execution_result
            )
        )
        return ParserReplayResult(
            raw_artifact=raw_record,
            parsed_document=parsed_document,
            fragments=persisted_fragments,
            reused_existing=False,
        )

    def _resolve_adapter(self, context: FetchContext) -> SourceAdapter:
        adapter = next(
            (candidate for candidate in self._source_adapters if candidate.can_handle(context)),
            None,
        )
        if adapter is None:
            raise ReplayWorkflowError(
                "No parser adapter is registered for "
                f"source_key={context.source_key} parser_profile={context.parser_profile}."
            )
        return adapter


class NormalizationReplayService:
    def __init__(
        self,
        *,
        claim_repository: ClaimBuildRepository,
        claim_build_service: ClaimBuildService,
        university_bootstrap_service: UniversityBootstrapService,
        resolved_fact_generation_service: ResolvedFactGenerationService,
        university_card_projection_service: UniversityCardProjectionService,
        normalizer_version: str = "normalizer.0.1.0",
    ) -> None:
        self._claim_repository = claim_repository
        self._claim_build_service = claim_build_service
        self._university_bootstrap_service = university_bootstrap_service
        self._resolved_fact_generation_service = resolved_fact_generation_service
        self._university_card_projection_service = university_card_projection_service
        self._normalizer_version = normalizer_version

    def replay(self, parsed_document_id: UUID) -> NormalizationReplayResult:
        parsed_document = self._claim_repository.get_parsed_document(parsed_document_id)
        if parsed_document is None:
            raise ReplayWorkflowError(
                f"Parsed document {parsed_document_id} was not found."
            )
        payload = NormalizeRequestPayload(
            crawl_run_id=parsed_document.crawl_run_id,
            source_key=parsed_document.source_key,
            parsed_document_id=parsed_document.parsed_document_id,
            parser_version=parsed_document.parser_version,
            normalizer_version=self._normalizer_version,
            metadata={
                "trigger": "replay",
                "replay_source": "stored_parsed_document",
            },
        )
        claim_result = self._claim_build_service.build_claims_from_extracted_fragments(payload)
        bootstrap_result = self._university_bootstrap_service.consolidate_claims(claim_result)
        fact_result = self._resolved_fact_generation_service.generate_for_bootstrap(
            bootstrap_result
        )
        projection_result = self._university_card_projection_service.create_projection(
            fact_result
        )
        return NormalizationReplayResult(
            parsed_document=parsed_document,
            claim_result=claim_result,
            bootstrap_result=bootstrap_result,
            fact_result=fact_result,
            projection_result=projection_result,
        )


class ReplayWorkflow:
    def __init__(
        self,
        *,
        parser_replay_service: ParserReplayService,
        normalization_replay_service: NormalizationReplayService,
    ) -> None:
        self._parser_replay_service = parser_replay_service
        self._normalization_replay_service = normalization_replay_service

    async def replay_parse(self, raw_artifact_id: UUID) -> ParserReplayResult:
        return await self._parser_replay_service.replay(raw_artifact_id)

    def replay_normalize(self, parsed_document_id: UUID) -> NormalizationReplayResult:
        return self._normalization_replay_service.replay(parsed_document_id)

    async def replay_full(
        self,
        raw_artifact_id: UUID,
    ) -> tuple[ParserReplayResult, NormalizationReplayResult]:
        parser_result = await self.replay_parse(raw_artifact_id)
        normalization_result = self.replay_normalize(
            parser_result.parsed_document.parsed_document_id
        )
        return parser_result, normalization_result


def build_parser_replay_service(session) -> ParserReplayService:
    raw_artifact_repository = RawArtifactRepository(session)
    parsed_document_repository = ParsedDocumentRepository(session)
    return ParserReplayService(
        raw_loader=StoredArtifactReplayLoader(
            raw_artifact_repository=raw_artifact_repository,
            storage=get_minio_storage(service_name="parser"),
        ),
        parsed_document_repository=parsed_document_repository,
        parsed_document_service=ParsedDocumentPersistenceService(parsed_document_repository),
        source_adapters=(
            AggregatorAdapter(fetcher=NoopFetcher()),
            OfficialSiteAdapter(fetcher=NoopFetcher()),
            RankingAdapter(fetcher=NoopFetcher()),
        ),
    )


def build_normalization_replay_service(
    session,
    *,
    normalizer_version: str = "normalizer.0.1.0",
) -> NormalizationReplayService:
    claim_repository = ClaimBuildRepository(session)
    from apps.normalizer.app.dependencies import (
        create_resolved_fact_generation_service,
        create_university_bootstrap_service,
        create_university_card_projection_service,
    )

    return NormalizationReplayService(
        claim_repository=claim_repository,
        claim_build_service=ClaimBuildService(claim_repository),
        university_bootstrap_service=create_university_bootstrap_service(session),
        resolved_fact_generation_service=create_resolved_fact_generation_service(session),
        university_card_projection_service=create_university_card_projection_service(session),
        normalizer_version=normalizer_version,
    )


def build_replay_workflow(
    *,
    parser_session,
    normalizer_session,
    normalizer_version: str = "normalizer.0.1.0",
) -> ReplayWorkflow:
    return ReplayWorkflow(
        parser_replay_service=build_parser_replay_service(parser_session),
        normalization_replay_service=build_normalization_replay_service(
            normalizer_session,
            normalizer_version=normalizer_version,
        ),
    )


@contextmanager
def managed_session(service_name: str):
    session_factory = get_postgres_session_factory(service_name=service_name)
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.replay",
        description="Replay parse and normalize stages from stored raw artifacts.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    parse_parser = subparsers.add_parser("parse", help="Replay parser stage from raw artifact.")
    parse_parser.add_argument("raw_artifact_id", type=UUID)

    normalize_parser = subparsers.add_parser(
        "normalize",
        help="Replay normalizer stage from parsed document.",
    )
    normalize_parser.add_argument("parsed_document_id", type=UUID)
    normalize_parser.add_argument(
        "--normalizer-version",
        default="normalizer.0.1.0",
    )

    full_parser = subparsers.add_parser(
        "full",
        help="Replay parse and normalize stages from raw artifact.",
    )
    full_parser.add_argument("raw_artifact_id", type=UUID)
    full_parser.add_argument(
        "--normalizer-version",
        default="normalizer.0.1.0",
    )
    return parser


async def run_cli(namespace: argparse.Namespace) -> dict[str, Any]:
    if namespace.command == "parse":
        with managed_session("parser") as parser_session, managed_session(
            "normalizer"
        ) as normalizer_session:
            workflow = build_replay_workflow(
                parser_session=parser_session,
                normalizer_session=normalizer_session,
            )
            result = await workflow.replay_parse(namespace.raw_artifact_id)
            return {"parse": result.to_dict()}

    if namespace.command == "normalize":
        with managed_session("parser") as parser_session, managed_session(
            "normalizer"
        ) as normalizer_session:
            workflow = build_replay_workflow(
                parser_session=parser_session,
                normalizer_session=normalizer_session,
                normalizer_version=namespace.normalizer_version,
            )
            result = workflow.replay_normalize(namespace.parsed_document_id)
            return {"normalize": result.to_dict()}

    if namespace.command == "full":
        with managed_session("parser") as parser_session, managed_session(
            "normalizer"
        ) as normalizer_session:
            workflow = build_replay_workflow(
                parser_session=parser_session,
                normalizer_session=normalizer_session,
                normalizer_version=namespace.normalizer_version,
            )
            parse_result, normalize_result = await workflow.replay_full(
                namespace.raw_artifact_id
            )
            return {
                "parse": parse_result.to_dict(),
                "normalize": normalize_result.to_dict(),
            }

    raise ReplayWorkflowError(f"Unsupported command {namespace.command}.")


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    namespace = parser.parse_args(argv)
    result = asyncio.run(run_cli(namespace))
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0
