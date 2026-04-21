from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

from apps.parser.app.parsed_documents import (
    ExtractedFragmentRecord,
    ParsedDocumentPersistenceService,
    ParsedDocumentRecord,
)
from apps.parser.app.raw_artifacts import RawArtifactPersistenceService
from libs.contracts.events import CrawlRequestEvent
from libs.source_sdk import (
    FetchContext,
    FetchedArtifact,
    ParserExecutionResult,
    ParserExecutionStatus,
    RawFetcher,
    SourceAdapter,
)

from .models import CrawlRequestProcessingResult


def utc_now() -> datetime:
    return datetime.now(UTC)


class CrawlRequestProcessingService:
    def __init__(
        self,
        *,
        fetcher: RawFetcher,
        raw_artifact_service: RawArtifactPersistenceService,
        parsed_document_service: ParsedDocumentPersistenceService | None = None,
        source_adapters: Sequence[SourceAdapter] = (),
    ) -> None:
        self._fetcher = fetcher
        self._raw_artifact_service = raw_artifact_service
        self._parsed_document_service = parsed_document_service
        self._source_adapters = tuple(source_adapters)

    async def process(self, event: CrawlRequestEvent) -> CrawlRequestProcessingResult:
        context = FetchContext.from_crawl_request(event.payload)
        fetched_artifact = await self._fetcher.fetch(context)
        stored_artifact, raw_record = (
            await self._raw_artifact_service.persist_after_successful_fetch(
                context=context,
                artifact=fetched_artifact,
            )
        )
        parsed_document, extracted_fragments = await self._persist_parsed_document(
            context=context,
            stored_artifact=stored_artifact,
        )
        return CrawlRequestProcessingResult(
            event_id=event.header.event_id,
            trace_id=event.header.trace_id,
            crawl_run_id=context.crawl_run_id,
            source_key=context.source_key,
            endpoint_url=context.endpoint_url,
            parser_profile=context.parser_profile,
            raw_artifact=raw_record,
            parsed_document=parsed_document,
            extracted_fragments=extracted_fragments,
            processed_at=utc_now(),
            metadata={
                "raw_bucket": stored_artifact.storage_bucket,
                "raw_object_key": stored_artifact.storage_object_key,
                "sha256": stored_artifact.sha256,
                "idempotency_key": f"{context.source_key}:{stored_artifact.sha256}",
                "parsed_document_id": (
                    str(parsed_document.parsed_document_id) if parsed_document else None
                ),
            },
        )

    async def _persist_parsed_document(
        self,
        *,
        context: FetchContext,
        stored_artifact: FetchedArtifact,
    ) -> tuple[ParsedDocumentRecord | None, list[ExtractedFragmentRecord]]:
        if self._parsed_document_service is None:
            return None, []
        adapter = self._resolve_adapter(context)
        if adapter is None:
            return None, []

        plan = adapter.build_execution_plan(context)
        started_at = utc_now()
        fragments = list(await adapter.extract(context, stored_artifact))
        intermediate_records = list(
            await adapter.map_to_intermediate(context, stored_artifact, fragments)
        )
        execution_result = ParserExecutionResult(
            execution_id=plan.execution_id,
            crawl_run_id=context.crawl_run_id,
            status=ParserExecutionStatus.SUCCEEDED,
            adapter_key=plan.adapter_key,
            adapter_version=plan.adapter_version,
            artifact=stored_artifact,
            fragments=fragments,
            intermediate_records=intermediate_records,
            started_at=started_at,
            completed_at=utc_now(),
        )
        return self._parsed_document_service.persist_successful_execution(
            execution_result=execution_result,
        )

    def _resolve_adapter(self, context: FetchContext) -> SourceAdapter | None:
        return next(
            (adapter for adapter in self._source_adapters if adapter.can_handle(context)),
            None,
        )
