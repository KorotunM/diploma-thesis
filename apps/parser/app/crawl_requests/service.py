from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from time import perf_counter
from uuid import UUID

from apps.parser.app.parse_completed import (
    ParseCompletedEmission,
    ParseCompletedEmitter,
)
from apps.parser.app.parsed_documents import (
    ExtractedFragmentRecord,
    ParsedDocumentPersistenceService,
    ParsedDocumentRecord,
)
from apps.parser.app.raw_artifacts import RawArtifactPersistenceService, RawArtifactRecord
from libs.contracts.events import CrawlRequestEvent
from libs.observability import DomainMetricsCollector, get_domain_metrics
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
        parse_completed_emitter: ParseCompletedEmitter | None = None,
        metrics_collector: DomainMetricsCollector | None = None,
    ) -> None:
        self._fetcher = fetcher
        self._raw_artifact_service = raw_artifact_service
        self._parsed_document_service = parsed_document_service
        self._source_adapters = tuple(source_adapters)
        self._parse_completed_emitter = parse_completed_emitter
        self._metrics = metrics_collector or get_domain_metrics()

    async def process(self, event: CrawlRequestEvent) -> CrawlRequestProcessingResult:
        context = FetchContext.from_crawl_request(event.payload)
        started_at = perf_counter()
        try:
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
            parse_completed = self._emit_parse_completed(
                context=context,
                raw_record=raw_record,
                parsed_document=parsed_document,
                extracted_fragments=extracted_fragments,
                trace_id=event.header.trace_id or event.header.event_id,
            )
            result = CrawlRequestProcessingResult(
                event_id=event.header.event_id,
                trace_id=event.header.trace_id,
                crawl_run_id=context.crawl_run_id,
                source_key=context.source_key,
                endpoint_url=context.endpoint_url,
                parser_profile=context.parser_profile,
                raw_artifact=raw_record,
                parsed_document=parsed_document,
                extracted_fragments=extracted_fragments,
                parse_completed=parse_completed,
                processed_at=utc_now(),
                metadata={
                    "raw_bucket": stored_artifact.storage_bucket,
                    "raw_object_key": stored_artifact.storage_object_key,
                    "sha256": stored_artifact.sha256,
                    "idempotency_key": f"{context.source_key}:{stored_artifact.sha256}",
                    "parsed_document_id": (
                        str(parsed_document.parsed_document_id) if parsed_document else None
                    ),
                    "parse_completed_event_id": (
                        str(parse_completed.event.header.event_id)
                        if parse_completed
                        else None
                    ),
                    "parse_completed_queue": (
                        parse_completed.queue_name if parse_completed else None
                    ),
                },
            )
            self._metrics.record_parse_run(
                status="parsed" if parsed_document is not None else "raw_only",
                parser_profile=context.parser_profile,
                parser_version=(
                    parsed_document.parser_version if parsed_document is not None else "unparsed"
                ),
                fragment_count=len(extracted_fragments),
                duration_seconds=perf_counter() - started_at,
                parse_completed_emitted=parse_completed is not None,
            )
            return result
        except Exception:
            self._metrics.record_parse_run(
                status="failed",
                parser_profile=context.parser_profile,
                parser_version="unknown",
                fragment_count=0,
                duration_seconds=perf_counter() - started_at,
                parse_completed_emitted=False,
            )
            raise

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

    def _emit_parse_completed(
        self,
        *,
        context: FetchContext,
        raw_record: RawArtifactRecord,
        parsed_document: ParsedDocumentRecord | None,
        extracted_fragments: list[ExtractedFragmentRecord],
        trace_id: UUID | None,
    ) -> ParseCompletedEmission | None:
        if self._parse_completed_emitter is None or parsed_document is None:
            return None
        return self._parse_completed_emitter.emit(
            context=context,
            raw_artifact=raw_record,
            parsed_document=parsed_document,
            extracted_fragments=extracted_fragments,
            trace_id=trace_id,
        )

    def _resolve_adapter(self, context: FetchContext) -> SourceAdapter | None:
        return next(
            (adapter for adapter in self._source_adapters if adapter.can_handle(context)),
            None,
        )
