from __future__ import annotations

from datetime import UTC, datetime

from apps.parser.app.raw_artifacts import RawArtifactPersistenceService
from libs.contracts.events import CrawlRequestEvent
from libs.source_sdk import FetchContext, RawFetcher

from .models import CrawlRequestProcessingResult


def utc_now() -> datetime:
    return datetime.now(UTC)


class CrawlRequestProcessingService:
    def __init__(
        self,
        *,
        fetcher: RawFetcher,
        raw_artifact_service: RawArtifactPersistenceService,
    ) -> None:
        self._fetcher = fetcher
        self._raw_artifact_service = raw_artifact_service

    async def process(self, event: CrawlRequestEvent) -> CrawlRequestProcessingResult:
        context = FetchContext.from_crawl_request(event.payload)
        fetched_artifact = await self._fetcher.fetch(context)
        stored_artifact, raw_record = (
            await self._raw_artifact_service.persist_after_successful_fetch(
                context=context,
                artifact=fetched_artifact,
            )
        )
        return CrawlRequestProcessingResult(
            event_id=event.header.event_id,
            trace_id=event.header.trace_id,
            crawl_run_id=context.crawl_run_id,
            source_key=context.source_key,
            endpoint_url=context.endpoint_url,
            parser_profile=context.parser_profile,
            raw_artifact=raw_record,
            processed_at=utc_now(),
            metadata={
                "raw_bucket": stored_artifact.storage_bucket,
                "raw_object_key": stored_artifact.storage_object_key,
                "sha256": stored_artifact.sha256,
                "idempotency_key": f"{context.source_key}:{stored_artifact.sha256}",
            },
        )
