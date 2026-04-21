from __future__ import annotations

from typing import Any, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from apps.parser.app.parsed_documents import (
    ExtractedFragmentRecord,
    ParsedDocumentRecord,
)
from apps.parser.app.raw_artifacts import RawArtifactRecord
from libs.contracts.events import (
    EventHeader,
    ParseCompletedEvent,
    ParseCompletedPayload,
)
from libs.source_sdk import FetchContext

NORMALIZE_HIGH_QUEUE = "normalize.high"
NORMALIZE_BULK_QUEUE = "normalize.bulk"
DEFAULT_PARSED_BUCKET = "parsed-snapshots"


class ParseCompletedPublisher(Protocol):
    def publish(
        self,
        payload: Any,
        *,
        queue_name: str,
        headers: dict[str, Any] | None = None,
    ) -> Any:
        """Publish parse.completed event into the normalizer topology."""


class ParseCompletedEmission(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event: ParseCompletedEvent
    queue_name: str
    exchange_name: str
    routing_key: str


def normalize_queue_for_priority(priority: str) -> str:
    if priority == "high":
        return NORMALIZE_HIGH_QUEUE
    return NORMALIZE_BULK_QUEUE


class ParseCompletedEmitter:
    def __init__(
        self,
        *,
        publisher: ParseCompletedPublisher,
        producer: str = "parser",
        parsed_bucket: str = DEFAULT_PARSED_BUCKET,
    ) -> None:
        self._publisher = publisher
        self._producer = producer
        self._parsed_bucket = parsed_bucket

    def emit(
        self,
        *,
        context: FetchContext,
        raw_artifact: RawArtifactRecord,
        parsed_document: ParsedDocumentRecord,
        extracted_fragments: list[ExtractedFragmentRecord],
        trace_id: UUID | None,
    ) -> ParseCompletedEmission:
        event = self._build_event(
            context=context,
            raw_artifact=raw_artifact,
            parsed_document=parsed_document,
            extracted_fragments=extracted_fragments,
            trace_id=trace_id,
        )
        queue_name = normalize_queue_for_priority(context.priority)
        publish_result = self._publisher.publish(
            event.model_dump(mode="json"),
            queue_name=queue_name,
            headers=self._headers(event=event, context=context),
        )
        return ParseCompletedEmission(
            event=event,
            queue_name=getattr(publish_result, "queue_name", queue_name),
            exchange_name=getattr(publish_result, "exchange_name", "normalize.jobs"),
            routing_key=getattr(
                publish_result,
                "routing_key",
                "high" if queue_name == NORMALIZE_HIGH_QUEUE else "bulk",
            ),
        )

    def _build_event(
        self,
        *,
        context: FetchContext,
        raw_artifact: RawArtifactRecord,
        parsed_document: ParsedDocumentRecord,
        extracted_fragments: list[ExtractedFragmentRecord],
        trace_id: UUID | None,
    ) -> ParseCompletedEvent:
        payload = ParseCompletedPayload(
            crawl_run_id=context.crawl_run_id,
            source_key=context.source_key,
            endpoint_url=context.endpoint_url,
            raw_artifact_id=raw_artifact.raw_artifact_id,
            parsed_document_id=parsed_document.parsed_document_id,
            parser_version=parsed_document.parser_version,
            raw_bucket=raw_artifact.storage_bucket,
            parsed_bucket=self._parsed_bucket,
            extracted_fragments=len(extracted_fragments),
            metadata=self._metadata(
                context=context,
                raw_artifact=raw_artifact,
                parsed_document=parsed_document,
                extracted_fragments=extracted_fragments,
            ),
        )
        return ParseCompletedEvent(
            header=EventHeader(producer=self._producer, trace_id=trace_id),
            payload=payload,
        )

    def _metadata(
        self,
        *,
        context: FetchContext,
        raw_artifact: RawArtifactRecord,
        parsed_document: ParsedDocumentRecord,
        extracted_fragments: list[ExtractedFragmentRecord],
    ) -> dict[str, Any]:
        return {
            "parser_profile": context.parser_profile,
            "priority": context.priority,
            "trigger": context.trigger,
            "raw_object_key": raw_artifact.storage_object_key,
            "parsed_table": "parsing.parsed_document",
            "fragment_table": "parsing.extracted_fragment",
            "entity_type": parsed_document.entity_type,
            "entity_hint": parsed_document.entity_hint,
            "fragment_ids": [
                str(fragment.fragment_id) for fragment in extracted_fragments
            ],
        }

    @staticmethod
    def _headers(
        *,
        event: ParseCompletedEvent,
        context: FetchContext,
    ) -> dict[str, str]:
        return {
            "event_name": event.event_name,
            "event_id": str(event.header.event_id),
            "schema_version": str(event.header.schema_version),
            "crawl_run_id": str(context.crawl_run_id),
            "source_key": context.source_key,
            "priority": context.priority,
        }
