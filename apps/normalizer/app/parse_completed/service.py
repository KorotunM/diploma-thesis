from __future__ import annotations

from libs.contracts.events import (
    EventHeader,
    NormalizeRequestEvent,
    NormalizeRequestPayload,
    ParseCompletedEvent,
)

from apps.normalizer.app.cards import UniversityCardProjectionService
from apps.normalizer.app.card_updated import CardUpdatedEmitter
from apps.normalizer.app.claims import ClaimBuildService
from apps.normalizer.app.facts import ResolvedFactGenerationService
from apps.normalizer.app.universities import UniversityBootstrapService

from .models import ParseCompletedProcessingResult


def build_normalize_request_event(
    event: ParseCompletedEvent,
    *,
    normalizer_version: str = "normalizer.0.1.0",
    producer: str = "normalizer",
) -> NormalizeRequestEvent:
    return NormalizeRequestEvent(
        header=EventHeader(
            producer=producer,
            trace_id=event.header.trace_id or event.header.event_id,
        ),
        payload=NormalizeRequestPayload(
            crawl_run_id=event.payload.crawl_run_id,
            source_key=event.payload.source_key,
            parsed_document_id=event.payload.parsed_document_id,
            parser_version=event.payload.parser_version,
            normalizer_version=normalizer_version,
            metadata={
                **event.payload.metadata,
                "raw_artifact_id": str(event.payload.raw_artifact_id),
                "raw_bucket": event.payload.raw_bucket,
                "parsed_bucket": event.payload.parsed_bucket,
                "endpoint_url": event.payload.endpoint_url,
                "extracted_fragments": event.payload.extracted_fragments,
            },
        ),
    )


class ParseCompletedProcessingService:
    def __init__(
        self,
        *,
        claim_build_service: ClaimBuildService,
        university_bootstrap_service: UniversityBootstrapService,
        resolved_fact_generation_service: ResolvedFactGenerationService,
        university_card_projection_service: UniversityCardProjectionService,
        card_updated_emitter: CardUpdatedEmitter | None = None,
        normalizer_version: str = "normalizer.0.1.0",
    ) -> None:
        self._claim_build_service = claim_build_service
        self._university_bootstrap_service = university_bootstrap_service
        self._resolved_fact_generation_service = resolved_fact_generation_service
        self._university_card_projection_service = university_card_projection_service
        self._card_updated_emitter = card_updated_emitter
        self._normalizer_version = normalizer_version

    def process(
        self,
        event: ParseCompletedEvent,
    ) -> ParseCompletedProcessingResult:
        normalize_request = build_normalize_request_event(
            event,
            normalizer_version=self._normalizer_version,
        )
        claim_result = self._claim_build_service.build_claims_from_extracted_fragments(
            normalize_request.payload
        )
        bootstrap_result = self._university_bootstrap_service.consolidate_claims(
            claim_result
        )
        fact_result = self._resolved_fact_generation_service.generate_for_bootstrap(
            bootstrap_result
        )
        projection_result = self._university_card_projection_service.create_projection(
            fact_result
        )
        card_updated = None
        if self._card_updated_emitter is not None:
            card_updated = self._card_updated_emitter.emit(
                university_id=projection_result.projection.university_id,
                card_version=projection_result.projection.card_version,
                updated_fields=[fact.field_name for fact in fact_result.facts],
                trace_id=normalize_request.header.trace_id,
            )
        return ParseCompletedProcessingResult(
            normalize_request=normalize_request,
            claim_result=claim_result,
            bootstrap_result=bootstrap_result,
            fact_result=fact_result,
            projection_result=projection_result,
            card_updated=card_updated,
        )
