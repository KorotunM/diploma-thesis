from uuid import uuid4

from fastapi import status

from libs.contracts.events import (
    CardUpdatedEvent,
    CardUpdatedPayload,
    EventHeader,
    NormalizeRequestEvent,
    NormalizeRequestPayload,
    ParseCompletedEvent,
)
from libs.observability import create_service_app

app = create_service_app(
    service_name="normalizer",
    description="Consolidates claims, resolves facts and builds delivery projections.",
)


@app.get("/", tags=["normalizer"])
def normalizer_overview() -> dict[str, object]:
    return {
        "service": "normalizer",
        "stages": ["field normalization", "blocking", "matching", "clustering", "resolution"],
        "delivery_targets": ["delivery.university_card", "delivery.university_search_doc"],
    }


@app.post(
    "/internal/v1/events/parse-completed",
    response_model=NormalizeRequestEvent,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["normalizer"],
)
def prepare_normalization(event: ParseCompletedEvent) -> NormalizeRequestEvent:
    payload = NormalizeRequestPayload(
        crawl_run_id=event.payload.crawl_run_id,
        source_key=event.payload.source_key,
        parsed_document_id=event.payload.parsed_document_id,
        parser_version=event.payload.parser_version,
        normalizer_version="normalizer.stub.0.1.0",
        metadata={"note": "stub normalize request"},
    )
    return NormalizeRequestEvent(
        header=EventHeader(
            producer="normalizer",
            trace_id=event.header.trace_id or event.header.event_id,
        ),
        payload=payload,
    )


@app.post(
    "/internal/v1/events/normalize-request",
    response_model=CardUpdatedEvent,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["normalizer"],
)
def build_card(event: NormalizeRequestEvent) -> CardUpdatedEvent:
    payload = CardUpdatedPayload(
        university_id=uuid4(),
        card_version=1,
        updated_fields=["canonical_name", "contacts.website", "location.city"],
    )
    return CardUpdatedEvent(
        header=EventHeader(
            producer="normalizer",
            trace_id=event.header.trace_id or event.header.event_id,
        ),
        payload=payload,
    )
