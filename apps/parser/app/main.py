from fastapi import status

from libs.contracts.events import (
    CrawlRequestEvent,
    EventHeader,
    ParseCompletedEvent,
    ParseCompletedPayload,
)
from libs.observability import create_service_app

app = create_service_app(
    service_name="parser",
    description="Fetches source data, stores raw artifacts and emits parsed snapshots.",
)


@app.get("/", tags=["parser"])
def parser_overview() -> dict[str, object]:
    return {
        "service": "parser",
        "adapter_families": ["official_sites", "aggregators", "rankings"],
        "pipeline_steps": ["fetch", "store_raw", "extract", "map_to_intermediate"],
    }


@app.post(
    "/internal/v1/events/crawl-request",
    response_model=ParseCompletedEvent,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["parser"],
)
def handle_crawl_request(event: CrawlRequestEvent) -> ParseCompletedEvent:
    payload = ParseCompletedPayload(
        crawl_run_id=event.payload.crawl_run_id,
        source_key=event.payload.source_key,
        endpoint_url=event.payload.endpoint_url,
        parser_version="parser.stub.0.1.0",
        extracted_fragments=3,
        metadata={"note": "stub parser completed event"},
    )
    return ParseCompletedEvent(
        header=EventHeader(
            producer="parser",
            trace_id=event.header.trace_id or event.header.event_id,
        ),
        payload=payload,
    )
