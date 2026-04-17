from fastapi import status

from libs.contracts.events import CrawlRequestEvent, CrawlRequestPayload, EventHeader
from libs.observability import create_service_app

app = create_service_app(
    service_name="scheduler",
    description="Plans crawl runs and publishes crawl.request events.",
)


@app.get("/", tags=["scheduler"])
def scheduler_overview() -> dict[str, object]:
    return {
        "service": "scheduler",
        "responsibilities": [
            "source registry",
            "crawl policy",
            "job planning",
            "manual trigger",
            "freshness tracking",
        ],
    }


@app.post(
    "/admin/v1/crawl-jobs",
    response_model=CrawlRequestEvent,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["scheduler"],
)
def create_crawl_job(payload: CrawlRequestPayload) -> CrawlRequestEvent:
    return CrawlRequestEvent(header=EventHeader(producer="scheduler"), payload=payload)
