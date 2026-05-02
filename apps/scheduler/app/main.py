from apps.scheduler.app.discovery.routes import router as discovery_router
from apps.scheduler.app.freshness.routes import router as freshness_router
from apps.scheduler.app.runs.routes import router as pipeline_run_router
from apps.scheduler.app.sources.routes import router as source_registry_router
from libs.observability import create_service_app

app = create_service_app(
    service_name="scheduler",
    description="Plans crawl runs and publishes crawl.request events.",
)
app.include_router(source_registry_router)
app.include_router(pipeline_run_router)
app.include_router(freshness_router)
app.include_router(discovery_router)


@app.get("/", tags=["scheduler"])
def scheduler_overview() -> dict[str, object]:
    return {
        "service": "scheduler",
        "responsibilities": [
            "source registry",
            "crawl policy",
            "job planning",
            "manual trigger",
            "scheduled trigger",
            "freshness tracking",
            "endpoint discovery",
        ],
    }

