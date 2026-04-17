from uuid import UUID

from libs.domain.university import UniversityCard
from libs.observability import create_service_app

app = create_service_app(
    service_name="backend",
    description="Serves delivery projections and provenance traces to the UI.",
)


@app.get("/", tags=["backend"])
def backend_overview() -> dict[str, object]:
    return {
        "service": "backend",
        "public_endpoints": ["/api/v1/search", "/api/v1/universities/{university_id}"],
        "internal_endpoints": ["/api/v1/universities/{university_id}/provenance"],
    }


@app.get("/api/v1/search", tags=["backend"])
def search_universities(query: str = "") -> dict[str, object]:
    card = UniversityCard.sample()
    return {
        "query": query,
        "total": 1,
        "items": [
            {
                "university_id": str(card.university_id),
                "canonical_name": card.canonical_name.value,
                "city": card.location.city,
                "website": card.contacts.website,
            }
        ],
    }


@app.get("/api/v1/universities/{university_id}", response_model=UniversityCard, tags=["backend"])
def get_university_card(university_id: UUID) -> UniversityCard:
    return UniversityCard.sample().model_copy(update={"university_id": university_id})


@app.get("/api/v1/universities/{university_id}/provenance", tags=["backend"])
def get_university_provenance(university_id: UUID) -> dict[str, object]:
    return {
        "university_id": str(university_id),
        "chain": ["raw", "parsed", "claims", "resolved_facts", "delivery_projection"],
        "note": "Placeholder provenance endpoint. Replace with real trace stitching from normalization data.",
    }
