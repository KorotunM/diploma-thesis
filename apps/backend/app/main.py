from uuid import UUID

from fastapi import Depends, HTTPException, status

from apps.backend.app.cards import UniversityCardNotFoundError, UniversityCardReadService
from apps.backend.app.dependencies import (
    get_university_card_read_service,
    get_university_provenance_read_service,
)
from apps.backend.app.provenance import (
    UniversityProvenanceNotFoundError,
    UniversityProvenanceReadService,
    UniversityProvenanceTrace,
)
from libs.domain.university import UniversityCard
from libs.observability import create_service_app

app = create_service_app(
    service_name="backend",
    description="Serves delivery projections and provenance traces to the UI.",
)

CARD_READ_SERVICE_DEPENDENCY = Depends(get_university_card_read_service)
PROVENANCE_READ_SERVICE_DEPENDENCY = Depends(get_university_provenance_read_service)


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


@app.get(
    "/api/v1/universities/{university_id}",
    response_model=UniversityCard,
    tags=["backend"],
)
def get_university_card(
    university_id: UUID,
    service: UniversityCardReadService = CARD_READ_SERVICE_DEPENDENCY,
) -> UniversityCard:
    try:
        return service.get_latest_card(university_id)
    except UniversityCardNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"University card {university_id} was not found.",
        ) from exc


@app.get(
    "/api/v1/universities/{university_id}/provenance",
    response_model=UniversityProvenanceTrace,
    tags=["backend"],
)
def get_university_provenance(
    university_id: UUID,
    service: UniversityProvenanceReadService = PROVENANCE_READ_SERVICE_DEPENDENCY,
) -> UniversityProvenanceTrace:
    try:
        return service.get_latest_trace(university_id)
    except UniversityProvenanceNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"University provenance {university_id} was not found.",
        ) from exc
