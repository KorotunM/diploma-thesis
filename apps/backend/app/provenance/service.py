from __future__ import annotations

from uuid import UUID

from .models import UniversityProvenanceTrace
from .repository import UniversityProvenanceRepository


class UniversityProvenanceNotFoundError(LookupError):
    def __init__(self, university_id: UUID) -> None:
        super().__init__(f"University provenance {university_id} was not found.")
        self.university_id = university_id


class UniversityProvenanceReadService:
    def __init__(self, repository: UniversityProvenanceRepository) -> None:
        self._repository = repository

    def get_latest_trace(self, university_id: UUID) -> UniversityProvenanceTrace:
        projection = self._repository.get_latest_projection_context(university_id)
        if projection is None:
            raise UniversityProvenanceNotFoundError(university_id)

        return UniversityProvenanceTrace(
            university_id=university_id,
            delivery_projection=projection,
            resolved_facts=self._repository.list_resolved_facts(
                university_id=university_id,
                card_version=projection.card_version,
            ),
            claims=self._repository.list_claims_for_card(
                university_id=university_id,
                card_version=projection.card_version,
            ),
            claim_evidence=self._repository.list_claim_evidence_for_card(
                university_id=university_id,
                card_version=projection.card_version,
            ),
            parsed_documents=self._repository.list_parsed_documents_for_card(
                university_id=university_id,
                card_version=projection.card_version,
            ),
            raw_artifacts=self._repository.list_raw_artifacts_for_card(
                university_id=university_id,
                card_version=projection.card_version,
            ),
        )
