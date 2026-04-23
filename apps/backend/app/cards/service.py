from __future__ import annotations

from uuid import UUID

from libs.domain.university import UniversityCard

from .repository import UniversityCardReadRepository


class UniversityCardNotFoundError(LookupError):
    def __init__(self, university_id: UUID) -> None:
        super().__init__(f"University card {university_id} was not found.")
        self.university_id = university_id


class UniversityCardReadService:
    def __init__(self, repository: UniversityCardReadRepository) -> None:
        self._repository = repository

    def get_latest_card(self, university_id: UUID) -> UniversityCard:
        record = self._repository.get_latest_by_university_id(university_id)
        if record is None:
            raise UniversityCardNotFoundError(university_id)
        return record.card
