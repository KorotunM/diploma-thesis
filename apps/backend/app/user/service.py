from __future__ import annotations

from uuid import UUID

from .models import ComparisonItem, ComparisonResponse, FavoriteItem, FavoritesResponse
from .repository import UserRepository


class UserService:
    def __init__(self, repository: UserRepository) -> None:
        self._repo = repository

    def get_favorites(self, user_id: UUID) -> FavoritesResponse:
        records = self._repo.list_favorites(user_id)
        return FavoritesResponse(
            items=[
                FavoriteItem(university_id=str(r.university_id), created_at=r.created_at)
                for r in records
            ]
        )

    def add_favorite(self, user_id: UUID, university_id: UUID) -> None:
        self._repo.add_favorite(user_id, university_id)

    def remove_favorite(self, user_id: UUID, university_id: UUID) -> None:
        self._repo.remove_favorite(user_id, university_id)

    def is_favorite(self, user_id: UUID, university_id: UUID) -> bool:
        return self._repo.is_favorite(user_id, university_id)

    def get_comparisons(self, user_id: UUID) -> ComparisonResponse:
        records = self._repo.list_comparisons(user_id)
        return ComparisonResponse(
            items=[
                ComparisonItem(university_id=str(r.university_id), added_at=r.added_at)
                for r in records
            ]
        )

    def add_comparison(self, user_id: UUID, university_id: UUID) -> None:
        self._repo.add_comparison(user_id, university_id)

    def remove_comparison(self, user_id: UUID, university_id: UUID) -> None:
        self._repo.remove_comparison(user_id, university_id)

    def is_compared(self, user_id: UUID, university_id: UUID) -> bool:
        return self._repo.is_compared(user_id, university_id)
