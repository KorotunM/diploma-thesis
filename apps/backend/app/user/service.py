from __future__ import annotations

from uuid import UUID

from .models import (
    ComparisonItem,
    ComparisonResponse,
    FavoriteItem,
    FavoritesResponse,
    SavedSearchCreateRequest,
    SavedSearchItem,
    SavedSearchesResponse,
)
from .repository import UserRepository


class UserService:
    def __init__(self, repository: UserRepository) -> None:
        self._repo = repository

    def get_favorites(self, user_id: UUID) -> FavoritesResponse:
        records = self._repo.list_favorites(user_id)
        return FavoritesResponse(
            items=[
                FavoriteItem(university_id=str(r.university_id), created_at=r.created_at)
                if r.canonical_name is None
                else FavoriteItem(
                    university_id=str(r.university_id),
                    created_at=r.created_at,
                    card_version=r.card_version,
                    canonical_name=r.canonical_name,
                    city=r.city,
                    country_code=r.country_code,
                    website=r.website,
                    logo_url=r.logo_url,
                )
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

    def list_saved_searches(self, user_id: UUID) -> SavedSearchesResponse:
        records = self._repo.list_saved_searches(user_id)
        return SavedSearchesResponse(items=[self._saved_search_item(r) for r in records])

    def create_saved_search(
        self,
        user_id: UUID,
        body: SavedSearchCreateRequest,
    ) -> SavedSearchItem:
        query = body.query.strip()
        city = _clean_optional(body.city)
        country = _clean_optional(body.country)
        source_type = _clean_optional(body.source_type)
        name = _clean_optional(body.name) or _default_saved_search_name(query, city)
        record = self._repo.create_saved_search(
            user_id=user_id,
            name=name,
            query=query,
            filters={
                "city": city,
                "country": country,
                "source_type": source_type,
            },
            page_size=body.page_size,
        )
        return self._saved_search_item(record)

    def delete_saved_search(self, user_id: UUID, saved_search_id: UUID) -> None:
        self._repo.delete_saved_search(user_id, saved_search_id)

    @staticmethod
    def _saved_search_item(record) -> SavedSearchItem:
        filters = record.filters
        return SavedSearchItem(
            saved_search_id=record.saved_search_id,
            name=record.name,
            query=record.query,
            city=filters.get("city"),
            country=filters.get("country"),
            source_type=filters.get("source_type"),
            page_size=record.page_size,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )


def _clean_optional(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _default_saved_search_name(query: str, city: str | None) -> str:
    if query and city:
        return f"{query} / {city}"
    if query:
        return query
    if city:
        return f"Вузы: {city}"
    return "Все вузы"
