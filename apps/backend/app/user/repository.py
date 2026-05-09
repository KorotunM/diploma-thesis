from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from apps.backend.app.persistence import json_from_db, sql_text


@dataclass
class FavoriteRecord:
    university_id: UUID
    created_at: datetime
    card_version: int | None = None
    canonical_name: str | None = None
    city: str | None = None
    country_code: str | None = None
    website: str | None = None
    logo_url: str | None = None


@dataclass
class ComparisonRecord:
    university_id: UUID
    added_at: datetime


@dataclass
class SavedSearchRecord:
    saved_search_id: UUID
    user_id: UUID
    name: str
    query: str
    filters: dict[str, Any]
    page_size: int
    created_at: datetime
    updated_at: datetime


class UserRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql = sql_text

    # ── Favorites ──────────────────────────────────────────────────────

    def list_favorites(self, user_id: UUID) -> list[FavoriteRecord]:
        result = self._session.execute(
            self._sql(
                """
                SELECT
                    favorite.university_id,
                    favorite.created_at,
                    search_doc.card_version,
                    search_doc.canonical_name,
                    search_doc.city_name,
                    search_doc.country_code,
                    search_doc.website_url,
                    (search_doc.search_document->>'logo_url') AS logo_url
                FROM core.favorite AS favorite
                LEFT JOIN LATERAL (
                    SELECT
                        university_id,
                        card_version,
                        canonical_name,
                        city_name,
                        country_code,
                        website_url,
                        search_document
                    FROM delivery.university_search_doc
                    WHERE university_id = favorite.university_id
                    ORDER BY card_version DESC
                    LIMIT 1
                ) AS search_doc ON TRUE
                WHERE favorite.user_id = :user_id
                ORDER BY favorite.created_at DESC
                """
            ),
            {"user_id": user_id},
        )
        return [
            FavoriteRecord(
                university_id=r["university_id"],
                created_at=r["created_at"],
                card_version=r["card_version"],
                canonical_name=r["canonical_name"],
                city=r["city_name"],
                country_code=r["country_code"],
                website=r["website_url"],
                logo_url=r["logo_url"],
            )
            for r in result.mappings().all()
        ]

    def is_favorite(self, user_id: UUID, university_id: UUID) -> bool:
        result = self._session.execute(
            self._sql(
                "SELECT 1 FROM core.favorite "
                "WHERE user_id = :user_id AND university_id = :university_id LIMIT 1"
            ),
            {"user_id": user_id, "university_id": university_id},
        )
        return result.one_or_none() is not None

    def add_favorite(self, user_id: UUID, university_id: UUID) -> None:
        self._session.execute(
            self._sql(
                "INSERT INTO core.favorite (user_id, university_id) "
                "VALUES (:user_id, :university_id) ON CONFLICT DO NOTHING"
            ),
            {"user_id": user_id, "university_id": university_id},
        )
        self._session.commit()

    def remove_favorite(self, user_id: UUID, university_id: UUID) -> None:
        self._session.execute(
            self._sql(
                "DELETE FROM core.favorite "
                "WHERE user_id = :user_id AND university_id = :university_id"
            ),
            {"user_id": user_id, "university_id": university_id},
        )
        self._session.commit()

    # ── Comparisons ────────────────────────────────────────────────────

    def list_comparisons(self, user_id: UUID) -> list[ComparisonRecord]:
        result = self._session.execute(
            self._sql(
                "SELECT university_id, added_at FROM core.comparison "
                "WHERE user_id = :user_id ORDER BY added_at DESC"
            ),
            {"user_id": user_id},
        )
        return [
            ComparisonRecord(university_id=r["university_id"], added_at=r["added_at"])
            for r in result.mappings().all()
        ]

    def is_compared(self, user_id: UUID, university_id: UUID) -> bool:
        result = self._session.execute(
            self._sql(
                "SELECT 1 FROM core.comparison "
                "WHERE user_id = :user_id AND university_id = :university_id LIMIT 1"
            ),
            {"user_id": user_id, "university_id": university_id},
        )
        return result.one_or_none() is not None

    def add_comparison(self, user_id: UUID, university_id: UUID) -> None:
        self._session.execute(
            self._sql(
                "INSERT INTO core.comparison (user_id, university_id) "
                "VALUES (:user_id, :university_id) ON CONFLICT DO NOTHING"
            ),
            {"user_id": user_id, "university_id": university_id},
        )
        self._session.commit()

    def remove_comparison(self, user_id: UUID, university_id: UUID) -> None:
        self._session.execute(
            self._sql(
                "DELETE FROM core.comparison "
                "WHERE user_id = :user_id AND university_id = :university_id"
            ),
            {"user_id": user_id, "university_id": university_id},
        )
        self._session.commit()

    # --- Saved searches -------------------------------------------------

    def list_saved_searches(self, user_id: UUID) -> list[SavedSearchRecord]:
        result = self._session.execute(
            self._sql(
                """
                SELECT
                    saved_search_id,
                    user_id,
                    name,
                    query,
                    filters,
                    page_size,
                    created_at,
                    updated_at
                FROM core.saved_search
                WHERE user_id = :user_id
                ORDER BY updated_at DESC, created_at DESC
                """
            ),
            {"user_id": user_id},
        )
        return [self._saved_search_from_row(row) for row in result.mappings().all()]

    def create_saved_search(
        self,
        *,
        user_id: UUID,
        name: str,
        query: str,
        filters: dict[str, Any],
        page_size: int,
    ) -> SavedSearchRecord:
        saved_search_id = uuid.uuid4()
        self._session.execute(
            self._sql(
                """
                INSERT INTO core.saved_search (
                    saved_search_id,
                    user_id,
                    name,
                    query,
                    filters,
                    page_size
                )
                VALUES (
                    :saved_search_id,
                    :user_id,
                    :name,
                    :query,
                    :filters,
                    :page_size
                )
                """
            ),
            {
                "saved_search_id": saved_search_id,
                "user_id": user_id,
                "name": name,
                "query": query,
                "filters": json.dumps(filters, ensure_ascii=False, sort_keys=True),
                "page_size": page_size,
            },
        )
        self._session.commit()
        return self.get_saved_search(user_id=user_id, saved_search_id=saved_search_id)

    def get_saved_search(
        self,
        *,
        user_id: UUID,
        saved_search_id: UUID,
    ) -> SavedSearchRecord:
        result = self._session.execute(
            self._sql(
                """
                SELECT
                    saved_search_id,
                    user_id,
                    name,
                    query,
                    filters,
                    page_size,
                    created_at,
                    updated_at
                FROM core.saved_search
                WHERE user_id = :user_id
                  AND saved_search_id = :saved_search_id
                LIMIT 1
                """
            ),
            {"user_id": user_id, "saved_search_id": saved_search_id},
        )
        row = result.mappings().one()
        return self._saved_search_from_row(row)

    def delete_saved_search(self, user_id: UUID, saved_search_id: UUID) -> None:
        self._session.execute(
            self._sql(
                """
                DELETE FROM core.saved_search
                WHERE user_id = :user_id
                  AND saved_search_id = :saved_search_id
                """
            ),
            {"user_id": user_id, "saved_search_id": saved_search_id},
        )
        self._session.commit()

    @staticmethod
    def _saved_search_from_row(row: Any) -> SavedSearchRecord:
        return SavedSearchRecord(
            saved_search_id=row["saved_search_id"],
            user_id=row["user_id"],
            name=row["name"],
            query=row["query"],
            filters=json_from_db(row["filters"]),
            page_size=row["page_size"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
