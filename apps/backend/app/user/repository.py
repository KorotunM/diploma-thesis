from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from apps.backend.app.persistence import sql_text


@dataclass
class FavoriteRecord:
    university_id: UUID
    created_at: datetime


@dataclass
class ComparisonRecord:
    university_id: UUID
    added_at: datetime


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
                "SELECT university_id, created_at FROM core.favorite "
                "WHERE user_id = :user_id ORDER BY created_at DESC"
            ),
            {"user_id": user_id},
        )
        return [
            FavoriteRecord(university_id=r["university_id"], created_at=r["created_at"])
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
