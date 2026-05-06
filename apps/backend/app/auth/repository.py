from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from apps.backend.app.persistence import sql_text


@dataclass
class UserRecord:
    user_id: UUID
    email: str
    password_hash: str
    display_name: str | None
    created_at: datetime


class AuthRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql = sql_text

    def find_user_by_email(self, email: str) -> UserRecord | None:
        result = self._session.execute(
            self._sql(
                "SELECT user_id, email, password_hash, display_name, created_at "
                "FROM core.user WHERE email = :email LIMIT 1"
            ),
            {"email": email.lower()},
        )
        row = result.mappings().one_or_none()
        return self._user_from_row(row) if row else None

    def find_user_by_id(self, user_id: UUID) -> UserRecord | None:
        result = self._session.execute(
            self._sql(
                "SELECT user_id, email, password_hash, display_name, created_at "
                "FROM core.user WHERE user_id = :user_id LIMIT 1"
            ),
            {"user_id": user_id},
        )
        row = result.mappings().one_or_none()
        return self._user_from_row(row) if row else None

    def find_user_by_token(self, token: str) -> UserRecord | None:
        result = self._session.execute(
            self._sql(
                "SELECT u.user_id, u.email, u.password_hash, u.display_name, u.created_at "
                "FROM core.user u "
                "JOIN core.user_session s ON s.user_id = u.user_id "
                "WHERE s.token = :token "
                "AND (s.expires_at IS NULL OR s.expires_at > now()) "
                "LIMIT 1"
            ),
            {"token": token},
        )
        row = result.mappings().one_or_none()
        return self._user_from_row(row) if row else None

    def create_user(
        self,
        *,
        email: str,
        password_hash: str,
        display_name: str | None,
    ) -> UserRecord:
        user_id = uuid.uuid4()
        self._session.execute(
            self._sql(
                "INSERT INTO core.user (user_id, email, password_hash, display_name) "
                "VALUES (:user_id, :email, :password_hash, :display_name)"
            ),
            {
                "user_id": user_id,
                "email": email.lower(),
                "password_hash": password_hash,
                "display_name": display_name,
            },
        )
        self._session.commit()
        user = self.find_user_by_id(user_id)
        assert user is not None
        return user

    def create_session(self, user_id: UUID) -> str:
        session_id = uuid.uuid4()
        token = uuid.uuid4()
        self._session.execute(
            self._sql(
                "INSERT INTO core.user_session (session_id, user_id, token) "
                "VALUES (:session_id, :user_id, :token)"
            ),
            {"session_id": session_id, "user_id": user_id, "token": token},
        )
        self._session.commit()
        return str(token)

    def delete_session_by_token(self, token: str) -> None:
        self._session.execute(
            self._sql("DELETE FROM core.user_session WHERE token = :token"),
            {"token": token},
        )
        self._session.commit()

    @staticmethod
    def _user_from_row(row: Any) -> UserRecord:
        return UserRecord(
            user_id=row["user_id"],
            email=row["email"],
            password_hash=row["password_hash"],
            display_name=row["display_name"],
            created_at=row["created_at"],
        )
