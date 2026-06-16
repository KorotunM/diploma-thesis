from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from apps.backend.app.persistence import sql_text

GUEST_TRIAL_LIMIT = 5


class AiChatLimitExceededError(RuntimeError):
    pass


@dataclass(frozen=True)
class AiChatUsageResult:
    remaining: int | None


class AiChatUsageRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def record_request(
        self,
        *,
        user_id: UUID | None,
        client_id: str,
        trial_limit: int = GUEST_TRIAL_LIMIT,
    ) -> AiChatUsageResult:
        if user_id is not None:
            self._record_user_request(user_id)
            self._session.commit()
            return AiChatUsageResult(remaining=None)

        normalized_client_id = client_id.strip()[:120] or "anonymous"
        used_count = self._record_client_request(normalized_client_id)
        self._session.commit()
        if used_count > trial_limit:
            raise AiChatLimitExceededError(
                "Бесплатные пробные запросы ИИ закончились. "
                "Войдите в аккаунт, чтобы продолжить подбор."
            )
        return AiChatUsageResult(remaining=max(0, trial_limit - used_count))

    def _record_user_request(self, user_id: UUID) -> int:
        result = self._session.execute(
            self._sql_text(
                """
                INSERT INTO core.ai_chat_usage (user_id, used_on, request_count)
                VALUES (:user_id, CURRENT_DATE, 1)
                ON CONFLICT (user_id, used_on) DO UPDATE
                SET
                    request_count = core.ai_chat_usage.request_count + 1,
                    updated_at = now()
                RETURNING request_count
                """
            ),
            {"user_id": user_id},
        )
        return int(result.scalar_one())

    def _record_client_request(self, client_id: str) -> int:
        result = self._session.execute(
            self._sql_text(
                """
                INSERT INTO core.ai_chat_usage (client_id, used_on, request_count)
                VALUES (:client_id, CURRENT_DATE, 1)
                ON CONFLICT (client_id, used_on) DO UPDATE
                SET
                    request_count = core.ai_chat_usage.request_count + 1,
                    updated_at = now()
                RETURNING request_count
                """
            ),
            {"client_id": client_id},
        )
        return int(result.scalar_one())
