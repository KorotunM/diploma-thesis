from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, ConfigDict

from apps.backend.app.persistence import sql_text


class EgeSubjectRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    label: str
    sort_order: int


class EgeSubjectRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def list_all(self) -> list[EgeSubjectRecord]:
        result = self._session.execute(
            self._sql_text(
                """
                SELECT code, label, sort_order
                FROM core.ege_subject
                ORDER BY sort_order, code
                """
            )
        )
        return [
            EgeSubjectRecord(
                code=row["code"],
                label=row["label"],
                sort_order=row["sort_order"],
            )
            for row in result.mappings().all()
        ]
