from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from apps.normalizer.app.persistence import json_from_db, sql_text
from libs.domain.university import UniversityCard

from .models import CardProjectionRecord, CardVersionRecord


class UniversityCardProjectionRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def upsert_card_version(
        self,
        *,
        university_id,
        card_version: int,
        normalizer_version: str,
    ) -> CardVersionRecord:
        result = self._session.execute(
            self._sql_text(
                """
                INSERT INTO core.card_version (
                    university_id,
                    card_version,
                    normalizer_version
                )
                VALUES (
                    :university_id,
                    :card_version,
                    :normalizer_version
                )
                ON CONFLICT (university_id, card_version)
                DO UPDATE SET
                    normalizer_version = EXCLUDED.normalizer_version,
                    generated_at = now()
                RETURNING
                    university_id,
                    card_version,
                    normalizer_version,
                    generated_at
                """
            ),
            {
                "university_id": university_id,
                "card_version": card_version,
                "normalizer_version": normalizer_version,
            },
        )
        return self._card_version_from_row(result.mappings().one())

    def upsert_delivery_projection(
        self,
        *,
        card: UniversityCard,
        generated_at,
    ) -> CardProjectionRecord:
        result = self._session.execute(
            self._sql_text(
                """
                INSERT INTO delivery.university_card (
                    university_id,
                    card_version,
                    card_json,
                    generated_at
                )
                VALUES (
                    :university_id,
                    :card_version,
                    CAST(:card_json AS jsonb),
                    :generated_at
                )
                ON CONFLICT (university_id, card_version)
                DO UPDATE SET
                    card_json = EXCLUDED.card_json,
                    generated_at = EXCLUDED.generated_at
                RETURNING
                    university_id,
                    card_version,
                    card_json,
                    generated_at
                """
            ),
            {
                "university_id": card.university_id,
                "card_version": card.version.card_version,
                "card_json": json.dumps(
                    card.model_dump(mode="json"),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "generated_at": generated_at,
            },
        )
        return self._projection_from_row(result.mappings().one())

    def commit(self) -> None:
        self._session.commit()

    @staticmethod
    def _card_version_from_row(row: Any) -> CardVersionRecord:
        return CardVersionRecord(
            university_id=row["university_id"],
            card_version=row["card_version"],
            normalizer_version=row["normalizer_version"],
            generated_at=row["generated_at"],
        )

    @staticmethod
    def _projection_from_row(row: Any) -> CardProjectionRecord:
        card_payload = json_from_db(row["card_json"])
        return CardProjectionRecord(
            university_id=row["university_id"],
            card_version=row["card_version"],
            card=UniversityCard.model_validate(card_payload),
            generated_at=row["generated_at"],
            metadata={},
        )
