from __future__ import annotations

import logging

from apps.normalizer.app.claims import ClaimRecord
from apps.normalizer.app.universities import UniversityBootstrapResult

from .models import RankingHistoryRecord
from .repository import RankingHistoryRepository

_log = logging.getLogger(__name__)

RANKING_SOURCE_KEYS = frozenset({"tabiturient-globalrating"})
DEFAULT_PROVIDER = "Tabiturient"


def _first_claim_value(claims: list[ClaimRecord], field_name: str) -> object:
    for claim in claims:
        if claim.field_name == field_name:
            return claim.value
    return None


class RankingHistoryService:
    def __init__(self, repository: RankingHistoryRepository) -> None:
        self._repository = repository

    def write_from_bootstrap(
        self,
        bootstrap_result: UniversityBootstrapResult,
        *,
        source_key: str,
    ) -> None:
        if source_key not in RANKING_SOURCE_KEYS:
            return

        claims = bootstrap_result.claims_used
        rank_raw = _first_claim_value(claims, "ratings.rank")
        year_raw = _first_claim_value(claims, "ratings.year")
        canonical_name_raw = _first_claim_value(claims, "canonical_name")

        if rank_raw is None or year_raw is None or canonical_name_raw is None:
            _log.debug(
                "Skipping ranking history write — missing rank/year/name "
                "for university_id=%s source_key=%s",
                bootstrap_result.university.university_id,
                source_key,
            )
            return

        try:
            rank = int(rank_raw)
            year = int(year_raw)
        except (TypeError, ValueError):
            _log.warning(
                "Invalid rank/year values rank=%r year=%r — skipping",
                rank_raw,
                year_raw,
            )
            return

        score_raw = _first_claim_value(claims, "ratings.value")
        score: float | None = None
        if score_raw is not None:
            try:
                score = float(str(score_raw).replace(",", "."))
            except ValueError:
                pass

        change_direction_raw = _first_claim_value(claims, "ratings.change.direction")
        change_direction = str(change_direction_raw) if change_direction_raw else None

        change_delta_raw = _first_claim_value(claims, "ratings.change.delta")
        change_delta: int = 0
        if change_delta_raw is not None:
            try:
                change_delta = int(change_delta_raw)
            except (TypeError, ValueError):
                pass

        external_id: str | None = None
        for claim in claims:
            if claim.field_name == "ratings.rank":
                ext = claim.metadata.get("external_id")
                if isinstance(ext, str):
                    external_id = ext
                break

        record = RankingHistoryRecord(
            university_id=bootstrap_result.university.university_id,
            source_key=source_key,
            provider=DEFAULT_PROVIDER,
            year=year,
            rank=rank,
            score=score,
            category=str(_first_claim_value(claims, "ratings.category") or ""),
            change_direction=change_direction,
            change_delta=change_delta,
            canonical_name=str(canonical_name_raw),
            external_id=external_id,
        )

        try:
            self._repository.upsert(record)
        except Exception:
            _log.exception(
                "Failed to upsert ranking history for university_id=%s rank=%d year=%d",
                bootstrap_result.university.university_id,
                rank,
                year,
            )
