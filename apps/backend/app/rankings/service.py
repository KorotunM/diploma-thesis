from __future__ import annotations

from .models import RankingItem, RankingsResponse
from .repository import RankingsRepository

DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 50


def _category(score: float) -> str:
    if score >= 150:
        return "А+"
    if score >= 120:
        return "А"
    if score >= 90:
        return "B+"
    if score >= 60:
        return "B"
    return "C"


def _trend(direction: str | None, delta: int | None) -> str:
    if direction == "up" and delta and int(delta) > 0:
        return "up"
    if direction == "down" and delta and int(delta) > 0:
        return "down"
    return "neutral"


class RankingsService:
    def __init__(self, repository: RankingsRepository) -> None:
        self._repository = repository

    def get_rankings(
        self,
        *,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> RankingsResponse:
        resolved_page = max(1, page)
        resolved_size = max(1, min(page_size, MAX_PAGE_SIZE))
        offset = (resolved_page - 1) * resolved_size

        rows = self._repository.fetch_ranked(limit=resolved_size, offset=offset)
        total = int(rows[0]["total"]) if rows else 0

        items = [
            RankingItem(
                rank=int(row["rank"]),
                trend=_trend(row.get("change_direction"), row.get("change_delta")),
                trend_delta=int(row["change_delta"]) if row.get("change_delta") else 0,
                university_id=str(row["university_id"]),
                canonical_name=row["canonical_name"],
                logo_url=row.get("logo_url"),
                city=row.get("city_name"),
                region=row.get("region_name"),
                composite_score=round(float(row["composite_score"]), 2),
                category=_category(float(row["composite_score"])),
            )
            for row in rows
        ]

        return RankingsResponse(
            total=total,
            page=resolved_page,
            page_size=resolved_size,
            has_more=(offset + len(items)) < total,
            items=items,
        )
