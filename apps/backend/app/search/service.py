from __future__ import annotations

import re

from .models import (
    UniversitySearchHitRecord,
    UniversitySearchResponse,
    UniversitySearchResultItem,
)
from .repository import UniversitySearchRepository

WHITESPACE_RE = re.compile(r"\s+")
DEFAULT_SEARCH_LIMIT = 20
MAX_SEARCH_LIMIT = 50


class UniversitySearchService:
    def __init__(self, repository: UniversitySearchRepository) -> None:
        self._repository = repository

    def search(
        self,
        query: str,
        *,
        limit: int = DEFAULT_SEARCH_LIMIT,
    ) -> UniversitySearchResponse:
        cleaned_query = self._clean_query(query)
        if cleaned_query is None:
            return UniversitySearchResponse(query="", total=0, items=[])

        capped_limit = max(1, min(limit, MAX_SEARCH_LIMIT))
        hits = self._repository.search(
            query=cleaned_query,
            normalized_query=cleaned_query.casefold(),
            limit=capped_limit,
        )
        return UniversitySearchResponse(
            query=cleaned_query,
            total=len(hits),
            items=[self._item_from_hit(hit) for hit in hits],
        )

    def _item_from_hit(
        self,
        hit: UniversitySearchHitRecord,
    ) -> UniversitySearchResultItem:
        return UniversitySearchResultItem(
            university_id=hit.university_id,
            card_version=hit.card_version,
            canonical_name=hit.canonical_name,
            city=hit.city_name,
            country_code=hit.country_code,
            website=hit.website_url,
            aliases=hit.aliases,
            score=round(hit.combined_score, 6),
            match_signals=self._match_signals(hit),
        )

    @staticmethod
    def _match_signals(hit: UniversitySearchHitRecord) -> list[str]:
        signals: list[str] = []
        if hit.text_rank > 0:
            signals.append("full_text")
        if hit.trigram_score > 0:
            signals.append("trigram")
        return signals

    @staticmethod
    def _clean_query(query: str) -> str | None:
        cleaned = WHITESPACE_RE.sub(" ", query).strip()
        return cleaned or None
