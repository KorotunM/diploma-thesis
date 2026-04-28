from __future__ import annotations

import re

from .models import (
    UniversitySearchFilters,
    UniversitySearchHitRecord,
    UniversitySearchResponse,
    UniversitySearchResultItem,
)
from .repository import UniversitySearchRepository

WHITESPACE_RE = re.compile(r"\s+")
DEFAULT_SEARCH_LIMIT = 20
MAX_SEARCH_LIMIT = 50
DEFAULT_SEARCH_PAGE = 1


class UniversitySearchService:
    def __init__(self, repository: UniversitySearchRepository) -> None:
        self._repository = repository

    def search(
        self,
        query: str,
        *,
        city: str | None = None,
        country: str | None = None,
        source_type: str | None = None,
        page: int = DEFAULT_SEARCH_PAGE,
        page_size: int = DEFAULT_SEARCH_LIMIT,
    ) -> UniversitySearchResponse:
        cleaned_query = self._clean_query(query)
        cleaned_city = self._clean_query(city) if city is not None else None
        cleaned_country = self._clean_country(country)
        cleaned_source_type = self._clean_source_type(source_type)
        if cleaned_query is None and not any(
            [cleaned_city, cleaned_country, cleaned_source_type]
        ):
            return UniversitySearchResponse(
                query="",
                total=0,
                page=DEFAULT_SEARCH_PAGE,
                page_size=max(1, min(page_size, MAX_SEARCH_LIMIT)),
                has_more=False,
                filters=UniversitySearchFilters(),
                items=[],
            )

        resolved_page = max(DEFAULT_SEARCH_PAGE, page)
        resolved_page_size = max(1, min(page_size, MAX_SEARCH_LIMIT))
        offset = (resolved_page - 1) * resolved_page_size
        hits = self._repository.search(
            query=cleaned_query,
            normalized_query=cleaned_query.casefold() if cleaned_query else None,
            city=cleaned_city,
            country_code=cleaned_country,
            source_type=cleaned_source_type,
            limit=resolved_page_size,
            offset=offset,
        )
        total = hits[0].total_count if hits else 0
        items = [self._item_from_hit(hit) for hit in hits]
        return UniversitySearchResponse(
            query=cleaned_query or "",
            total=total,
            page=resolved_page,
            page_size=resolved_page_size,
            has_more=(offset + len(items)) < total,
            filters=UniversitySearchFilters(
                city=cleaned_city,
                country=cleaned_country,
                source_type=cleaned_source_type,
            ),
            items=items,
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

    @staticmethod
    def _clean_country(country: str | None) -> str | None:
        if country is None:
            return None
        cleaned = WHITESPACE_RE.sub(" ", country).strip().upper()
        return cleaned or None

    @staticmethod
    def _clean_source_type(source_type: str | None) -> str | None:
        if source_type is None:
            return None
        cleaned = WHITESPACE_RE.sub(" ", source_type).strip().lower()
        return cleaned or None
