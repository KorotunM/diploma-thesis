from __future__ import annotations

import re

from .models import LocationSuggestResponse
from .repository import LocationSuggestRepository

WHITESPACE_RE = re.compile(r"\s+")
MAX_SUGGEST_LIMIT = 20
MAX_GEO_TREE_LIMIT = 100


class LocationSuggestService:
    def __init__(self, repository: LocationSuggestRepository) -> None:
        self._repository = repository

    def suggest_regions(self, q: str) -> LocationSuggestResponse:
        cleaned = self._clean(q)
        items = self._repository.suggest_regions(cleaned, MAX_GEO_TREE_LIMIT if not cleaned else MAX_SUGGEST_LIMIT)
        return LocationSuggestResponse(items=items)

    def suggest_cities(self, q: str) -> LocationSuggestResponse:
        cleaned = self._clean(q)
        if not cleaned:
            return LocationSuggestResponse(items=[])
        items = self._repository.suggest_cities(cleaned, MAX_SUGGEST_LIMIT)
        return LocationSuggestResponse(items=items)

    def suggest_cities_by_region(self, region: str) -> LocationSuggestResponse:
        cleaned = self._clean(region)
        if not cleaned:
            return LocationSuggestResponse(items=[])
        items = self._repository.suggest_cities_by_region(cleaned, MAX_GEO_TREE_LIMIT)
        return LocationSuggestResponse(items=items)

    @staticmethod
    def _clean(q: str) -> str:
        return WHITESPACE_RE.sub(" ", q).strip()
