from __future__ import annotations

import re

from apps.normalizer.app.universities.repository import UniversityBootstrapRepository

from .models import UniversityExactMatchCandidate, UniversityExactMatchResult

WHITESPACE_RE = re.compile(r"\s+")


class UniversityExactMatchService:
    def __init__(self, repository: UniversityBootstrapRepository) -> None:
        self._repository = repository

    def match(
        self,
        candidate: UniversityExactMatchCandidate,
    ) -> UniversityExactMatchResult | None:
        canonical_domain = self._normalized_domain(candidate.canonical_domain)
        if canonical_domain is not None:
            university = self._repository.find_university_by_canonical_domain(
                canonical_domain
            )
            if university is not None:
                return UniversityExactMatchResult(
                    university=university,
                    matched_by="canonical_domain",
                    matched_value=canonical_domain,
                )

        canonical_name = self._normalized_name(candidate.canonical_name)
        if canonical_name is None:
            return None

        university = self._repository.find_university_by_canonical_name(canonical_name)
        if university is None:
            return None
        return UniversityExactMatchResult(
            university=university,
            matched_by="canonical_name",
            matched_value=canonical_name,
        )

    @staticmethod
    def _normalized_domain(value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().lower()
        return normalized or None

    @staticmethod
    def _normalized_name(value: str | None) -> str | None:
        if value is None:
            return None
        normalized = WHITESPACE_RE.sub(" ", value).strip()
        return normalized or None
