from __future__ import annotations

import re

from apps.normalizer.app.universities.repository import UniversityBootstrapRepository

from .models import UniversityMatchCandidate, UniversityMatchDecision

WHITESPACE_RE = re.compile(r"\s+")


class UniversityMatchService:
    def __init__(
        self,
        repository: UniversityBootstrapRepository,
        *,
        trigram_auto_match_threshold: float = 0.93,
        trigram_review_threshold: float = 0.78,
        trigram_ambiguity_gap: float = 0.04,
        trigram_candidate_limit: int = 5,
    ) -> None:
        self._repository = repository
        self._trigram_auto_match_threshold = trigram_auto_match_threshold
        self._trigram_review_threshold = trigram_review_threshold
        self._trigram_ambiguity_gap = trigram_ambiguity_gap
        self._trigram_candidate_limit = trigram_candidate_limit

    def match(
        self,
        candidate: UniversityMatchCandidate,
    ) -> UniversityMatchDecision:
        canonical_domain = self._normalized_domain(candidate.canonical_domain)
        if canonical_domain is not None:
            university = self._repository.find_university_by_canonical_domain(
                canonical_domain
            )
            if university is not None:
                return UniversityMatchDecision(
                    status="matched",
                    university=university,
                    matched_by="canonical_domain",
                    matched_value=canonical_domain,
                    strategy="exact",
                )

        canonical_name = self._normalized_name(candidate.canonical_name)
        if canonical_name is None:
            return UniversityMatchDecision(status="unmatched")

        university = self._repository.find_university_by_canonical_name(canonical_name)
        if university is not None:
            return UniversityMatchDecision(
                status="matched",
                university=university,
                matched_by="canonical_name",
                matched_value=canonical_name,
                strategy="exact",
            )

        candidates = self._repository.find_universities_by_canonical_name_similarity(
            canonical_name,
            threshold=self._trigram_review_threshold,
            limit=self._trigram_candidate_limit,
        )
        if not candidates:
            return UniversityMatchDecision(status="unmatched")

        top_candidate = candidates[0]
        if self._is_confident_trigram_match(candidates):
            return UniversityMatchDecision(
                status="matched",
                university=top_candidate.university,
                matched_by="canonical_name",
                matched_value=canonical_name,
                strategy="trigram",
                similarity_score=top_candidate.similarity_score,
                review_candidates=candidates,
            )

        return UniversityMatchDecision(
            status="review_required",
            matched_by="canonical_name",
            matched_value=canonical_name,
            strategy="trigram",
            similarity_score=top_candidate.similarity_score,
            review_candidates=candidates,
        )

    def _is_confident_trigram_match(self, candidates) -> bool:
        top_candidate = candidates[0]
        if top_candidate.similarity_score < self._trigram_auto_match_threshold:
            return False
        if len(candidates) == 1:
            return True
        score_gap = top_candidate.similarity_score - candidates[1].similarity_score
        return score_gap >= self._trigram_ambiguity_gap

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


class UniversityExactMatchService(UniversityMatchService):
    pass
