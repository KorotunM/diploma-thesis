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
ALLOWED_SORTS = {"rating", "budget_places", "avg_passing_score"}
POPULAR_DIRECTIONS: dict[str, list[str]] = {
    "it и цифровые технологии": [
        "01.03.02",
        "02.03.02",
        "02.03.03",
        "09.03.01",
        "09.03.02",
        "09.03.03",
        "09.03.04",
        "10.03.01",
    ],
    "инженерия": [
        "08.03.01",
        "11.03.01",
        "11.03.02",
        "12.03.01",
        "13.03.01",
        "13.03.02",
        "15.03.01",
        "15.03.04",
        "15.03.06",
        "27.03.04",
    ],
    "экономика": ["38.03.01", "38.03.05", "38.03.06", "38.03.07"],
    "медицина": [
        "31.05.01",
        "31.05.02",
        "31.05.03",
        "32.05.01",
        "33.05.01",
        "34.03.01",
    ],
    "управление": ["38.03.02", "38.03.03", "38.03.04", "38.03.05", "27.03.05"],
    "гуманитарные науки": [
        "37.03.01",
        "39.03.01",
        "39.03.02",
        "40.03.01",
        "41.03.01",
        "41.03.05",
        "42.03.01",
        "42.03.02",
        "44.03.01",
        "45.03.01",
        "45.03.02",
        "46.03.01",
    ],
}
SUBJECT_LABEL_BY_CODE = {
    "russian": "Русский язык",
    "math": "Математика",
    "physics": "Физика",
    "chemistry": "Химия",
    "biology": "Биология",
    "informatics": "Информатика",
    "social": "Обществознание",
    "history": "История",
    "literature": "Литература",
    "geography": "География",
    "foreign": "Иностранный язык",
}
SUBJECT_CODE_BY_LABEL = {
    re.sub(r"\s+", "", label).casefold(): code
    for code, label in SUBJECT_LABEL_BY_CODE.items()
}


def _rating_category(score: float | None, explicit_category: str | None = None) -> str | None:
    if explicit_category:
        return explicit_category
    if score is None:
        return None
    if score >= 150:
        return "А+"
    if score >= 120:
        return "А"
    if score >= 90:
        return "B+"
    if score >= 60:
        return "B"
    return "C"


class UniversitySearchService:
    def __init__(self, repository: UniversitySearchRepository) -> None:
        self._repository = repository

    def search(
        self,
        query: str,
        *,
        city: str | None = None,
        region: str | None = None,
        country: str | None = None,
        source_type: str | None = None,
        ege_subjects: list[str] | None = None,
        ege_scores: dict[str, int] | None = None,
        program_codes: list[str] | None = None,
        dormitory: bool = False,
        military_department: bool = False,
        sort_by: str = "rating",
        page: int = DEFAULT_SEARCH_PAGE,
        page_size: int = DEFAULT_SEARCH_LIMIT,
    ) -> UniversitySearchResponse:
        cleaned_query = self._clean_query(query)
        cleaned_city = self._clean_query(city) if city is not None else None
        cleaned_region = self._clean_query(region) if region is not None else None
        cleaned_country = self._clean_country(country)
        cleaned_source_type = self._clean_source_type(source_type)
        cleaned_ege_subjects = self._clean_ege_subjects(ege_subjects)
        cleaned_ege_scores = self._clean_ege_scores(ege_scores)
        cleaned_program_codes = self._clean_program_codes(program_codes)
        if not cleaned_program_codes:
            cleaned_program_codes = self._popular_direction_codes(cleaned_query)
        cleaned_sort_by = sort_by if sort_by in ALLOWED_SORTS else "rating"
        resolved_page = max(DEFAULT_SEARCH_PAGE, page)
        resolved_page_size = max(1, min(page_size, MAX_SEARCH_LIMIT))
        offset = (resolved_page - 1) * resolved_page_size
        hits = self._repository.search(
            query=cleaned_query,
            normalized_query=cleaned_query.casefold() if cleaned_query else None,
            city=cleaned_city,
            region=cleaned_region,
            country_code=cleaned_country,
            source_type=cleaned_source_type,
            ege_subjects=cleaned_ege_subjects or None,
            ege_scores=(
                self._repository_ege_scores(cleaned_ege_scores)
                if cleaned_ege_scores
                else None
            ),
            program_codes=cleaned_program_codes or None,
            dormitory=dormitory,
            military_department=military_department,
            sort_by=cleaned_sort_by,
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
                region=cleaned_region,
                country=cleaned_country,
                source_type=cleaned_source_type,
                ege_subjects=cleaned_ege_subjects,
                ege_scores=cleaned_ege_scores,
                program_codes=cleaned_program_codes,
                dormitory=dormitory,
                military_department=military_department,
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
            region=hit.region_name,
            country_code=hit.country_code,
            website=hit.website_url,
            logo_url=hit.logo_url,
            aliases=hit.aliases,
            score=round(hit.combined_score, 6),
            rating_score=round(hit.rating_score, 2) if hit.rating_score is not None else None,
            rating_category=_rating_category(hit.rating_score, hit.rating_category),
            budget_places=hit.budget_places,
            paid_places=hit.paid_places,
            avg_passing_score=(
                round(hit.avg_passing_score, 1) if hit.avg_passing_score is not None else None
            ),
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

    @staticmethod
    def _clean_ege_subjects(subjects: list[str] | None) -> list[str]:
        if not subjects:
            return []
        seen: set[str] = set()
        result: list[str] = []
        for s in subjects:
            cleaned = UniversitySearchService._canonical_subject_label(s)
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                result.append(cleaned)
        return result

    @staticmethod
    def _clean_ege_scores(scores: dict[str, int] | None) -> dict[str, int]:
        if not scores:
            return {}
        result: dict[str, int] = {}
        for subject, score in scores.items():
            label = UniversitySearchService._canonical_subject_label(subject)
            if not label:
                continue
            try:
                parsed = int(score)
            except (TypeError, ValueError):
                continue
            if 0 <= parsed <= 100:
                result[label] = parsed
        return result

    @staticmethod
    def _canonical_subject_label(subject: str | None) -> str | None:
        if not subject:
            return None
        cleaned = WHITESPACE_RE.sub("", subject).strip().casefold()
        if not cleaned:
            return None
        if cleaned in SUBJECT_LABEL_BY_CODE:
            return SUBJECT_LABEL_BY_CODE[cleaned]
        code = SUBJECT_CODE_BY_LABEL.get(cleaned)
        if code:
            return SUBJECT_LABEL_BY_CODE[code]
        return subject.strip()

    @staticmethod
    def _subject_aliases(subject: str) -> list[str]:
        normalized = WHITESPACE_RE.sub("", subject).casefold()
        aliases = {normalized}
        code = SUBJECT_CODE_BY_LABEL.get(normalized)
        if code:
            aliases.add(code)
        elif normalized in SUBJECT_LABEL_BY_CODE:
            aliases.add(
                WHITESPACE_RE.sub("", SUBJECT_LABEL_BY_CODE[normalized]).casefold()
            )
        return sorted(aliases)

    @staticmethod
    def _repository_ege_scores(scores: dict[str, int]) -> list[dict[str, object]]:
        return [
            {
                "subject": subject,
                "aliases": UniversitySearchService._subject_aliases(subject),
                "score": score,
            }
            for subject, score in sorted(scores.items())
        ]

    @staticmethod
    def _clean_program_codes(program_codes: list[str] | None) -> list[str]:
        if not program_codes:
            return []
        seen: set[str] = set()
        result: list[str] = []
        for code in program_codes:
            cleaned = WHITESPACE_RE.sub("", code).strip()
            if re.fullmatch(r"\d{2}\.\d{2}\.\d{2}", cleaned) and cleaned not in seen:
                seen.add(cleaned)
                result.append(cleaned)
        return result

    @staticmethod
    def _popular_direction_codes(query: str | None) -> list[str]:
        if not query:
            return []
        normalized = query.casefold().strip()
        return POPULAR_DIRECTIONS.get(normalized, [])
