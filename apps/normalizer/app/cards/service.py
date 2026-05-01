from __future__ import annotations

from time import perf_counter
from uuid import UUID

from apps.normalizer.app.facts import (
    PROGRAM_FIELD_PREFIX,
    ResolvedFactBuildResult,
    ResolvedFactRecord,
)
from apps.normalizer.app.search_docs import UniversitySearchDocProjectionService
from libs.domain.university.models import (
    CardVersionInfo,
    ConfidenceValue,
    ContactsInfo,
    FieldAttribution,
    InstitutionalInfo,
    LocationInfo,
    RatingItem,
    ReviewSummary,
    UniversityCard,
)
from libs.observability import DomainMetricsCollector, get_domain_metrics

from .models import UniversityCardProjectionResult
from .repository import UniversityCardProjectionRepository


class UniversityCardProjectionService:
    def __init__(
        self,
        repository: UniversityCardProjectionRepository,
        *,
        normalizer_version: str = "normalizer.0.1.0",
        search_doc_service: UniversitySearchDocProjectionService,
        metrics_collector: DomainMetricsCollector | None = None,
    ) -> None:
        self._repository = repository
        self._normalizer_version = normalizer_version
        self._search_doc_service = search_doc_service
        self._metrics = metrics_collector or get_domain_metrics()

    def create_projection(
        self,
        fact_result: ResolvedFactBuildResult,
    ) -> UniversityCardProjectionResult:
        started_at = perf_counter()
        try:
            card_version = self._card_version(fact_result.facts)
            persisted_version = self._repository.upsert_card_version(
                university_id=fact_result.university.university_id,
                card_version=card_version,
                normalizer_version=self._normalizer_version,
            )
            card = self._build_card(
                fact_result=fact_result,
                generated_at=persisted_version.generated_at,
                card_version=card_version,
            )
            projection = self._repository.upsert_delivery_projection(
                card=card,
                generated_at=persisted_version.generated_at,
            )
            search_doc = self._search_doc_service.refresh_for_card(card)
            self._repository.commit()
            self._metrics.record_card_build(
                status="succeeded",
                normalizer_version=self._normalizer_version,
                resolved_fact_count=len(fact_result.facts),
                rating_count=len(card.ratings),
                duration_seconds=perf_counter() - started_at,
                search_doc_refreshed=search_doc is not None,
            )
            return UniversityCardProjectionResult(
                card_version=persisted_version,
                projection=projection,
                search_doc=search_doc,
            )
        except Exception:
            self._metrics.record_card_build(
                status="failed",
                normalizer_version=self._normalizer_version,
                resolved_fact_count=len(fact_result.facts),
                rating_count=0,
                duration_seconds=perf_counter() - started_at,
                search_doc_refreshed=False,
            )
            raise

    @staticmethod
    def _card_version(facts: list[ResolvedFactRecord]) -> int:
        versions = {fact.card_version for fact in facts}
        if not versions:
            return 1
        return max(versions)

    def _build_card(
        self,
        *,
        fact_result: ResolvedFactBuildResult,
        generated_at,
        card_version: int,
    ) -> UniversityCard:
        facts_by_field = {fact.field_name: fact for fact in fact_result.facts}
        canonical_name_fact = facts_by_field.get("canonical_name")
        canonical_name = self._confidence_value(canonical_name_fact)
        if canonical_name.value is None:
            canonical_name = ConfidenceValue(
                value=fact_result.university.canonical_name,
                confidence=1.0,
                sources=[],
            )

        return UniversityCard(
            university_id=fact_result.university.university_id,
            canonical_name=canonical_name,
            aliases=[],
            location=LocationInfo(
                country=self._string_fact_value(facts_by_field.get("location.country_code")),
                city=self._string_fact_value(facts_by_field.get("location.city")),
            ),
            contacts=ContactsInfo(
                website=self._string_fact_value(facts_by_field.get("contacts.website")),
                emails=self._string_list_fact_value(facts_by_field.get("contacts.emails")),
                phones=self._string_list_fact_value(facts_by_field.get("contacts.phones")),
            ),
            institutional=InstitutionalInfo.model_validate(
                {"type": None, "founded_year": None}
            ),
            programs=self._programs(fact_result.facts),
            tuition=[],
            ratings=self._ratings(fact_result.facts),
            dormitory={},
            reviews=ReviewSummary(),
            sources=self._sources(fact_result.facts),
            version=CardVersionInfo(
                card_version=card_version,
                generated_at=generated_at,
            ),
        )

    def _confidence_value(
        self,
        fact: ResolvedFactRecord | None,
    ) -> ConfidenceValue:
        if fact is None:
            return ConfidenceValue(value=None, confidence=0.0, sources=[])
        return ConfidenceValue(
            value=fact.value,
            confidence=fact.fact_score,
            sources=self._field_sources(fact),
        )

    @staticmethod
    def _string_fact_value(fact: ResolvedFactRecord | None) -> str | None:
        if fact is None or not isinstance(fact.value, str):
            return None
        value = fact.value.strip()
        return value or None

    @staticmethod
    def _string_list_fact_value(fact: ResolvedFactRecord | None) -> list[str]:
        if fact is None or not isinstance(fact.value, list):
            return []
        result: list[str] = []
        seen: set[str] = set()
        for value in fact.value:
            if not isinstance(value, str):
                continue
            cleaned = value.strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            result.append(cleaned)
        return result

    def _sources(self, facts: list[ResolvedFactRecord]) -> list[FieldAttribution]:
        deduped: dict[tuple[str, str], set[UUID]] = {}
        for fact in facts:
            for source in self._field_sources(fact):
                key = (source.source_key, source.source_url)
                deduped.setdefault(key, set()).update(source.evidence_ids)
        return [
            FieldAttribution(
                source_key=source_key,
                source_url=source_url,
                evidence_ids=sorted(evidence_ids, key=str),
            )
            for (source_key, source_url), evidence_ids in sorted(deduped.items())
        ]

    @staticmethod
    def _field_sources(fact: ResolvedFactRecord) -> list[FieldAttribution]:
        source_key = fact.metadata.get("source_key")
        source_urls = fact.metadata.get("source_urls")
        if not isinstance(source_key, str) or not isinstance(source_urls, list):
            return []
        evidence_ids = [
            evidence_id
            for evidence_id in fact.selected_evidence_ids
            if isinstance(evidence_id, UUID)
        ]
        return [
            FieldAttribution(
                source_key=source_key,
                source_url=source_url,
                evidence_ids=evidence_ids,
            )
            for source_url in source_urls
            if isinstance(source_url, str)
        ]

    @staticmethod
    def _ratings(facts: list[ResolvedFactRecord]) -> list[RatingItem]:
        ratings: list[RatingItem] = []
        for fact in sorted(facts, key=lambda item: item.field_name):
            if not fact.field_name.startswith("ratings."):
                continue
            if not isinstance(fact.value, dict):
                continue
            ratings.append(RatingItem.model_validate(fact.value))
        return ratings

    @staticmethod
    def _programs(facts: list[ResolvedFactRecord]) -> list[dict[str, object]]:
        programs: list[dict[str, object]] = []
        for fact in sorted(facts, key=lambda item: item.field_name):
            if not fact.field_name.startswith(PROGRAM_FIELD_PREFIX):
                continue
            if not isinstance(fact.value, dict):
                continue
            programs.append(
                {
                    "faculty": fact.value.get("faculty"),
                    "code": fact.value.get("code"),
                    "name": fact.value.get("name"),
                    "budget_places": fact.value.get("budget_places"),
                    "passing_score": fact.value.get("passing_score"),
                    "year": fact.value.get("year"),
                    "confidence": fact.fact_score,
                    "sources": [
                        source.model_dump(mode="python")
                        for source in UniversityCardProjectionService._field_sources(fact)
                    ],
                }
            )
        return programs
