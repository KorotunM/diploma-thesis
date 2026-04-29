from __future__ import annotations

from time import perf_counter
from uuid import UUID

from apps.normalizer.app.claims import ClaimEvidenceRecord, ClaimRecord
from apps.normalizer.app.resolution import (
    RATING_FIELD_POLICY,
    FieldResolutionPolicyMatrix,
    SourceTrustTier,
    source_tier_map,
)
from apps.normalizer.app.universities import UniversityBootstrapResult
from libs.observability import DomainMetricsCollector, get_domain_metrics

from .models import ResolvedFactBuildResult, ResolvedFactCandidate
from .repository import ResolvedFactRepository, deterministic_resolved_fact_id

CANONICAL_FACT_FIELDS = (
    "canonical_name",
    "contacts.website",
    "location.city",
    "location.country_code",
)
RATING_FIELD_PREFIX = "ratings."
RATING_COMPONENT_FIELDS = (
    "ratings.provider",
    "ratings.year",
    "ratings.metric",
    "ratings.value",
)


class ResolvedFactGenerationService:
    def __init__(
        self,
        repository: ResolvedFactRepository,
        *,
        canonical_fields: tuple[str, ...] = CANONICAL_FACT_FIELDS,
        card_version: int = 1,
        policy_matrix: FieldResolutionPolicyMatrix | None = None,
        metrics_collector: DomainMetricsCollector | None = None,
    ) -> None:
        self._repository = repository
        self._canonical_fields = canonical_fields
        self._card_version = card_version
        self._policy_matrix = policy_matrix or FieldResolutionPolicyMatrix()
        self._metrics = metrics_collector or get_domain_metrics()

    def generate_for_bootstrap(
        self,
        bootstrap_result: UniversityBootstrapResult,
    ) -> ResolvedFactBuildResult:
        started_at = perf_counter()
        parser_version = self._parser_version(bootstrap_result)
        normalizer_version = self._normalizer_version(bootstrap_result)
        source_count = len(bootstrap_result.sources_used or [bootstrap_result.source])
        try:
            source_tiers = source_tier_map(
                {
                    source.source_key: source.trust_tier
                    for source in (
                        bootstrap_result.sources_used or [bootstrap_result.source]
                    )
                },
                default_source_key=bootstrap_result.source.source_key,
                default_trust_tier=bootstrap_result.source.trust_tier,
            )
            claims_by_field = self._best_claims_by_field(
                bootstrap_result.claims_used,
                source_tiers=source_tiers,
            )
            evidence_by_claim_id = self._evidence_by_claim_id(bootstrap_result.evidence_used)
            candidates = [
                self._candidate_from_claim(
                    bootstrap_result=bootstrap_result,
                    claim=claim,
                    evidence=evidence_by_claim_id.get(claim.claim_id, []),
                    source_trust_tier=source_tiers[claim.source_key],
                )
                for field_name in self._canonical_fields
                if (claim := claims_by_field.get(field_name)) is not None
            ]
            rating_candidates = self._rating_candidates(
                bootstrap_result=bootstrap_result,
                evidence_by_claim_id=evidence_by_claim_id,
                source_tiers=source_tiers,
            )
            candidates.extend(rating_candidates)
            facts = self._repository.upsert_resolved_facts(candidates)
            self._repository.commit()
            self._metrics.record_normalize_run(
                status="succeeded",
                parser_version=parser_version,
                normalizer_version=normalizer_version,
                claim_count=len(bootstrap_result.claims_used),
                evidence_count=len(bootstrap_result.evidence_used),
                resolved_fact_count=len(facts),
                source_count=source_count,
                rating_fact_count=len(rating_candidates),
                duration_seconds=perf_counter() - started_at,
            )
            return ResolvedFactBuildResult(
                university=bootstrap_result.university,
                facts=facts,
            )
        except Exception:
            self._metrics.record_normalize_run(
                status="failed",
                parser_version=parser_version,
                normalizer_version=normalizer_version,
                claim_count=len(bootstrap_result.claims_used),
                evidence_count=len(bootstrap_result.evidence_used),
                resolved_fact_count=0,
                source_count=source_count,
                rating_fact_count=0,
                duration_seconds=perf_counter() - started_at,
            )
            raise

    @staticmethod
    def _parser_version(bootstrap_result: UniversityBootstrapResult) -> str:
        versions = {claim.parser_version for claim in bootstrap_result.claims_used}
        if not versions:
            return "unknown"
        if len(versions) == 1:
            return next(iter(versions))
        return "mixed"

    @staticmethod
    def _normalizer_version(bootstrap_result: UniversityBootstrapResult) -> str:
        versions = {
            claim.normalizer_version
            for claim in bootstrap_result.claims_used
            if claim.normalizer_version
        }
        if not versions:
            return "unknown"
        if len(versions) == 1:
            return next(iter(versions))
        return "mixed"

    def _best_claims_by_field(
        self,
        claims: list[ClaimRecord],
        *,
        source_tiers: dict[str, SourceTrustTier],
    ) -> dict[str, ClaimRecord]:
        result: dict[str, ClaimRecord] = {}
        for field_name in self._canonical_fields:
            selected = self._policy_matrix.select_best_claim(
                field_name=field_name,
                claims=(claim for claim in claims if claim.field_name == field_name),
                source_tiers=source_tiers,
            )
            if selected is not None:
                result[field_name] = selected
        return result

    @staticmethod
    def _evidence_by_claim_id(
        evidence: list[ClaimEvidenceRecord],
    ) -> dict[UUID, list[ClaimEvidenceRecord]]:
        result: dict[UUID, list[ClaimEvidenceRecord]] = {}
        for record in evidence:
            result.setdefault(record.claim_id, []).append(record)
        return result

    def _candidate_from_claim(
        self,
        *,
        bootstrap_result: UniversityBootstrapResult,
        claim: ClaimRecord,
        evidence: list[ClaimEvidenceRecord],
        source_trust_tier: SourceTrustTier,
    ) -> ResolvedFactCandidate:
        university_id = bootstrap_result.university.university_id
        policy = self._policy_matrix.policy_for(claim.field_name)
        return ResolvedFactCandidate(
            resolved_fact_id=deterministic_resolved_fact_id(
                university_id=university_id,
                field_name=claim.field_name,
                card_version=self._card_version,
            ),
            university_id=university_id,
            field_name=claim.field_name,
            value=claim.value,
            value_type=claim.value_type,
            fact_score=claim.parser_confidence,
            resolution_policy=policy.policy_name,
            card_version=self._card_version,
            selected_claim_ids=[claim.claim_id],
            selected_evidence_ids=[record.evidence_id for record in evidence],
            metadata={
                "source_key": claim.source_key,
                "source_trust_tier": source_trust_tier.value,
                "source_keys": sorted(
                    {
                        candidate.source_key
                        for candidate in bootstrap_result.claims_used
                        if candidate.field_name == claim.field_name
                        and candidate.value is not None
                    }
                ),
                "parser_version": claim.parser_version,
                "normalizer_version": claim.normalizer_version,
                "entity_hint": claim.entity_hint,
                "bootstrap_policy": bootstrap_result.university.metadata.get(
                    "bootstrap_policy"
                ),
                "field_resolution_policy": policy.policy_name,
                "allowed_trust_tiers": [tier.value for tier in policy.allowed_tiers],
                "preferred_trust_tiers": [
                    tier.value for tier in policy.preferred_tiers
                ],
                "resolution_strategy": policy.strategy.value,
                "source_urls": sorted({record.source_url for record in evidence}),
            },
        )

    def _rating_candidates(
        self,
        *,
        bootstrap_result: UniversityBootstrapResult,
        evidence_by_claim_id: dict[UUID, list[ClaimEvidenceRecord]],
        source_tiers: dict[str, SourceTrustTier],
    ) -> list[ResolvedFactCandidate]:
        claims_by_item_key = self._rating_claims_by_item_key(bootstrap_result.claims_used)
        candidates: list[ResolvedFactCandidate] = []
        for rating_item_key, rating_claims in sorted(claims_by_item_key.items()):
            selected_claims = self._select_rating_claims(
                claims=rating_claims,
                source_tiers=source_tiers,
            )
            if selected_claims is None:
                continue
            evidence = self._selected_rating_evidence(
                selected_claims=selected_claims,
                evidence_by_claim_id=evidence_by_claim_id,
            )
            candidates.append(
                self._rating_candidate(
                    bootstrap_result=bootstrap_result,
                    rating_item_key=rating_item_key,
                    selected_claims=selected_claims,
                    evidence=evidence,
                    source_tiers=source_tiers,
                )
            )
        return candidates

    @staticmethod
    def _rating_claims_by_item_key(
        claims: list[ClaimRecord],
    ) -> dict[str, list[ClaimRecord]]:
        grouped: dict[str, list[ClaimRecord]] = {}
        for claim in claims:
            if claim.field_name not in RATING_COMPONENT_FIELDS:
                continue
            rating_item_key = ResolvedFactGenerationService._rating_item_key(claim)
            if rating_item_key is None:
                continue
            grouped.setdefault(rating_item_key, []).append(claim)
        return grouped

    def _select_rating_claims(
        self,
        *,
        claims: list[ClaimRecord],
        source_tiers: dict[str, SourceTrustTier],
    ) -> dict[str, ClaimRecord] | None:
        selected: dict[str, ClaimRecord] = {}
        for field_name in RATING_COMPONENT_FIELDS:
            claim = self._policy_matrix.select_best_claim(
                field_name=field_name,
                claims=(item for item in claims if item.field_name == field_name),
                source_tiers=source_tiers,
            )
            if claim is None:
                return None
            selected[field_name] = claim
        return selected

    @staticmethod
    def _selected_rating_evidence(
        *,
        selected_claims: dict[str, ClaimRecord],
        evidence_by_claim_id: dict[UUID, list[ClaimEvidenceRecord]],
    ) -> list[ClaimEvidenceRecord]:
        evidence: dict[UUID, ClaimEvidenceRecord] = {}
        for field_name in RATING_COMPONENT_FIELDS:
            claim = selected_claims[field_name]
            for record in evidence_by_claim_id.get(claim.claim_id, []):
                evidence.setdefault(record.evidence_id, record)
        return list(evidence.values())

    def _rating_candidate(
        self,
        *,
        bootstrap_result: UniversityBootstrapResult,
        rating_item_key: str,
        selected_claims: dict[str, ClaimRecord],
        evidence: list[ClaimEvidenceRecord],
        source_tiers: dict[str, SourceTrustTier],
    ) -> ResolvedFactCandidate:
        provider_claim = selected_claims["ratings.provider"]
        year_claim = selected_claims["ratings.year"]
        metric_claim = selected_claims["ratings.metric"]
        value_claim = selected_claims["ratings.value"]
        selected_rating_claims = [
            selected_claims[field_name] for field_name in RATING_COMPONENT_FIELDS
        ]
        source_key = value_claim.source_key
        policy = self._policy_matrix.policy_for("ratings.value")
        rating_field_name = f"{RATING_FIELD_PREFIX}{rating_item_key}"
        return ResolvedFactCandidate(
            resolved_fact_id=deterministic_resolved_fact_id(
                university_id=bootstrap_result.university.university_id,
                field_name=rating_field_name,
                card_version=self._card_version,
            ),
            university_id=bootstrap_result.university.university_id,
            field_name=rating_field_name,
            value={
                "provider": provider_claim.value,
                "year": year_claim.value,
                "metric": metric_claim.value,
                "value": value_claim.value,
            },
            value_type="rating_item",
            fact_score=min(claim.parser_confidence for claim in selected_rating_claims),
            resolution_policy=RATING_FIELD_POLICY,
            card_version=self._card_version,
            selected_claim_ids=[claim.claim_id for claim in selected_rating_claims],
            selected_evidence_ids=[record.evidence_id for record in evidence],
            metadata={
                "source_key": source_key,
                "source_trust_tier": source_tiers[source_key].value,
                "source_keys": sorted(
                    {
                        claim.source_key
                        for claim in bootstrap_result.claims_used
                        if self._rating_item_key(claim) == rating_item_key
                    }
                ),
                "parser_version": value_claim.parser_version,
                "normalizer_version": value_claim.normalizer_version,
                "entity_hint": value_claim.entity_hint,
                "bootstrap_policy": bootstrap_result.university.metadata.get(
                    "bootstrap_policy"
                ),
                "field_resolution_policy": policy.policy_name,
                "allowed_trust_tiers": [tier.value for tier in policy.allowed_tiers],
                "preferred_trust_tiers": [tier.value for tier in policy.preferred_tiers],
                "resolution_strategy": policy.strategy.value,
                "source_urls": sorted({record.source_url for record in evidence}),
                "rating_item_key": rating_item_key,
                "provider_name": self._metadata_string(provider_claim, "provider_name")
                or self._string_value(provider_claim),
                "provider_key": self._metadata_string(provider_claim, "provider_key")
                or self._metadata_string(value_claim, "provider_key"),
                "rank_display": self._metadata_string(value_claim, "rank_display"),
                "scale": self._metadata_string(value_claim, "scale"),
                "rating_components": list(RATING_COMPONENT_FIELDS),
            },
        )

    @staticmethod
    def _metadata_string(claim: ClaimRecord, key: str) -> str | None:
        fragment_metadata = claim.metadata.get("fragment_metadata")
        if isinstance(fragment_metadata, dict):
            value = fragment_metadata.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        value = claim.metadata.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        return None

    @staticmethod
    def _rating_item_key(claim: ClaimRecord) -> str | None:
        return ResolvedFactGenerationService._metadata_string(claim, "rating_item_key")

    @staticmethod
    def _string_value(claim: ClaimRecord) -> str | None:
        if isinstance(claim.value, str) and claim.value.strip():
            return claim.value.strip()
        return None
