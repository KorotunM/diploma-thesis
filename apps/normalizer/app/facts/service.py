from __future__ import annotations

from uuid import UUID

from apps.normalizer.app.claims import ClaimEvidenceRecord, ClaimRecord
from apps.normalizer.app.universities import UniversityBootstrapResult

from .models import ResolvedFactBuildResult, ResolvedFactCandidate
from .repository import ResolvedFactRepository, deterministic_resolved_fact_id

CANONICAL_FACT_FIELDS = (
    "canonical_name",
    "contacts.website",
    "location.city",
    "location.country_code",
)
SINGLE_SOURCE_POLICY = "single_source_authoritative_highest_confidence"


class ResolvedFactGenerationService:
    def __init__(
        self,
        repository: ResolvedFactRepository,
        *,
        canonical_fields: tuple[str, ...] = CANONICAL_FACT_FIELDS,
        card_version: int = 1,
    ) -> None:
        self._repository = repository
        self._canonical_fields = canonical_fields
        self._card_version = card_version

    def generate_for_bootstrap(
        self,
        bootstrap_result: UniversityBootstrapResult,
    ) -> ResolvedFactBuildResult:
        claims_by_field = self._best_claims_by_field(bootstrap_result.claims_used)
        evidence_by_claim_id = self._evidence_by_claim_id(bootstrap_result.evidence_used)
        candidates = [
            self._candidate_from_claim(
                bootstrap_result=bootstrap_result,
                claim=claim,
                evidence=evidence_by_claim_id.get(claim.claim_id, []),
            )
            for field_name in self._canonical_fields
            if (claim := claims_by_field.get(field_name)) is not None
        ]
        facts = self._repository.upsert_resolved_facts(candidates)
        self._repository.commit()
        return ResolvedFactBuildResult(
            university=bootstrap_result.university,
            facts=facts,
        )

    @staticmethod
    def _best_claims_by_field(claims: list[ClaimRecord]) -> dict[str, ClaimRecord]:
        result: dict[str, ClaimRecord] = {}
        for claim in sorted(claims, key=lambda item: item.parser_confidence, reverse=True):
            if claim.value is not None:
                result.setdefault(claim.field_name, claim)
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
    ) -> ResolvedFactCandidate:
        university_id = bootstrap_result.university.university_id
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
            resolution_policy=SINGLE_SOURCE_POLICY,
            card_version=self._card_version,
            selected_claim_ids=[claim.claim_id],
            selected_evidence_ids=[record.evidence_id for record in evidence],
            metadata={
                "source_key": claim.source_key,
                "parser_version": claim.parser_version,
                "normalizer_version": claim.normalizer_version,
                "entity_hint": claim.entity_hint,
                "bootstrap_policy": bootstrap_result.university.metadata.get(
                    "bootstrap_policy"
                ),
                "source_urls": sorted({record.source_url for record in evidence}),
            },
        )
