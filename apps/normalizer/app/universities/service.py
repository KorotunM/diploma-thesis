from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

from apps.normalizer.app.claims import (
    ClaimBuildResult,
    ClaimEvidenceRecord,
    ClaimRecord,
)
from apps.normalizer.app.matching import (
    MatchStrategy,
    UniversityMatchCandidate,
    UniversityMatchDecision,
    UniversityMatchService,
)
from apps.normalizer.app.resolution import (
    SINGLE_SOURCE_AUTHORITATIVE_POLICY,
    FieldResolutionPolicyMatrix,
    SourceTrustTier,
    source_tier_map,
)
from apps.normalizer.app.review_required import ReviewRequiredEmitter

from .models import (
    SourceAuthorityRecord,
    UniversityBootstrapCandidate,
    UniversityBootstrapResult,
    UniversityRecord,
)
from .repository import (
    UniversityBootstrapRepository,
    deterministic_university_id,
    deterministic_university_id_from_identity,
)

AUTHORITATIVE_EXACT_MATCH_MERGE_POLICY = "authoritative_anchor_exact_match_merge"
AUTHORITATIVE_SOURCE_DOCUMENT_MERGE_POLICY = "authoritative_source_document_merge"
TRUSTED_SOURCE_BOOTSTRAP_POLICY = "trusted_source_bootstrap"
AUTHORITATIVE_PROMOTION_POLICY = "authoritative_promotion_from_trusted"


class UniversityBootstrapError(ValueError):
    pass


class UniversityBootstrapService:
    def __init__(
        self,
        repository: UniversityBootstrapRepository,
        *,
        policy_matrix: FieldResolutionPolicyMatrix | None = None,
        match_service: UniversityMatchService | None = None,
        review_required_emitter: ReviewRequiredEmitter | None = None,
    ) -> None:
        self._repository = repository
        self._policy_matrix = policy_matrix or FieldResolutionPolicyMatrix()
        self._match_service = match_service or UniversityMatchService(repository)
        self._review_required_emitter = review_required_emitter

    def consolidate_claims(
        self,
        claim_result: ClaimBuildResult,
    ) -> UniversityBootstrapResult:
        source_key = self._single_source_key(claim_result.claims)
        source = self._repository.get_source(source_key)
        if source is None:
            raise UniversityBootstrapError(f"Source {source_key} was not found.")
        if not source.is_active:
            raise UniversityBootstrapError(
                f"Source {source.source_key} is inactive and cannot be consolidated."
            )
        if source.trust_tier is SourceTrustTier.AUTHORITATIVE:
            return self.bootstrap_single_source_authoritative(claim_result)
        return self._merge_secondary_source(
            source=source,
            claim_result=claim_result,
        )

    def bootstrap_single_source_authoritative(
        self,
        claim_result: ClaimBuildResult,
    ) -> UniversityBootstrapResult:
        source_key = self._single_source_key(claim_result.claims)
        source = self._repository.get_source(source_key)
        if source is None:
            raise UniversityBootstrapError(f"Source {source_key} was not found.")
        self._validate_authoritative_source(source)

        university_id = deterministic_university_id(source.source_key)
        existing_university = self._repository.find_university_by_id(university_id)
        if existing_university is not None:
            return self._merge_authoritative_source_document(
                source=source,
                university=existing_university,
                claim_result=claim_result,
            )

        # Before creating a fresh record, check if a trusted-bootstrapped university
        # already exists for the same identity (domain / name). If so, promote it in-place
        # so the authoritative source takes ownership without duplicating the row.
        claims_by_field_pre = self._claims_by_field(
            claims=claim_result.claims,
            source=source,
        )
        pre_domain = self._canonical_domain(claims_by_field_pre)
        pre_name = self._canonical_name_optional(claims_by_field_pre)
        if pre_domain or pre_name:
            pre_match = self._match_service.match(
                UniversityMatchCandidate(
                    canonical_domain=pre_domain,
                    canonical_name=pre_name,
                )
            )
            if pre_match.status == "matched" and pre_match.university is not None:
                return self._promote_trusted_to_authoritative(
                    source=source,
                    university=pre_match.university,
                    claim_result=claim_result,
                )

        candidate = self._build_candidate(
            source=source,
            claim_result=claim_result,
        )
        university = self._repository.upsert_university(candidate)
        self._repository.commit()
        return UniversityBootstrapResult(
            source=source,
            sources_used=[source],
            university=university,
            claims_used=self._sort_claims(claim_result.claims),
            evidence_used=self._sort_evidence(claim_result.evidence),
        )

    def _merge_authoritative_source_document(
        self,
        *,
        source: SourceAuthorityRecord,
        university: UniversityRecord,
        claim_result: ClaimBuildResult,
    ) -> UniversityBootstrapResult:
        existing_claims = self._repository.list_claims_for_university(
            university.university_id
        )
        existing_evidence = self._repository.list_evidence_for_university(
            university.university_id
        )
        combined_claims = self._merge_claims(
            existing_claims,
            claim_result.claims,
        )
        combined_evidence = self._merge_evidence(
            existing_evidence,
            claim_result.evidence,
        )
        claims_by_field = self._claims_by_field(
            claims=combined_claims,
            source=source,
        )
        candidate = UniversityBootstrapCandidate(
            university_id=university.university_id,
            canonical_name=university.canonical_name
            or self._canonical_name(claims_by_field),
            canonical_domain=university.canonical_domain
            or self._canonical_domain(claims_by_field),
            country_code=university.country_code
            or self._string_claim_value(claims_by_field, "location.country_code"),
            city_name=university.city_name
            or self._string_claim_value(claims_by_field, "location.city"),
            metadata=self._same_source_merge_metadata(
                source=source,
                combined_claims=combined_claims,
                combined_evidence=combined_evidence,
                claim_result=claim_result,
            ),
        )
        persisted = self._repository.upsert_university(candidate)
        self._repository.commit()
        return UniversityBootstrapResult(
            source=source,
            sources_used=self._merged_sources(
                university=university,
                anchor_source=source,
                merged_source=source,
            ),
            university=persisted,
            claims_used=combined_claims,
            evidence_used=combined_evidence,
        )

    def _merge_secondary_source(
        self,
        *,
        source: SourceAuthorityRecord,
        claim_result: ClaimBuildResult,
    ) -> UniversityBootstrapResult:
        claims_by_field = self._claims_by_field(
            claims=claim_result.claims,
            source=source,
        )
        canonical_domain = self._canonical_domain(claims_by_field)
        canonical_name = self._canonical_name_optional(claims_by_field)
        match = self._match_service.match(
            UniversityMatchCandidate(
                canonical_domain=canonical_domain,
                canonical_name=canonical_name,
            )
        )
        if match.status == "review_required":
            self._emit_review_required(
                source=source,
                claim_result=claim_result,
                match=match,
            )
            raise UniversityBootstrapError(
                "Gray-zone trigram match requires manual review before merge."
            )
        if match.status != "matched" or match.university is None:
            return self._bootstrap_trusted_source(
                source=source,
                claim_result=claim_result,
                canonical_domain=canonical_domain,
                canonical_name=canonical_name,
            )

        university = match.university
        anchor_source = self._anchor_source(university)
        existing_claims = self._repository.list_claims_for_university(
            university.university_id
        )
        existing_evidence = self._repository.list_evidence_for_university(
            university.university_id
        )
        combined_claims = self._sort_claims([*existing_claims, *claim_result.claims])
        combined_evidence = self._sort_evidence([*existing_evidence, *claim_result.evidence])

        candidate = UniversityBootstrapCandidate(
            university_id=university.university_id,
            canonical_name=university.canonical_name,
            canonical_domain=university.canonical_domain or canonical_domain,
            country_code=university.country_code
            or self._string_claim_value(claims_by_field, "location.country_code"),
            city_name=university.city_name
            or self._string_claim_value(claims_by_field, "location.city"),
            metadata=self._merged_metadata(
                university=university,
                anchor_source=anchor_source,
                merged_source=source,
                claim_result=claim_result,
                combined_claims=combined_claims,
                combined_evidence=combined_evidence,
                matched_by=match.matched_by,
                matched_value=match.matched_value,
                match_strategy=match.strategy,
                similarity_score=match.similarity_score,
            ),
        )
        persisted = self._repository.upsert_university(candidate)
        self._repository.commit()
        return UniversityBootstrapResult(
            source=anchor_source,
            sources_used=self._merged_sources(
                university=university,
                anchor_source=anchor_source,
                merged_source=source,
            ),
            university=persisted,
            claims_used=combined_claims,
            evidence_used=combined_evidence,
        )

    def _build_candidate(
        self,
        *,
        source: SourceAuthorityRecord,
        claim_result: ClaimBuildResult,
    ) -> UniversityBootstrapCandidate:
        claims_by_field = self._claims_by_field(
            claims=claim_result.claims,
            source=source,
        )
        canonical_name = self._canonical_name(claims_by_field)
        canonical_domain = self._canonical_domain(claims_by_field)
        metadata = self._metadata(
            source=source,
            claim_result=claim_result,
        )
        return UniversityBootstrapCandidate(
            university_id=deterministic_university_id(source.source_key),
            canonical_name=canonical_name,
            canonical_domain=canonical_domain,
            country_code=self._string_claim_value(claims_by_field, "location.country_code"),
            city_name=self._string_claim_value(claims_by_field, "location.city"),
            metadata=metadata,
        )

    @staticmethod
    def _single_source_key(claims: list[ClaimRecord]) -> str:
        source_keys = {claim.source_key for claim in claims}
        if len(source_keys) != 1:
            raise UniversityBootstrapError(
                "University bootstrap requires claims from exactly one source."
            )
        return next(iter(source_keys))

    @staticmethod
    def _validate_authoritative_source(source: SourceAuthorityRecord) -> None:
        if not source.is_active:
            raise UniversityBootstrapError(
                f"Source {source.source_key} is inactive and cannot bootstrap a university."
            )
        if source.trust_tier is not SourceTrustTier.AUTHORITATIVE:
            raise UniversityBootstrapError(
                f"Source {source.source_key} is not authoritative."
            )

    def _claims_by_field(
        self,
        *,
        claims: list[ClaimRecord],
        source: SourceAuthorityRecord,
        extra_source_tiers: dict[str, SourceTrustTier] | None = None,
    ) -> dict[str, ClaimRecord]:
        result: dict[str, ClaimRecord] = {}
        tiers = source_tier_map(
            source_tiers=extra_source_tiers,
            default_source_key=source.source_key,
            default_trust_tier=source.trust_tier,
        )
        for field_name in {claim.field_name for claim in claims}:
            selected = self._policy_matrix.select_best_claim(
                field_name=field_name,
                claims=(claim for claim in claims if claim.field_name == field_name),
                source_tiers=tiers,
            )
            if selected is not None:
                result[field_name] = selected
        return result

    @staticmethod
    def _source_tiers_from_university(university: UniversityRecord) -> dict[str, SourceTrustTier]:
        result: dict[str, SourceTrustTier] = {}
        for snap in university.metadata.get("source_snapshots", []):
            if not isinstance(snap, dict):
                continue
            key = snap.get("source_key")
            tier_val = snap.get("trust_tier")
            if isinstance(key, str) and isinstance(tier_val, str):
                try:
                    result[key] = SourceTrustTier(tier_val)
                except ValueError:
                    pass
        return result

    def _anchor_source(self, university: UniversityRecord) -> SourceAuthorityRecord:
        snapshots = university.metadata.get("source_snapshots")
        if isinstance(snapshots, list):
            for snapshot in snapshots:
                if (
                    isinstance(snapshot, dict)
                    and snapshot.get("trust_tier") == SourceTrustTier.AUTHORITATIVE.value
                ):
                    return self._source_from_snapshot(snapshot)
        source_key = university.metadata.get("source_key")
        if not isinstance(source_key, str):
            raise UniversityBootstrapError(
                f"University {university.university_id} does not have an authoritative anchor."
            )
        source = self._repository.get_source(source_key)
        if source is None:
            raise UniversityBootstrapError(
                f"Source {source_key} was not found for university {university.university_id}."
            )
        return source

    def _merged_sources(
        self,
        *,
        university: UniversityRecord,
        anchor_source: SourceAuthorityRecord,
        merged_source: SourceAuthorityRecord,
    ) -> list[SourceAuthorityRecord]:
        merged: dict[str, SourceAuthorityRecord] = {
            anchor_source.source_key: anchor_source,
            merged_source.source_key: merged_source,
        }
        snapshots = university.metadata.get("source_snapshots")
        if isinstance(snapshots, list):
            for snapshot in snapshots:
                if not isinstance(snapshot, dict):
                    continue
                source = self._source_from_snapshot(snapshot)
                merged.setdefault(source.source_key, source)
        return sorted(merged.values(), key=lambda record: record.source_key)

    def _canonical_name(self, claims_by_field: dict[str, ClaimRecord]) -> str:
        value = self._canonical_name_optional(claims_by_field)
        if not value:
            raise UniversityBootstrapError(
                "Authoritative bootstrap requires canonical_name claim."
            )
        return value

    def _canonical_name_optional(
        self,
        claims_by_field: dict[str, ClaimRecord],
    ) -> str | None:
        return self._string_claim_value(claims_by_field, "canonical_name")

    def _canonical_domain(self, claims_by_field: dict[str, ClaimRecord]) -> str | None:
        website = self._string_claim_value(claims_by_field, "contacts.website")
        if not website:
            return None
        parsed = urlparse(website if "://" in website else f"https://{website}")
        host = parsed.hostname
        if host is None:
            return None
        return host.removeprefix("www.").lower()

    @staticmethod
    def _string_claim_value(
        claims_by_field: dict[str, ClaimRecord],
        field_name: str,
    ) -> str | None:
        claim = claims_by_field.get(field_name)
        if claim is None or not isinstance(claim.value, str):
            return None
        value = claim.value.strip()
        return value or None

    @staticmethod
    def _metadata(
        *,
        source: SourceAuthorityRecord,
        claim_result: ClaimBuildResult,
    ) -> dict[str, Any]:
        claim_ids = [str(claim.claim_id) for claim in claim_result.claims]
        evidence_ids = [str(evidence.evidence_id) for evidence in claim_result.evidence]
        field_names = sorted({claim.field_name for claim in claim_result.claims})
        source_urls = sorted(UniversityBootstrapService._source_urls(claim_result.evidence))
        return {
            "bootstrap_policy": SINGLE_SOURCE_AUTHORITATIVE_POLICY,
            "source_id": str(source.source_id),
            "source_key": source.source_key,
            "source_type": source.source_type,
            "trust_tier": source.trust_tier.value,
            "parsed_document_id": str(claim_result.parsed_document.parsed_document_id),
            "parser_version": claim_result.parsed_document.parser_version,
            "claim_ids": claim_ids,
            "evidence_ids": evidence_ids,
            "field_names": field_names,
            "source_urls": source_urls,
            "source_keys": [source.source_key],
            "source_snapshots": [
                UniversityBootstrapService._source_snapshot(
                    source=source,
                    parsed_document_id=str(claim_result.parsed_document.parsed_document_id),
                    parser_version=claim_result.parsed_document.parser_version,
                    claim_ids=claim_ids,
                    evidence_ids=evidence_ids,
                    field_names=field_names,
                    source_urls=source_urls,
                )
            ],
        }

    def _merged_metadata(
        self,
        *,
        university: UniversityRecord,
        anchor_source: SourceAuthorityRecord,
        merged_source: SourceAuthorityRecord,
        claim_result: ClaimBuildResult,
        combined_claims: list[ClaimRecord],
        combined_evidence: list[ClaimEvidenceRecord],
        matched_by: str,
        matched_value: str,
        match_strategy: MatchStrategy | None,
        similarity_score: float | None,
    ) -> dict[str, Any]:
        return {
            "bootstrap_policy": AUTHORITATIVE_EXACT_MATCH_MERGE_POLICY,
            "merge_strategy": AUTHORITATIVE_EXACT_MATCH_MERGE_POLICY,
            "match_strategy": match_strategy,
            "matched_by": matched_by,
            "matched_value": matched_value,
            "similarity_score": similarity_score,
            "source_id": str(anchor_source.source_id),
            "source_key": anchor_source.source_key,
            "source_type": anchor_source.source_type,
            "trust_tier": anchor_source.trust_tier.value,
            "claim_ids": [str(claim.claim_id) for claim in combined_claims],
            "evidence_ids": [str(record.evidence_id) for record in combined_evidence],
            "field_names": sorted({claim.field_name for claim in combined_claims}),
            "source_urls": sorted(self._source_urls(combined_evidence)),
            "source_keys": sorted({claim.source_key for claim in combined_claims}),
            "source_snapshots": self._merge_source_snapshots(
                university=university,
                merged_source=merged_source,
                claim_result=claim_result,
            ),
        }

    def _same_source_merge_metadata(
        self,
        *,
        source: SourceAuthorityRecord,
        combined_claims: list[ClaimRecord],
        combined_evidence: list[ClaimEvidenceRecord],
        claim_result: ClaimBuildResult,
    ) -> dict[str, Any]:
        claim_ids = [str(claim.claim_id) for claim in combined_claims]
        evidence_ids = [str(record.evidence_id) for record in combined_evidence]
        field_names = sorted({claim.field_name for claim in combined_claims})
        source_urls = sorted(self._source_urls(combined_evidence))
        return {
            "bootstrap_policy": SINGLE_SOURCE_AUTHORITATIVE_POLICY,
            "merge_strategy": AUTHORITATIVE_SOURCE_DOCUMENT_MERGE_POLICY,
            "source_id": str(source.source_id),
            "source_key": source.source_key,
            "source_type": source.source_type,
            "trust_tier": source.trust_tier.value,
            "parsed_document_id": str(claim_result.parsed_document.parsed_document_id),
            "parser_version": claim_result.parsed_document.parser_version,
            "claim_ids": claim_ids,
            "evidence_ids": evidence_ids,
            "field_names": field_names,
            "source_urls": source_urls,
            "source_keys": [source.source_key],
            "source_snapshots": [
                self._source_snapshot(
                    source=source,
                    parsed_document_id=str(claim_result.parsed_document.parsed_document_id),
                    parser_version=claim_result.parsed_document.parser_version,
                    claim_ids=claim_ids,
                    evidence_ids=evidence_ids,
                    field_names=field_names,
                    source_urls=source_urls,
                )
            ],
        }

    def _emit_review_required(
        self,
        *,
        source: SourceAuthorityRecord,
        claim_result: ClaimBuildResult,
        match: UniversityMatchDecision,
    ) -> None:
        if self._review_required_emitter is None:
            return
        self._review_required_emitter.emit_gray_zone_match(
            source=source,
            claim_result=claim_result,
            decision=match,
            trace_id=claim_result.parsed_document.crawl_run_id,
        )

    def _merge_source_snapshots(
        self,
        *,
        university: UniversityRecord,
        merged_source: SourceAuthorityRecord,
        claim_result: ClaimBuildResult,
    ) -> list[dict[str, Any]]:
        existing = university.metadata.get("source_snapshots")
        snapshots_by_key: dict[str, dict[str, Any]] = {}
        if isinstance(existing, list):
            for snapshot in existing:
                if isinstance(snapshot, dict) and isinstance(snapshot.get("source_key"), str):
                    snapshots_by_key[snapshot["source_key"]] = snapshot
        snapshots_by_key[merged_source.source_key] = self._source_snapshot(
            source=merged_source,
            parsed_document_id=str(claim_result.parsed_document.parsed_document_id),
            parser_version=claim_result.parsed_document.parser_version,
            claim_ids=[str(claim.claim_id) for claim in claim_result.claims],
            evidence_ids=[str(evidence.evidence_id) for evidence in claim_result.evidence],
            field_names=sorted({claim.field_name for claim in claim_result.claims}),
            source_urls=sorted(self._source_urls(claim_result.evidence)),
        )
        return [snapshots_by_key[key] for key in sorted(snapshots_by_key)]

    @staticmethod
    def _source_snapshot(
        *,
        source: SourceAuthorityRecord,
        parsed_document_id: str,
        parser_version: str,
        claim_ids: list[str],
        evidence_ids: list[str],
        field_names: list[str],
        source_urls: list[str],
    ) -> dict[str, Any]:
        return {
            "source_id": str(source.source_id),
            "source_key": source.source_key,
            "source_type": source.source_type,
            "trust_tier": source.trust_tier.value,
            "parsed_document_id": parsed_document_id,
            "parser_version": parser_version,
            "claim_ids": claim_ids,
            "evidence_ids": evidence_ids,
            "field_names": field_names,
            "source_urls": source_urls,
        }

    @staticmethod
    def _source_from_snapshot(snapshot: dict[str, Any]) -> SourceAuthorityRecord:
        return SourceAuthorityRecord(
            source_id=UUID(str(snapshot["source_id"])),
            source_key=str(snapshot["source_key"]),
            source_type=str(snapshot["source_type"]),
            trust_tier=SourceTrustTier(str(snapshot["trust_tier"])),
            is_active=True,
            metadata={},
        )

    def _bootstrap_trusted_source(
        self,
        *,
        source: SourceAuthorityRecord,
        claim_result: ClaimBuildResult,
        canonical_domain: str | None,
        canonical_name: str | None,
    ) -> UniversityBootstrapResult:
        if not canonical_name:
            raise UniversityBootstrapError(
                "Trusted bootstrap requires at least a canonical_name claim."
            )
        university_id = deterministic_university_id_from_identity(
            canonical_domain=canonical_domain,
            canonical_name=canonical_name,
        )
        existing = self._repository.find_university_by_id(university_id)
        if existing is not None:
            return self._merge_trusted_source_update(
                source=source,
                university=existing,
                claim_result=claim_result,
            )
        claims_by_field = self._claims_by_field(claims=claim_result.claims, source=source)
        candidate = UniversityBootstrapCandidate(
            university_id=university_id,
            canonical_name=canonical_name,
            canonical_domain=canonical_domain,
            country_code=self._string_claim_value(claims_by_field, "location.country_code"),
            city_name=self._string_claim_value(claims_by_field, "location.city"),
            metadata=self._trusted_bootstrap_metadata(
                source=source,
                claim_result=claim_result,
            ),
        )
        university = self._repository.upsert_university(candidate)
        self._repository.commit()
        return UniversityBootstrapResult(
            source=source,
            sources_used=[source],
            university=university,
            claims_used=self._sort_claims(claim_result.claims),
            evidence_used=self._sort_evidence(claim_result.evidence),
        )

    def _merge_trusted_source_update(
        self,
        *,
        source: SourceAuthorityRecord,
        university: UniversityRecord,
        claim_result: ClaimBuildResult,
    ) -> UniversityBootstrapResult:
        existing_claims = self._repository.list_claims_for_university(university.university_id)
        existing_evidence = self._repository.list_evidence_for_university(university.university_id)
        combined_claims = self._merge_claims(existing_claims, claim_result.claims)
        combined_evidence = self._merge_evidence(existing_evidence, claim_result.evidence)
        candidate = UniversityBootstrapCandidate(
            university_id=university.university_id,
            canonical_name=university.canonical_name,
            canonical_domain=university.canonical_domain,
            country_code=university.country_code,
            city_name=university.city_name,
            metadata=self._merge_source_snapshots_metadata(
                university=university,
                source=source,
                claim_result=claim_result,
                combined_claims=combined_claims,
                combined_evidence=combined_evidence,
            ),
        )
        persisted = self._repository.upsert_university(candidate)
        self._repository.commit()
        return UniversityBootstrapResult(
            source=source,
            sources_used=[source],
            university=persisted,
            claims_used=combined_claims,
            evidence_used=combined_evidence,
        )

    def _promote_trusted_to_authoritative(
        self,
        *,
        source: SourceAuthorityRecord,
        university: UniversityRecord,
        claim_result: ClaimBuildResult,
    ) -> UniversityBootstrapResult:
        existing_claims = self._repository.list_claims_for_university(university.university_id)
        existing_evidence = self._repository.list_evidence_for_university(university.university_id)
        combined_claims = self._merge_claims(existing_claims, claim_result.claims)
        combined_evidence = self._merge_evidence(existing_evidence, claim_result.evidence)
        extra_tiers = self._source_tiers_from_university(university)
        claims_by_field = self._claims_by_field(
            claims=combined_claims, source=source, extra_source_tiers=extra_tiers
        )
        candidate = UniversityBootstrapCandidate(
            university_id=university.university_id,
            canonical_name=self._canonical_name(claims_by_field),
            canonical_domain=self._canonical_domain(claims_by_field) or university.canonical_domain,
            country_code=(
                self._string_claim_value(claims_by_field, "location.country_code")
                or university.country_code
            ),
            city_name=(
                self._string_claim_value(claims_by_field, "location.city")
                or university.city_name
            ),
            metadata=self._merge_source_snapshots_metadata(
                university=university,
                source=source,
                claim_result=claim_result,
                combined_claims=combined_claims,
                combined_evidence=combined_evidence,
                bootstrap_policy=AUTHORITATIVE_PROMOTION_POLICY,
            ),
        )
        persisted = self._repository.upsert_university(candidate)
        self._repository.commit()
        return UniversityBootstrapResult(
            source=source,
            sources_used=[source],
            university=persisted,
            claims_used=combined_claims,
            evidence_used=combined_evidence,
        )

    @staticmethod
    def _trusted_bootstrap_metadata(
        *,
        source: SourceAuthorityRecord,
        claim_result: ClaimBuildResult,
    ) -> dict[str, Any]:
        claim_ids = [str(claim.claim_id) for claim in claim_result.claims]
        evidence_ids = [str(ev.evidence_id) for ev in claim_result.evidence]
        field_names = sorted({claim.field_name for claim in claim_result.claims})
        source_urls = sorted(UniversityBootstrapService._source_urls(claim_result.evidence))
        return {
            "bootstrap_policy": TRUSTED_SOURCE_BOOTSTRAP_POLICY,
            "source_id": str(source.source_id),
            "source_key": source.source_key,
            "source_type": source.source_type,
            "trust_tier": source.trust_tier.value,
            "parsed_document_id": str(claim_result.parsed_document.parsed_document_id),
            "parser_version": claim_result.parsed_document.parser_version,
            "claim_ids": claim_ids,
            "evidence_ids": evidence_ids,
            "field_names": field_names,
            "source_urls": source_urls,
            "source_keys": [source.source_key],
            "source_snapshots": [
                UniversityBootstrapService._source_snapshot(
                    source=source,
                    parsed_document_id=str(claim_result.parsed_document.parsed_document_id),
                    parser_version=claim_result.parsed_document.parser_version,
                    claim_ids=claim_ids,
                    evidence_ids=evidence_ids,
                    field_names=field_names,
                    source_urls=source_urls,
                )
            ],
        }

    def _merge_source_snapshots_metadata(
        self,
        *,
        university: UniversityRecord,
        source: SourceAuthorityRecord,
        claim_result: ClaimBuildResult,
        combined_claims: list[ClaimRecord],
        combined_evidence: list[ClaimEvidenceRecord],
        bootstrap_policy: str = TRUSTED_SOURCE_BOOTSTRAP_POLICY,
    ) -> dict[str, Any]:
        claim_ids = [str(c.claim_id) for c in combined_claims]
        evidence_ids = [str(e.evidence_id) for e in combined_evidence]
        field_names = sorted({c.field_name for c in combined_claims})
        source_urls = sorted(self._source_urls(combined_evidence))
        return {
            "bootstrap_policy": bootstrap_policy,
            "source_id": str(source.source_id),
            "source_key": source.source_key,
            "source_type": source.source_type,
            "trust_tier": source.trust_tier.value,
            "claim_ids": claim_ids,
            "evidence_ids": evidence_ids,
            "field_names": field_names,
            "source_urls": source_urls,
            "source_keys": sorted({c.source_key for c in combined_claims}),
            "source_snapshots": self._merge_source_snapshots(
                university=university,
                merged_source=source,
                claim_result=claim_result,
            ),
        }

    @staticmethod
    def _sort_claims(claims: list[ClaimRecord]) -> list[ClaimRecord]:
        return sorted(
            claims,
            key=lambda claim: (
                claim.source_key,
                claim.field_name,
                -claim.parser_confidence,
                str(claim.claim_id),
            ),
        )

    @staticmethod
    def _sort_evidence(
        evidence: list[ClaimEvidenceRecord],
    ) -> list[ClaimEvidenceRecord]:
        return sorted(
            evidence,
            key=lambda record: (
                record.source_key,
                record.source_url,
                str(record.evidence_id),
            ),
        )

    def _merge_claims(
        self,
        existing_claims: list[ClaimRecord],
        new_claims: list[ClaimRecord],
    ) -> list[ClaimRecord]:
        merged = {
            claim.claim_id: claim
            for claim in [*existing_claims, *new_claims]
        }
        return self._sort_claims(list(merged.values()))

    def _merge_evidence(
        self,
        existing_evidence: list[ClaimEvidenceRecord],
        new_evidence: list[ClaimEvidenceRecord],
    ) -> list[ClaimEvidenceRecord]:
        merged = {
            record.evidence_id: record
            for record in [*existing_evidence, *new_evidence]
        }
        return self._sort_evidence(list(merged.values()))

    @staticmethod
    def _source_urls(evidence: Iterable[ClaimEvidenceRecord]) -> set[str]:
        return {record.source_url for record in evidence}
