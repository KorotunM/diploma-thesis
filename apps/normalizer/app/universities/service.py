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
from apps.normalizer.app.resolution import (
    SINGLE_SOURCE_AUTHORITATIVE_POLICY,
    FieldResolutionPolicyMatrix,
    SourceTrustTier,
    source_tier_map,
)

from .models import (
    SourceAuthorityRecord,
    UniversityBootstrapCandidate,
    UniversityBootstrapResult,
    UniversityRecord,
)
from .repository import UniversityBootstrapRepository, deterministic_university_id

AUTHORITATIVE_EXACT_DOMAIN_MERGE_POLICY = "authoritative_anchor_exact_domain_merge"


class UniversityBootstrapError(ValueError):
    pass


class UniversityBootstrapService:
    def __init__(
        self,
        repository: UniversityBootstrapRepository,
        *,
        policy_matrix: FieldResolutionPolicyMatrix | None = None,
    ) -> None:
        self._repository = repository
        self._policy_matrix = policy_matrix or FieldResolutionPolicyMatrix()

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
        if canonical_domain is None:
            raise UniversityBootstrapError(
                f"Source {source.source_key} does not provide contacts.website for merge."
            )
        university = self._repository.find_university_by_canonical_domain(canonical_domain)
        if university is None:
            raise UniversityBootstrapError(
                f"No authoritative university was found for canonical domain {canonical_domain}."
            )

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
                matched_by="canonical_domain",
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
    ) -> dict[str, ClaimRecord]:
        result: dict[str, ClaimRecord] = {}
        tiers = source_tier_map(
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
        value = self._string_claim_value(claims_by_field, "canonical_name")
        if not value:
            raise UniversityBootstrapError(
                "Authoritative bootstrap requires canonical_name claim."
            )
        return value

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
    ) -> dict[str, Any]:
        return {
            "bootstrap_policy": AUTHORITATIVE_EXACT_DOMAIN_MERGE_POLICY,
            "merge_strategy": AUTHORITATIVE_EXACT_DOMAIN_MERGE_POLICY,
            "matched_by": matched_by,
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

    @staticmethod
    def _source_urls(evidence: Iterable[ClaimEvidenceRecord]) -> set[str]:
        return {record.source_url for record in evidence}
