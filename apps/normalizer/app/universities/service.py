from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from urllib.parse import urlparse

from apps.normalizer.app.claims import (
    ClaimBuildResult,
    ClaimEvidenceRecord,
    ClaimRecord,
)

from .models import (
    SourceAuthorityRecord,
    UniversityBootstrapCandidate,
    UniversityBootstrapResult,
)
from .repository import UniversityBootstrapRepository, deterministic_university_id

AUTHORITATIVE_TRUST_TIER = "authoritative"


class UniversityBootstrapError(ValueError):
    pass


class UniversityBootstrapService:
    def __init__(self, repository: UniversityBootstrapRepository) -> None:
        self._repository = repository

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
            university=university,
            claims_used=claim_result.claims,
            evidence_used=claim_result.evidence,
        )

    def _build_candidate(
        self,
        *,
        source: SourceAuthorityRecord,
        claim_result: ClaimBuildResult,
    ) -> UniversityBootstrapCandidate:
        claims_by_field = self._claims_by_field(claim_result.claims)
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
        if source.trust_tier != AUTHORITATIVE_TRUST_TIER:
            raise UniversityBootstrapError(
                f"Source {source.source_key} is not authoritative."
            )

    @staticmethod
    def _claims_by_field(claims: list[ClaimRecord]) -> dict[str, ClaimRecord]:
        result: dict[str, ClaimRecord] = {}
        for claim in sorted(claims, key=lambda item: item.parser_confidence, reverse=True):
            result.setdefault(claim.field_name, claim)
        return result

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
        return {
            "bootstrap_policy": "single_source_authoritative",
            "source_id": str(source.source_id),
            "source_key": source.source_key,
            "source_type": source.source_type,
            "trust_tier": source.trust_tier,
            "parsed_document_id": str(claim_result.parsed_document.parsed_document_id),
            "parser_version": claim_result.parsed_document.parser_version,
            "claim_ids": [str(claim.claim_id) for claim in claim_result.claims],
            "evidence_ids": [
                str(evidence.evidence_id) for evidence in claim_result.evidence
            ],
            "field_names": sorted({claim.field_name for claim in claim_result.claims}),
            "source_urls": sorted(
                UniversityBootstrapService._source_urls(claim_result.evidence)
            ),
        }

    @staticmethod
    def _source_urls(evidence: Iterable[ClaimEvidenceRecord]) -> set[str]:
        return {record.source_url for record in evidence}
