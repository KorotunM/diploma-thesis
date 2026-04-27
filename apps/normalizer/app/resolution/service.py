from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

from apps.normalizer.app.claims import ClaimRecord

from .models import (
    FieldResolutionPolicy,
    FieldResolutionStrategy,
    SourceTrustTier,
)

AUTHORITATIVE_FIRST_TIERS = (
    SourceTrustTier.AUTHORITATIVE,
    SourceTrustTier.TRUSTED,
    SourceTrustTier.AUXILIARY,
    SourceTrustTier.EXPERIMENTAL,
)
TRUSTED_FIRST_TIERS = (
    SourceTrustTier.TRUSTED,
    SourceTrustTier.AUTHORITATIVE,
    SourceTrustTier.AUXILIARY,
    SourceTrustTier.EXPERIMENTAL,
)
RANKING_FIRST_TIERS = (
    SourceTrustTier.TRUSTED,
    SourceTrustTier.AUTHORITATIVE,
    SourceTrustTier.AUXILIARY,
    SourceTrustTier.EXPERIMENTAL,
)
SINGLE_SOURCE_AUTHORITATIVE_POLICY = "single_source_authoritative"
CANONICAL_FIELD_POLICY = "tiered_authority_highest_confidence"
SUPPORTING_FIELD_POLICY = "tiered_supporting_union"
RATING_FIELD_POLICY = "ranking_provider_highest_confidence"

DEFAULT_FIELD_POLICIES: tuple[FieldResolutionPolicy, ...] = (
    FieldResolutionPolicy(
        field_name="canonical_name",
        policy_name=CANONICAL_FIELD_POLICY,
        preferred_tiers=AUTHORITATIVE_FIRST_TIERS,
        allowed_tiers=AUTHORITATIVE_FIRST_TIERS,
        strategy=FieldResolutionStrategy.PREFER_HIGHER_TRUST_TIER,
    ),
    FieldResolutionPolicy(
        field_name="contacts.website",
        policy_name=CANONICAL_FIELD_POLICY,
        preferred_tiers=AUTHORITATIVE_FIRST_TIERS,
        allowed_tiers=AUTHORITATIVE_FIRST_TIERS,
        strategy=FieldResolutionStrategy.PREFER_HIGHER_TRUST_TIER,
    ),
    FieldResolutionPolicy(
        field_name="location.city",
        policy_name=CANONICAL_FIELD_POLICY,
        preferred_tiers=AUTHORITATIVE_FIRST_TIERS,
        allowed_tiers=AUTHORITATIVE_FIRST_TIERS,
        strategy=FieldResolutionStrategy.PREFER_HIGHER_TRUST_TIER,
    ),
    FieldResolutionPolicy(
        field_name="location.country_code",
        policy_name=CANONICAL_FIELD_POLICY,
        preferred_tiers=AUTHORITATIVE_FIRST_TIERS,
        allowed_tiers=AUTHORITATIVE_FIRST_TIERS,
        strategy=FieldResolutionStrategy.PREFER_HIGHER_TRUST_TIER,
    ),
    FieldResolutionPolicy(
        field_name="aliases",
        policy_name=SUPPORTING_FIELD_POLICY,
        preferred_tiers=TRUSTED_FIRST_TIERS,
        allowed_tiers=TRUSTED_FIRST_TIERS,
        strategy=FieldResolutionStrategy.UNION_BY_TRUST_TIER,
        multi_value=True,
    ),
    FieldResolutionPolicy(
        field_name="contacts.emails",
        policy_name=SUPPORTING_FIELD_POLICY,
        preferred_tiers=TRUSTED_FIRST_TIERS,
        allowed_tiers=TRUSTED_FIRST_TIERS,
        strategy=FieldResolutionStrategy.UNION_BY_TRUST_TIER,
        multi_value=True,
    ),
    FieldResolutionPolicy(
        field_name="contacts.phones",
        policy_name=SUPPORTING_FIELD_POLICY,
        preferred_tiers=TRUSTED_FIRST_TIERS,
        allowed_tiers=TRUSTED_FIRST_TIERS,
        strategy=FieldResolutionStrategy.UNION_BY_TRUST_TIER,
        multi_value=True,
    ),
    FieldResolutionPolicy(
        field_name="ratings.provider",
        policy_name=RATING_FIELD_POLICY,
        preferred_tiers=RANKING_FIRST_TIERS,
        allowed_tiers=RANKING_FIRST_TIERS,
        strategy=FieldResolutionStrategy.PREFER_HIGHER_TRUST_TIER,
    ),
    FieldResolutionPolicy(
        field_name="ratings.year",
        policy_name=RATING_FIELD_POLICY,
        preferred_tiers=RANKING_FIRST_TIERS,
        allowed_tiers=RANKING_FIRST_TIERS,
        strategy=FieldResolutionStrategy.PREFER_HIGHER_TRUST_TIER,
    ),
    FieldResolutionPolicy(
        field_name="ratings.metric",
        policy_name=RATING_FIELD_POLICY,
        preferred_tiers=RANKING_FIRST_TIERS,
        allowed_tiers=RANKING_FIRST_TIERS,
        strategy=FieldResolutionStrategy.PREFER_HIGHER_TRUST_TIER,
    ),
    FieldResolutionPolicy(
        field_name="ratings.value",
        policy_name=RATING_FIELD_POLICY,
        preferred_tiers=RANKING_FIRST_TIERS,
        allowed_tiers=RANKING_FIRST_TIERS,
        strategy=FieldResolutionStrategy.PREFER_HIGHER_TRUST_TIER,
    ),
)


class FieldResolutionPolicyMatrix:
    def __init__(
        self,
        policies: Sequence[FieldResolutionPolicy] = DEFAULT_FIELD_POLICIES,
    ) -> None:
        self._policies = {policy.field_name: policy for policy in policies}

    def policy_for(self, field_name: str) -> FieldResolutionPolicy:
        policy = self._policies.get(field_name)
        if policy is not None:
            return policy
        return FieldResolutionPolicy(
            field_name=field_name,
            policy_name=CANONICAL_FIELD_POLICY,
            preferred_tiers=AUTHORITATIVE_FIRST_TIERS,
            allowed_tiers=AUTHORITATIVE_FIRST_TIERS,
            strategy=FieldResolutionStrategy.PREFER_HIGHER_TRUST_TIER,
        )

    def select_best_claim(
        self,
        *,
        field_name: str,
        claims: Iterable[ClaimRecord],
        source_tiers: Mapping[str, SourceTrustTier],
    ) -> ClaimRecord | None:
        policy = self.policy_for(field_name)
        eligible_claims = [
            claim
            for claim in claims
            if claim.value is not None and policy.allows(source_tiers[claim.source_key])
        ]
        if not eligible_claims:
            return None
        ranked_claims = sorted(
            eligible_claims,
            key=lambda claim: (
                policy.preference_rank(source_tiers[claim.source_key]),
                -claim.parser_confidence,
                claim.created_at,
                str(claim.claim_id),
            ),
        )
        return ranked_claims[0]


def source_tier_map(
    source_tiers: Mapping[str, SourceTrustTier] | None = None,
    *,
    default_source_key: str | None = None,
    default_trust_tier: SourceTrustTier | None = None,
) -> dict[str, SourceTrustTier]:
    resolved = dict(source_tiers or {})
    if default_source_key is not None and default_trust_tier is not None:
        resolved.setdefault(default_source_key, default_trust_tier)
    return resolved
