from __future__ import annotations

from enum import IntEnum, StrEnum

from pydantic import BaseModel, ConfigDict, Field


class SourceTrustTier(StrEnum):
    AUTHORITATIVE = "authoritative"
    TRUSTED = "trusted"
    AUXILIARY = "auxiliary"
    EXPERIMENTAL = "experimental"


class SourceTrustRank(IntEnum):
    AUTHORITATIVE = 0
    TRUSTED = 1
    AUXILIARY = 2
    EXPERIMENTAL = 3


TRUST_TIER_RANKS: dict[SourceTrustTier, SourceTrustRank] = {
    SourceTrustTier.AUTHORITATIVE: SourceTrustRank.AUTHORITATIVE,
    SourceTrustTier.TRUSTED: SourceTrustRank.TRUSTED,
    SourceTrustTier.AUXILIARY: SourceTrustRank.AUXILIARY,
    SourceTrustTier.EXPERIMENTAL: SourceTrustRank.EXPERIMENTAL,
}


class FieldResolutionStrategy(StrEnum):
    PICK_HIGHEST_CONFIDENCE = "pick_highest_confidence"
    PREFER_HIGHER_TRUST_TIER = "prefer_higher_trust_tier"
    UNION_BY_TRUST_TIER = "union_by_trust_tier"


class FieldResolutionPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field_name: str
    policy_name: str
    preferred_tiers: tuple[SourceTrustTier, ...] = Field(default_factory=tuple)
    allowed_tiers: tuple[SourceTrustTier, ...] = Field(default_factory=tuple)
    strategy: FieldResolutionStrategy
    multi_value: bool = False

    def allows(self, trust_tier: SourceTrustTier) -> bool:
        return trust_tier in self.allowed_tiers

    def preference_rank(self, trust_tier: SourceTrustTier) -> int:
        try:
            return self.preferred_tiers.index(trust_tier)
        except ValueError:
            return len(self.preferred_tiers) + int(TRUST_TIER_RANKS[trust_tier])
