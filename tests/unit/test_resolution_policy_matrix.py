from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from apps.normalizer.app.claims import ClaimRecord
from apps.normalizer.app.resolution import (
    CANONICAL_FIELD_POLICY,
    SUPPORTING_FIELD_POLICY,
    FieldResolutionPolicyMatrix,
    SourceTrustTier,
)


def claim(
    *,
    source_key: str,
    field_name: str,
    value,
    confidence: float,
) -> ClaimRecord:
    return ClaimRecord(
        claim_id=uuid4(),
        parsed_document_id=uuid4(),
        source_key=source_key,
        field_name=field_name,
        value=value,
        value_type="str",
        entity_hint="Example University",
        parser_version="0.1.0",
        normalizer_version="normalizer.0.1.0",
        parser_confidence=confidence,
        created_at=datetime(2026, 4, 26, 12, 0, tzinfo=UTC),
        metadata={},
    )


def test_policy_matrix_prefers_higher_trust_tier_over_higher_confidence() -> None:
    matrix = FieldResolutionPolicyMatrix()
    claims = [
        claim(
            source_key="aggregator",
            field_name="canonical_name",
            value="Example University Directory",
            confidence=0.99,
        ),
        claim(
            source_key="official-site",
            field_name="canonical_name",
            value="Example University",
            confidence=0.91,
        ),
    ]

    selected = matrix.select_best_claim(
        field_name="canonical_name",
        claims=claims,
        source_tiers={
            "official-site": SourceTrustTier.AUTHORITATIVE,
            "aggregator": SourceTrustTier.TRUSTED,
        },
    )

    assert selected is not None
    assert selected.source_key == "official-site"
    assert selected.value == "Example University"
    assert matrix.policy_for("canonical_name").policy_name == CANONICAL_FIELD_POLICY


def test_policy_matrix_exposes_supporting_union_policy_for_secondary_fields() -> None:
    matrix = FieldResolutionPolicyMatrix()

    policy = matrix.policy_for("contacts.emails")

    assert policy.policy_name == SUPPORTING_FIELD_POLICY
    assert policy.multi_value is True
    assert policy.allowed_tiers == (
        SourceTrustTier.TRUSTED,
        SourceTrustTier.AUTHORITATIVE,
        SourceTrustTier.AUXILIARY,
        SourceTrustTier.EXPERIMENTAL,
    )
