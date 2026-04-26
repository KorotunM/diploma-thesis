from __future__ import annotations

from collections import defaultdict
from uuid import UUID

from .models import (
    UniversityCardFieldAttribution,
    UniversityCardResponse,
    UniversityCardSourceRationale,
)
from .repository import UniversityCardReadRepository


class UniversityCardNotFoundError(LookupError):
    def __init__(self, university_id: UUID) -> None:
        super().__init__(f"University card {university_id} was not found.")
        self.university_id = university_id


class UniversityCardReadService:
    def __init__(self, repository: UniversityCardReadRepository) -> None:
        self._repository = repository

    def get_latest_card(self, university_id: UUID) -> UniversityCardResponse:
        record = self._repository.get_latest_by_university_id(university_id)
        if record is None:
            raise UniversityCardNotFoundError(university_id)
        facts = self._repository.list_resolved_facts(
            university_id=university_id,
            card_version=record.card_version,
        )
        field_attribution = {
            fact.field_name: self._field_attribution(fact)
            for fact in facts
        }
        source_rationale = self._source_rationale(
            attributions=list(field_attribution.values())
        )
        return UniversityCardResponse.model_validate(
            {
                **record.card.model_dump(mode="python"),
                "field_attribution": field_attribution,
                "source_rationale": source_rationale,
            }
        )

    def _field_attribution(self, fact) -> UniversityCardFieldAttribution:
        metadata = fact.metadata
        source_key = metadata.get("source_key")
        source_trust_tier = metadata.get("source_trust_tier")
        preferred_tiers = metadata.get("preferred_trust_tiers")
        resolution_strategy = metadata.get("resolution_strategy")
        source_keys = metadata.get("source_keys")
        rationale = self._rationale_text(
            source_key=source_key,
            source_trust_tier=source_trust_tier,
            resolution_policy=fact.resolution_policy,
            resolution_strategy=resolution_strategy,
            preferred_tiers=preferred_tiers,
            source_keys=source_keys,
        )
        return UniversityCardFieldAttribution(
            field_name=fact.field_name,
            source_key=source_key if isinstance(source_key, str) else None,
            source_trust_tier=(
                source_trust_tier if isinstance(source_trust_tier, str) else None
            ),
            source_urls=sorted(
                source_url
                for source_url in metadata.get("source_urls", [])
                if isinstance(source_url, str)
            ),
            selected_claim_ids=fact.selected_claim_ids,
            selected_evidence_ids=fact.selected_evidence_ids,
            resolution_policy=fact.resolution_policy,
            resolution_strategy=(
                resolution_strategy if isinstance(resolution_strategy, str) else None
            ),
            rationale=rationale,
        )

    def _source_rationale(
        self,
        *,
        attributions: list[UniversityCardFieldAttribution],
    ) -> list[UniversityCardSourceRationale]:
        grouped_fields: dict[tuple[str, str | None], list[UniversityCardFieldAttribution]] = (
            defaultdict(list)
        )
        for attribution in attributions:
            if attribution.source_key is None:
                continue
            grouped_fields[
                (attribution.source_key, attribution.source_trust_tier)
            ].append(attribution)

        result: list[UniversityCardSourceRationale] = []
        for (source_key, trust_tier), group in sorted(grouped_fields.items()):
            selected_fields = sorted({item.field_name for item in group})
            source_urls = sorted(
                {
                    source_url
                    for item in group
                    for source_url in item.source_urls
                }
            )
            result.append(
                UniversityCardSourceRationale(
                    source_key=source_key,
                    trust_tier=trust_tier,
                    selected_fields=selected_fields,
                    source_urls=source_urls,
                    rationale=self._source_summary_text(
                        source_key=source_key,
                        trust_tier=trust_tier,
                        selected_fields=selected_fields,
                    ),
                )
            )
        return result

    @staticmethod
    def _rationale_text(
        *,
        source_key,
        source_trust_tier,
        resolution_policy: str,
        resolution_strategy,
        preferred_tiers,
        source_keys,
    ) -> str:
        parts = [resolution_policy]
        if isinstance(resolution_strategy, str):
            parts.append(f"strategy={resolution_strategy}")
        if isinstance(source_key, str):
            parts.append(f"winner={source_key}")
        if isinstance(source_trust_tier, str):
            parts.append(f"tier={source_trust_tier}")
        if isinstance(preferred_tiers, list) and preferred_tiers:
            parts.append(f"preferred={','.join(str(item) for item in preferred_tiers)}")
        if isinstance(source_keys, list) and len(source_keys) > 1:
            parts.append(f"contenders={','.join(str(item) for item in source_keys)}")
        return "; ".join(parts)

    @staticmethod
    def _source_summary_text(
        *,
        source_key: str,
        trust_tier: str | None,
        selected_fields: list[str],
    ) -> str:
        fields = ",".join(selected_fields)
        if trust_tier is None:
            return f"{source_key} selected for fields {fields}"
        return f"{source_key} ({trust_tier}) selected for fields {fields}"
