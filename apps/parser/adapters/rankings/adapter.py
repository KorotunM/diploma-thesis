from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from libs.source_sdk import (
    ExtractedFragment,
    FetchContext,
    FetchedArtifact,
    IntermediateRecord,
    RawArtifactStore,
    RawFetcher,
    SourceAdapter,
)

from .payload_extractor import RankingPayloadExtractor

RANKING_ADAPTER_VERSION = "0.1.0"


class RankingAdapter(SourceAdapter):
    source_key = "rankings"
    adapter_version = RANKING_ADAPTER_VERSION
    supported_parser_profiles = ("ranking.default",)

    def __init__(
        self,
        *,
        fetcher: RawFetcher,
        raw_store: RawArtifactStore | None = None,
        extractor: RankingPayloadExtractor | None = None,
    ) -> None:
        self._fetcher = fetcher
        self._raw_store = raw_store
        self._extractor = extractor or RankingPayloadExtractor()

    @property
    def adapter_key(self) -> str:
        return f"rankings:{self.adapter_version}"

    def can_handle(self, context: FetchContext) -> bool:
        return context.parser_profile in self.supported_parser_profiles

    async def fetch(self, context: FetchContext) -> FetchedArtifact:
        return await self._fetcher.fetch(context)

    async def store_raw(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> FetchedArtifact:
        if self._raw_store is None:
            return await super().store_raw(context, artifact)
        return await self._raw_store.store_raw(context, artifact)

    async def extract(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> Sequence[ExtractedFragment]:
        return self._extractor.extract(context=context, artifact=artifact)

    async def map_to_intermediate(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
        fragments: Sequence[ExtractedFragment],
    ) -> Sequence[IntermediateRecord]:
        claims = [
            self._claim_from_fragment(
                context=context,
                artifact=artifact,
                fragment=fragment,
            )
            for fragment in fragments
        ]
        entity_hint = next(
            (
                fragment.value
                for fragment in fragments
                if fragment.field_name == "canonical_name"
            ),
            None,
        )
        provider_name = next(
            (
                fragment.metadata.get("provider_name")
                for fragment in fragments
                if fragment.field_name == "ratings.provider"
                and isinstance(fragment.metadata.get("provider_name"), str)
            ),
            None,
        )
        return [
            IntermediateRecord(
                source_key=context.source_key,
                entity_type="university",
                entity_hint=str(entity_hint) if entity_hint is not None else None,
                claims=claims,
                fragment_ids=[fragment.fragment_id for fragment in fragments],
                metadata={
                    "adapter_key": self.adapter_key,
                    "adapter_version": self.adapter_version,
                    "parser_profile": context.parser_profile,
                    "raw_artifact_id": str(artifact.raw_artifact_id),
                    "source_kind": "ranking",
                    "provider_name": provider_name,
                },
            )
        ]

    def _claim_from_fragment(
        self,
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
        fragment: ExtractedFragment,
    ) -> dict[str, Any]:
        return {
            "field_name": fragment.field_name,
            "value": fragment.value,
            "value_type": self._value_type(fragment.value),
            "source_key": context.source_key,
            "source_url": artifact.source_url,
            "raw_artifact_id": str(artifact.raw_artifact_id),
            "fragment_id": str(fragment.fragment_id),
            "parser_version": self.adapter_version,
            "parser_confidence": fragment.confidence,
            "locator": fragment.locator,
            "metadata": {
                **fragment.metadata,
                "adapter_key": self.adapter_key,
                "source_kind": "ranking",
            },
        }

    @staticmethod
    def _value_type(value: Any) -> str:
        if isinstance(value, list):
            return "list"
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "float"
        return "str"
