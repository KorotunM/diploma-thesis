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

from .base import AggregatorFragmentExtractor
from .payload_extractor import AggregatorPayloadExtractor
from .tabiturient_html_extractor import TabiturientUniversityHtmlExtractor

AGGREGATOR_ADAPTER_VERSION = "0.1.0"


class AggregatorAdapter(SourceAdapter):
    source_key = "aggregators"
    adapter_version = AGGREGATOR_ADAPTER_VERSION
    supported_parser_profiles = (
        "aggregator.default",
        "aggregator.tabiturient.university_html",
    )

    def __init__(
        self,
        *,
        fetcher: RawFetcher,
        raw_store: RawArtifactStore | None = None,
        extractor: AggregatorFragmentExtractor | None = None,
        extractors: tuple[AggregatorFragmentExtractor, ...] | None = None,
    ) -> None:
        self._fetcher = fetcher
        self._raw_store = raw_store
        self._extractors = self._build_extractors(
            extractor=extractor,
            extractors=extractors,
        )

    @property
    def adapter_key(self) -> str:
        return f"aggregators:{self.adapter_version}"

    def can_handle(self, context: FetchContext) -> bool:
        return self._resolve_extractor(context) is not None

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
        extractor = self._resolve_extractor(context)
        if extractor is None:
            raise ValueError(
                "No aggregator extractor is registered for "
                f"parser_profile={context.parser_profile}."
            )
        return extractor.extract(context=context, artifact=artifact)

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
                    "source_kind": "secondary",
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
                "source_kind": "secondary",
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

    @staticmethod
    def _build_extractors(
        *,
        extractor: AggregatorFragmentExtractor | None,
        extractors: tuple[AggregatorFragmentExtractor, ...] | None,
    ) -> tuple[AggregatorFragmentExtractor, ...]:
        if extractor is not None and extractors is not None:
            raise ValueError("Pass either extractor or extractors, not both.")
        if extractors is not None:
            return extractors
        if extractor is not None:
            return (extractor,)
        return (
            AggregatorPayloadExtractor(),
            TabiturientUniversityHtmlExtractor(),
        )

    def _resolve_extractor(
        self,
        context: FetchContext,
    ) -> AggregatorFragmentExtractor | None:
        return next(
            (
                extractor
                for extractor in self._extractors
                if extractor.can_handle(context)
            ),
            None,
        )
