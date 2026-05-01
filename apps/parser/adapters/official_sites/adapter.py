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

from .base import OfficialSiteFragmentExtractor
from .html_extractor import OfficialSiteHtmlExtractor
from .kubsu_abiturient_html_extractor import KubSUAbiturientHtmlExtractor
from .kubsu_programs_html_extractor import KubSUProgramsHtmlExtractor

OFFICIAL_SITE_SOURCE_KEY = "official_sites"
OFFICIAL_SITE_ADAPTER_VERSION = "0.1.0"


class OfficialSiteAdapter(SourceAdapter):
    source_key = OFFICIAL_SITE_SOURCE_KEY
    adapter_version = OFFICIAL_SITE_ADAPTER_VERSION
    supported_parser_profiles = (
        "official_site.default",
        "official_site.kubsu.abiturient_html",
        "official_site.kubsu.programs_html",
    )

    def __init__(
        self,
        *,
        fetcher: RawFetcher,
        raw_store: RawArtifactStore | None = None,
        extractor: OfficialSiteFragmentExtractor | None = None,
        extractors: tuple[OfficialSiteFragmentExtractor, ...] | None = None,
    ) -> None:
        self._fetcher = fetcher
        self._raw_store = raw_store
        self._extractors = self._build_extractors(
            extractor=extractor,
            extractors=extractors,
        )

    @property
    def adapter_key(self) -> str:
        return f"official_sites:{self.adapter_version}"

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
                "No official-site extractor is registered for "
                f"parser_profile={context.parser_profile}."
            )
        return extractor.extract(context=context, artifact=artifact)

    async def map_to_intermediate(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
        fragments: Sequence[ExtractedFragment],
    ) -> Sequence[IntermediateRecord]:
        records: list[IntermediateRecord] = []
        for group_key, grouped_fragments in self._group_fragments(fragments).items():
            claims = [
                self._claim_from_fragment(
                    context=context,
                    artifact=artifact,
                    fragment=fragment,
                )
                for fragment in grouped_fragments
            ]
            records.append(
                IntermediateRecord(
                    source_key=context.source_key,
                    entity_type=self._entity_type(grouped_fragments),
                    entity_hint=self._entity_hint(grouped_fragments),
                    claims=claims,
                    fragment_ids=[fragment.fragment_id for fragment in grouped_fragments],
                    metadata={
                        "adapter_key": self.adapter_key,
                        "adapter_version": self.adapter_version,
                        "parser_profile": context.parser_profile,
                        "raw_artifact_id": str(artifact.raw_artifact_id),
                        "record_group_key": group_key,
                    },
                )
            )
        return records

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
    def _group_fragments(
        fragments: Sequence[ExtractedFragment],
    ) -> dict[str, list[ExtractedFragment]]:
        groups: dict[str, list[ExtractedFragment]] = {}
        for fragment in fragments:
            raw_group_key = fragment.metadata.get("record_group_key")
            group_key = (
                str(raw_group_key).strip()
                if isinstance(raw_group_key, str) and raw_group_key.strip()
                else "__default__"
            )
            groups.setdefault(group_key, []).append(fragment)
        return groups

    @staticmethod
    def _entity_hint(fragments: Sequence[ExtractedFragment]) -> str | None:
        for field_name in ("canonical_name", "programs.name", "programs.code"):
            value = next(
                (
                    fragment.value
                    for fragment in fragments
                    if fragment.field_name == field_name
                ),
                None,
            )
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    @staticmethod
    def _entity_type(fragments: Sequence[ExtractedFragment]) -> str:
        for fragment in fragments:
            entity_type = fragment.metadata.get("entity_type")
            if isinstance(entity_type, str) and entity_type.strip():
                return entity_type.strip()
        return "university"

    @staticmethod
    def _build_extractors(
        *,
        extractor: OfficialSiteFragmentExtractor | None,
        extractors: tuple[OfficialSiteFragmentExtractor, ...] | None,
    ) -> tuple[OfficialSiteFragmentExtractor, ...]:
        if extractor is not None and extractors is not None:
            raise ValueError("Pass either extractor or extractors, not both.")
        if extractors is not None:
            return extractors
        if extractor is not None:
            return (extractor,)
        return (
            OfficialSiteHtmlExtractor(),
            KubSUAbiturientHtmlExtractor(),
            KubSUProgramsHtmlExtractor(),
        )

    def _resolve_extractor(
        self,
        context: FetchContext,
    ) -> OfficialSiteFragmentExtractor | None:
        return next(
            (
                extractor
                for extractor in self._extractors
                if extractor.can_handle(context)
            ),
            None,
        )
