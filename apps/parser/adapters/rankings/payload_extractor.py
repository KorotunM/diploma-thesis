from __future__ import annotations

import json
from typing import Any

from libs.source_sdk import ExtractedFragment, FetchContext, FetchedArtifact


class RankingPayloadExtractor:
    def extract(
        self,
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> list[ExtractedFragment]:
        payload = self._decode_payload(artifact)
        entry = self._entry_payload(payload)
        university = self._university_payload(entry)
        provider_name = self._provider_name(payload, entry)
        provider_key = self._provider_key(payload, entry)
        rating_item_key = self._rating_item_key(
            provider_key=provider_key,
            entry=entry,
            university=university,
        )

        fragments: list[ExtractedFragment] = []
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="canonical_name",
            value=self._string(
                university.get("display_name")
                or university.get("name")
                or entry.get("display_name")
                or entry.get("name"),
            ),
            locator=(
                "$.ranking_entry.university.display_name|$.ranking_entry.university.name|"
                "$.ranking_entry.display_name|$.ranking_entry.name"
            ),
            confidence=0.9,
            metadata=self._metadata(
                provider_name=provider_name,
                provider_key=provider_key,
                source_field="ranking.university.display_name",
                rating_item_key=rating_item_key,
            ),
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="contacts.website",
            value=self._string(
                university.get("official_website")
                or university.get("website")
                or entry.get("official_website")
            ),
            locator=(
                "$.ranking_entry.university.official_website|"
                "$.ranking_entry.university.website|$.ranking_entry.official_website"
            ),
            confidence=0.86,
            metadata=self._metadata(
                provider_name=provider_name,
                provider_key=provider_key,
                source_field="ranking.university.official_website",
                rating_item_key=rating_item_key,
            ),
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="location.country_code",
            value=self._string(
                university.get("country_code")
                or university.get("country")
                or entry.get("country_code")
            ),
            locator=(
                "$.ranking_entry.university.country_code|"
                "$.ranking_entry.university.country|$.ranking_entry.country_code"
            ),
            confidence=0.82,
            metadata=self._metadata(
                provider_name=provider_name,
                provider_key=provider_key,
                source_field="ranking.university.country_code",
                rating_item_key=rating_item_key,
            ),
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="ratings.provider",
            value=provider_name,
            locator="$.provider.name|$.ranking_entry.provider.name|$.provider",
            confidence=0.99,
            metadata=self._metadata(
                provider_name=provider_name,
                provider_key=provider_key,
                source_field="ranking.provider",
                rating_item_key=rating_item_key,
            ),
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="ratings.year",
            value=self._int(entry.get("year") or payload.get("year")),
            locator="$.ranking_entry.year|$.year",
            confidence=0.99,
            metadata=self._metadata(
                provider_name=provider_name,
                provider_key=provider_key,
                source_field="ranking.year",
                rating_item_key=rating_item_key,
            ),
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="ratings.metric",
            value=self._string(entry.get("metric") or payload.get("metric")),
            locator="$.ranking_entry.metric|$.metric",
            confidence=0.95,
            metadata=self._metadata(
                provider_name=provider_name,
                provider_key=provider_key,
                source_field="ranking.metric",
                rating_item_key=rating_item_key,
                scale=self._string(entry.get("scale") or payload.get("scale")),
            ),
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="ratings.value",
            value=self._string(entry.get("value") or payload.get("value")),
            locator="$.ranking_entry.value|$.value",
            confidence=0.96,
            metadata=self._metadata(
                provider_name=provider_name,
                provider_key=provider_key,
                source_field="ranking.value",
                rating_item_key=rating_item_key,
                rank_display=self._string(
                    entry.get("rank_display") or payload.get("rank_display")
                ),
                scale=self._string(entry.get("scale") or payload.get("scale")),
            ),
        )
        return fragments

    @staticmethod
    def _decode_payload(artifact: FetchedArtifact) -> dict[str, Any]:
        if artifact.content is None:
            raise ValueError("Fetched artifact content is required for ranking extraction.")
        payload = json.loads(artifact.content.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Ranking payload must decode to a JSON object.")
        return payload

    @staticmethod
    def _entry_payload(payload: dict[str, Any]) -> dict[str, Any]:
        ranking_entry = payload.get("ranking_entry")
        if isinstance(ranking_entry, dict):
            return ranking_entry
        return payload

    @staticmethod
    def _university_payload(entry: dict[str, Any]) -> dict[str, Any]:
        university = entry.get("university")
        if isinstance(university, dict):
            return university
        return entry

    @staticmethod
    def _provider_name(payload: dict[str, Any], entry: dict[str, Any]) -> str | None:
        provider = entry.get("provider")
        if isinstance(provider, dict):
            name = provider.get("name")
            if name:
                return str(name).strip() or None
        provider = payload.get("provider")
        if isinstance(provider, dict):
            name = provider.get("name")
            if name:
                return str(name).strip() or None
        if isinstance(provider, str):
            return provider.strip() or None
        return None

    @staticmethod
    def _provider_key(payload: dict[str, Any], entry: dict[str, Any]) -> str | None:
        provider = entry.get("provider")
        if isinstance(provider, dict):
            key = provider.get("key")
            if key:
                return str(key).strip() or None
        provider = payload.get("provider")
        if isinstance(provider, dict):
            key = provider.get("key")
            if key:
                return str(key).strip() or None
        return None

    @classmethod
    def _rating_item_key(
        cls,
        *,
        provider_key: str | None,
        entry: dict[str, Any],
        university: dict[str, Any],
    ) -> str:
        explicit = cls._string(entry.get("entry_id") or entry.get("id"))
        if explicit is not None:
            return explicit
        provider = provider_key or "ranking"
        year = cls._string(entry.get("year")) or "unknown-year"
        metric = cls._string(entry.get("metric")) or "unknown-metric"
        name = cls._string(university.get("display_name") or university.get("name")) or "unknown"
        slug = (
            name.lower()
            .replace(" ", "-")
            .replace("/", "-")
            .replace("_", "-")
        )
        return f"{provider}:{year}:{metric}:{slug}"

    @staticmethod
    def _append_fragment(
        fragments: list[ExtractedFragment],
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
        field_name: str,
        value: Any,
        locator: str,
        confidence: float,
        metadata: dict[str, Any],
    ) -> None:
        if value is None or value == "":
            return
        if isinstance(value, list) and not value:
            return
        fragments.append(
            ExtractedFragment(
                raw_artifact_id=artifact.raw_artifact_id,
                source_key=context.source_key,
                source_url=artifact.source_url,
                field_name=field_name,
                value=value,
                locator=locator,
                confidence=confidence,
                metadata={
                    **metadata,
                    "parser_profile": context.parser_profile,
                    "adapter_family": "rankings",
                },
            )
        )

    @staticmethod
    def _metadata(
        *,
        provider_name: str | None,
        provider_key: str | None,
        source_field: str,
        rating_item_key: str,
        rank_display: str | None = None,
        scale: str | None = None,
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "source_field": source_field,
            "rating_item_key": rating_item_key,
        }
        if provider_name is not None:
            metadata["provider_name"] = provider_name
        if provider_key is not None:
            metadata["provider_key"] = provider_key
        if rank_display is not None:
            metadata["rank_display"] = rank_display
        if scale is not None:
            metadata["scale"] = scale
        return metadata

    @staticmethod
    def _string(value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @classmethod
    def _int(cls, value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, int):
            return value
        normalized = cls._string(value)
        if normalized is None:
            return None
        return int(normalized)
