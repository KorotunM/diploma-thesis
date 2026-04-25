from __future__ import annotations

import json
from typing import Any

from libs.source_sdk import ExtractedFragment, FetchContext, FetchedArtifact


class AggregatorPayloadExtractor:
    def extract(
        self,
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> list[ExtractedFragment]:
        payload = self._decode_payload(artifact)
        university = self._university_payload(payload)
        provider_name = self._provider_name(payload)

        fragments: list[ExtractedFragment] = []
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="canonical_name",
            value=self._string(
                university.get("display_name")
                or university.get("name")
                or payload.get("display_name")
                or payload.get("name"),
            ),
            locator="$.display_name|$.name|$.university.display_name|$.university.name",
            confidence=0.94,
            metadata=self._metadata(
                provider_name=provider_name,
                source_field="aggregator.display_name",
                external_id=self._string(
                    university.get("external_id")
                    or university.get("id")
                    or payload.get("external_id"),
                ),
            ),
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="aliases",
            value=self._string_list(university.get("aliases") or payload.get("aliases")),
            locator="$.aliases|$.university.aliases",
            confidence=0.84,
            metadata=self._metadata(
                provider_name=provider_name,
                source_field="aggregator.aliases",
            ),
        )

        location = self._mapping(university.get("location"))
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="location.city",
            value=self._string(location.get("city") or university.get("city")),
            locator="$.location.city|$.university.location.city|$.university.city",
            confidence=0.9,
            metadata=self._metadata(
                provider_name=provider_name,
                source_field="aggregator.city",
            ),
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="location.country_code",
            value=self._string(
                location.get("country_code")
                or location.get("country")
                or university.get("country_code")
                or university.get("country")
            ),
            locator=(
                "$.location.country_code|$.location.country|"
                "$.university.location.country_code|$.university.country_code"
            ),
            confidence=0.86,
            metadata=self._metadata(
                provider_name=provider_name,
                source_field="aggregator.country_code",
            ),
        )

        contacts = self._mapping(university.get("contacts"))
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="contacts.website",
            value=self._string(
                university.get("official_website")
                or contacts.get("website")
                or university.get("website")
            ),
            locator=(
                "$.official_website|$.website|$.contacts.website|"
                "$.university.official_website|$.university.contacts.website"
            ),
            confidence=0.88,
            metadata=self._metadata(
                provider_name=provider_name,
                source_field="aggregator.official_website",
            ),
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="contacts.emails",
            value=self._string_list(contacts.get("emails") or university.get("emails")),
            locator="$.contacts.emails|$.university.contacts.emails|$.university.emails",
            confidence=0.82,
            metadata=self._metadata(
                provider_name=provider_name,
                source_field="aggregator.contacts.emails",
            ),
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="contacts.phones",
            value=self._string_list(contacts.get("phones") or university.get("phones")),
            locator="$.contacts.phones|$.university.contacts.phones|$.university.phones",
            confidence=0.8,
            metadata=self._metadata(
                provider_name=provider_name,
                source_field="aggregator.contacts.phones",
            ),
        )
        return fragments

    @staticmethod
    def _decode_payload(artifact: FetchedArtifact) -> dict[str, Any]:
        if artifact.content is None:
            raise ValueError("Fetched artifact content is required for aggregator extraction.")
        payload = json.loads(artifact.content.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Aggregator payload must decode to a JSON object.")
        return payload

    @staticmethod
    def _university_payload(payload: dict[str, Any]) -> dict[str, Any]:
        university = payload.get("university")
        if isinstance(university, dict):
            return university
        return payload

    @staticmethod
    def _provider_name(payload: dict[str, Any]) -> str | None:
        provider = payload.get("provider")
        if isinstance(provider, dict):
            name = provider.get("name")
            return str(name).strip() if name else None
        if isinstance(provider, str):
            return provider.strip() or None
        return None

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
                    "adapter_family": "aggregators",
                },
            )
        )

    @staticmethod
    def _metadata(
        *,
        provider_name: str | None,
        source_field: str,
        external_id: str | None = None,
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {"source_field": source_field}
        if provider_name is not None:
            metadata["provider_name"] = provider_name
        if external_id is not None:
            metadata["external_id"] = external_id
        return metadata

    @staticmethod
    def _mapping(value: Any) -> dict[str, Any]:
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _string(value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @classmethod
    def _string_list(cls, value: Any) -> list[str]:
        if isinstance(value, list):
            result: list[str] = []
            for item in value:
                normalized = cls._string(item)
                if normalized and normalized not in result:
                    result.append(normalized)
            return result
        normalized = cls._string(value)
        return [normalized] if normalized else []
