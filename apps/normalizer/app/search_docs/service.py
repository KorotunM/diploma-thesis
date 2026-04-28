from __future__ import annotations

import re
from urllib.parse import urlparse

from libs.domain.university import UniversityCard

from .models import UniversitySearchDocRecord
from .repository import UniversitySearchDocProjectionRepository

WHITESPACE_RE = re.compile(r"\s+")


class UniversitySearchDocProjectionService:
    def __init__(self, repository: UniversitySearchDocProjectionRepository) -> None:
        self._repository = repository

    def refresh_for_card(
        self,
        card: UniversityCard,
    ) -> UniversitySearchDocRecord:
        search_doc = self._build_search_doc(card)
        return self._repository.upsert_search_doc(
            search_doc=search_doc,
            search_text_source=self._search_text_source(search_doc),
        )

    def _build_search_doc(
        self,
        card: UniversityCard,
    ) -> UniversitySearchDocRecord:
        canonical_name = self._clean_text(card.canonical_name.value)
        aliases = self._aliases(card.aliases)
        website_url = self._clean_text(card.contacts.website)
        website_domain = self._website_domain(website_url)
        country_code = self._clean_text(card.location.country)
        city_name = self._clean_text(card.location.city)
        source_keys = sorted({source.source_key for source in card.sources})
        search_document = {
            "canonical_name": canonical_name,
            "aliases": aliases,
            "website_url": website_url,
            "website_domain": website_domain,
            "country_code": country_code,
            "city_name": city_name,
            "ratings": [item.model_dump(mode="json") for item in card.ratings],
            "source_keys": source_keys,
        }
        metadata = {
            "projection_kind": "delivery.university_search_doc",
            "source_count": len(card.sources),
            "rating_count": len(card.ratings),
        }
        return UniversitySearchDocRecord(
            university_id=card.university_id,
            card_version=card.version.card_version,
            canonical_name=canonical_name or "",
            canonical_name_normalized=self._normalized_name(canonical_name),
            website_url=website_url,
            website_domain=website_domain,
            country_code=country_code,
            city_name=city_name,
            aliases=aliases,
            search_document=search_document,
            generated_at=card.version.generated_at,
            metadata=metadata,
        )

    def _search_text_source(
        self,
        search_doc: UniversitySearchDocRecord,
    ) -> str:
        terms: list[str] = [search_doc.canonical_name, search_doc.canonical_name_normalized]
        if search_doc.website_domain:
            terms.append(search_doc.website_domain)
        if search_doc.city_name:
            terms.append(search_doc.city_name)
        if search_doc.country_code:
            terms.append(search_doc.country_code)
        terms.extend(search_doc.aliases)
        terms.extend(
            str(value)
            for rating in search_doc.search_document.get("ratings", [])
            if isinstance(rating, dict)
            for value in rating.values()
            if value is not None
        )
        return " ".join(term for term in terms if term)

    @staticmethod
    def _clean_text(value: object) -> str | None:
        if not isinstance(value, str):
            return None
        normalized = WHITESPACE_RE.sub(" ", value).strip()
        return normalized or None

    def _normalized_name(self, value: str | None) -> str:
        if not value:
            return ""
        return self._clean_text(value.casefold()) or ""

    def _aliases(self, aliases: list[str]) -> list[str]:
        deduped: dict[str, str] = {}
        for alias in aliases:
            cleaned = self._clean_text(alias)
            if not cleaned:
                continue
            deduped.setdefault(cleaned.casefold(), cleaned)
        return [deduped[key] for key in sorted(deduped)]

    @staticmethod
    def _website_domain(website_url: str | None) -> str | None:
        if not website_url:
            return None
        parsed = urlparse(
            website_url if "://" in website_url else f"https://{website_url}"
        )
        hostname = parsed.hostname
        if hostname is None:
            return None
        return hostname.removeprefix("www.").lower()
