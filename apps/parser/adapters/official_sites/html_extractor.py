from __future__ import annotations

import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse

from libs.source_sdk import ExtractedFragment, FetchContext, FetchedArtifact

from .base import OfficialSiteFragmentExtractor

EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_PATTERN = re.compile(r"(?:\+?\d[\d\s().-]{7,}\d)")
WHITESPACE_PATTERN = re.compile(r"\s+")

CANONICAL_NAME_HINTS = {"canonical_name", "university-name", "institution-name", "org-name"}
CITY_HINTS = {"city", "location-city", "address-locality"}
ADDRESS_HINTS = {"address", "street-address", "postal-address"}
CONTACT_HINTS = {"contacts", "contact", "footer-contacts"}


def normalize_text(value: str) -> str:
    return WHITESPACE_PATTERN.sub(" ", value).strip()


def split_tokens(value: str | None) -> set[str]:
    if not value:
        return set()
    return {token for token in re.split(r"[\s_.:-]+", value.lower()) if token}


def normalize_phone(value: str) -> str:
    return normalize_text(value).strip(".,;")


def unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    unique_values: list[str] = []
    for value in values:
        normalized_value = normalize_text(value)
        if not normalized_value or normalized_value in seen:
            continue
        seen.add(normalized_value)
        unique_values.append(normalized_value)
    return unique_values


@dataclass
class HtmlElement:
    tag: str
    attrs: dict[str, str]
    text_parts: list[str] = field(default_factory=list)

    @property
    def text(self) -> str:
        return normalize_text(" ".join(self.text_parts))

    @property
    def tokens(self) -> set[str]:
        return split_tokens(self.attrs.get("class")) | split_tokens(self.attrs.get("id"))

    def locator(self) -> str:
        element_id = self.attrs.get("id")
        if element_id:
            return f"{self.tag}#{element_id}"
        element_class = self.attrs.get("class")
        if element_class:
            first_class = element_class.split()[0]
            return f"{self.tag}.{first_class}"
        return self.tag


class _OfficialSiteHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title_parts: list[str] = []
        self.meta: dict[str, str] = {}
        self.links: list[dict[str, str]] = []
        self.elements: list[HtmlElement] = []
        self._capture_stack: list[HtmlElement] = []
        self._title_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized_attrs = {key.lower(): value or "" for key, value in attrs}
        if tag == "title":
            self._title_depth += 1
            return
        if tag == "meta":
            key = (
                normalized_attrs.get("name")
                or normalized_attrs.get("property")
                or normalized_attrs.get("itemprop")
            )
            content = normalized_attrs.get("content")
            if key and content:
                self.meta[key.lower()] = normalize_text(content)
            return
        if tag == "a":
            self.links.append(normalized_attrs)
        if tag in {"h1", "h2", "p", "span", "div", "address", "a"}:
            self._capture_stack.append(HtmlElement(tag=tag, attrs=normalized_attrs))

    def handle_endtag(self, tag: str) -> None:
        if tag == "title" and self._title_depth:
            self._title_depth -= 1
            return
        if not self._capture_stack:
            return
        if self._capture_stack[-1].tag != tag:
            return
        element = self._capture_stack.pop()
        if element.text:
            self.elements.append(element)
        if self._capture_stack:
            self._capture_stack[-1].text_parts.append(element.text)

    def handle_data(self, data: str) -> None:
        text = normalize_text(data)
        if not text:
            return
        if self._title_depth:
            self.title_parts.append(text)
        if self._capture_stack:
            self._capture_stack[-1].text_parts.append(text)

    @property
    def title(self) -> str | None:
        title = normalize_text(" ".join(self.title_parts))
        return title or None


class OfficialSiteHtmlExtractor(OfficialSiteFragmentExtractor):
    supported_parser_profiles = ("official_site.default",)

    def extract(
        self,
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> list[ExtractedFragment]:
        parser = _OfficialSiteHtmlParser()
        parser.feed(self._decode_content(artifact))
        parser.close()

        fragments: list[ExtractedFragment] = []
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="canonical_name",
            value=self._canonical_name(parser),
            locator=self._canonical_name_locator(parser),
            confidence=0.92,
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="location.city",
            value=self._first_text_for_hints(parser.elements, CITY_HINTS),
            locator=self._first_locator_for_hints(parser.elements, CITY_HINTS),
            confidence=0.78,
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="location.address",
            value=self._first_text_for_hints(parser.elements, ADDRESS_HINTS),
            locator=self._first_locator_for_hints(parser.elements, ADDRESS_HINTS),
            confidence=0.78,
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="contacts.website",
            value=self._website(context=context, artifact=artifact, parser=parser),
            locator="canonical-link|endpoint_url",
            confidence=0.9,
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="contacts.emails",
            value=self._emails(parser),
            locator="mailto|text",
            confidence=0.88,
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="contacts.phones",
            value=self._phones(parser),
            locator="tel|contact-text",
            confidence=0.82,
        )
        return fragments

    @staticmethod
    def _decode_content(artifact: FetchedArtifact) -> str:
        if artifact.content is None:
            raise ValueError("Fetched artifact content is required for official-site extraction.")
        return artifact.content.decode("utf-8", errors="replace")

    @staticmethod
    def _append_fragment(
        fragments: list[ExtractedFragment],
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
        field_name: str,
        value: Any,
        locator: str | None,
        confidence: float,
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
                    "parser_profile": context.parser_profile,
                    "adapter_family": "official_sites",
                },
            )
        )

    @staticmethod
    def _canonical_name(parser: _OfficialSiteHtmlParser) -> str | None:
        hinted = OfficialSiteHtmlExtractor._first_text_for_hints(
            parser.elements,
            CANONICAL_NAME_HINTS,
        )
        if hinted:
            return hinted
        h1 = next((element.text for element in parser.elements if element.tag == "h1"), None)
        if h1:
            return h1
        site_name = parser.meta.get("og:site_name") or parser.meta.get("application-name")
        if site_name:
            return site_name
        if parser.title:
            return re.split(r"\s+[|-]\s+", parser.title, maxsplit=1)[0]
        return None

    @staticmethod
    def _canonical_name_locator(parser: _OfficialSiteHtmlParser) -> str | None:
        hinted = OfficialSiteHtmlExtractor._first_element_for_hints(
            parser.elements,
            CANONICAL_NAME_HINTS,
        )
        if hinted:
            return hinted.locator()
        h1 = next((element for element in parser.elements if element.tag == "h1"), None)
        if h1:
            return h1.locator()
        if parser.meta.get("og:site_name"):
            return 'meta[property="og:site_name"]'
        if parser.title:
            return "title"
        return None

    @staticmethod
    def _first_element_for_hints(
        elements: list[HtmlElement],
        hints: set[str],
    ) -> HtmlElement | None:
        return next((element for element in elements if element.tokens & hints), None)

    @staticmethod
    def _first_text_for_hints(elements: list[HtmlElement], hints: set[str]) -> str | None:
        element = OfficialSiteHtmlExtractor._first_element_for_hints(elements, hints)
        return element.text if element else None

    @staticmethod
    def _first_locator_for_hints(elements: list[HtmlElement], hints: set[str]) -> str | None:
        element = OfficialSiteHtmlExtractor._first_element_for_hints(elements, hints)
        return element.locator() if element else None

    @staticmethod
    def _website(
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
        parser: _OfficialSiteHtmlParser,
    ) -> str:
        canonical_href = next(
            (
                link.get("href")
                for link in parser.links
                if link.get("rel", "").lower() == "canonical" and link.get("href")
            ),
            None,
        )
        if canonical_href:
            return urljoin(artifact.final_url or artifact.source_url, canonical_href)
        parsed = urlparse(context.endpoint_url)
        return f"{parsed.scheme}://{parsed.netloc}" if parsed.netloc else context.endpoint_url

    @staticmethod
    def _emails(parser: _OfficialSiteHtmlParser) -> list[str]:
        values: list[str] = []
        for link in parser.links:
            href = link.get("href", "")
            if href.lower().startswith("mailto:"):
                values.append(href.split(":", 1)[1].split("?", 1)[0])
        values.extend(EMAIL_PATTERN.findall(" ".join(element.text for element in parser.elements)))
        return unique_preserve_order(values)

    @staticmethod
    def _phones(parser: _OfficialSiteHtmlParser) -> list[str]:
        values: list[str] = []
        for link in parser.links:
            href = link.get("href", "")
            if href.lower().startswith("tel:"):
                values.append(href.split(":", 1)[1])
        contact_text = " ".join(
            element.text for element in parser.elements if element.tokens & CONTACT_HINTS
        )
        values.extend(PHONE_PATTERN.findall(contact_text))
        return unique_preserve_order([normalize_phone(value) for value in values])
