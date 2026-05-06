from __future__ import annotations

import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlparse

from libs.source_sdk import ExtractedFragment, FetchContext, FetchedArtifact

from .base import AggregatorFragmentExtractor

WHITESPACE_PATTERN = re.compile(r"\s+")

# Russian date pattern: "29 марта 2026"
_DATE_PATTERN = re.compile(
    r"\b(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})\b",
    re.IGNORECASE,
)
_AUTHOR_TYPE_PATTERN = re.compile(r"(Студент|Выпускник|Абитуриент)[^\n]{0,30}вуза", re.IGNORECASE)
_RATING_PATTERN = re.compile(r"(\d+[.,]\d+)\s*/\s*10")
_RATING_COUNT_PATTERN = re.compile(r"([\d\s]+)\s+оцен[оки]+")


def normalize_text(value: str) -> str:
    return WHITESPACE_PATTERN.sub(" ", value).strip()


def unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = normalize_text(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


@dataclass
class HtmlNode:
    tag: str
    attrs: dict[str, str]
    text_parts: list[str] = field(default_factory=list)

    @property
    def text(self) -> str:
        return normalize_text(" ".join(self.text_parts))

    def locator(self) -> str:
        itemprop = self.attrs.get("itemprop")
        if itemprop:
            return f'{self.tag}[itemprop="{itemprop}"]'
        element_class = self.attrs.get("class")
        if element_class:
            return f"{self.tag}.{element_class.split()[0]}"
        return self.tag


class _TabiturientUniversityHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.nodes: list[HtmlNode] = []
        self._stack: list[HtmlNode] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized_attrs = {key.lower(): value or "" for key, value in attrs}
        if tag in {"h1", "h2", "a", "span", "div", "td", "b"}:
            self._stack.append(HtmlNode(tag=tag, attrs=normalized_attrs))

    def handle_endtag(self, tag: str) -> None:
        if not self._stack or self._stack[-1].tag != tag:
            return
        node = self._stack.pop()
        if node.text or node.attrs:
            self.nodes.append(node)
        if self._stack and node.text:
            self._stack[-1].text_parts.append(node.text)

    def handle_data(self, data: str) -> None:
        text = normalize_text(data)
        if not text or not self._stack:
            return
        self._stack[-1].text_parts.append(text)


class TabiturientUniversityHtmlExtractor(AggregatorFragmentExtractor):
    supported_parser_profiles = ("aggregator.tabiturient.university_html",)

    def extract(
        self,
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> list[ExtractedFragment]:
        parser = _TabiturientUniversityHtmlParser()
        parser.feed(self._decode_content(artifact))
        parser.close()

        canonical_name = self._first_text_for_itemprop(parser.nodes, "name")
        alias = self._first_text_for_itemprop(parser.nodes, "alternateName")
        official_website = self._official_website(parser.nodes)
        slug = self._endpoint_slug(context.endpoint_url)

        fragments: list[ExtractedFragment] = []
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="canonical_name",
            value=canonical_name,
            locator=self._first_locator_for_itemprop(parser.nodes, "name"),
            confidence=0.96,
            metadata=self._metadata(
                source_field="tabiturient.itemprop.name",
                external_id=slug,
            ),
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="aliases",
            value=self._aliases(alias=alias, canonical_name=canonical_name),
            locator=self._first_locator_for_itemprop(parser.nodes, "alternateName"),
            confidence=0.88,
            metadata=self._metadata(
                source_field="tabiturient.itemprop.alternateName",
                external_id=slug,
            ),
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="contacts.website",
            value=official_website,
            locator=self._first_locator_for_itemprop(parser.nodes, "sameAs"),
            confidence=0.92,
            metadata=self._metadata(
                source_field="tabiturient.itemprop.sameAs",
                external_id=slug,
            ),
        )

        # ── Reviews ──────────────────────────────────────────────────────────
        decoded = self._decode_content(artifact)
        reviews = self._extract_reviews(decoded)
        if reviews:
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="reviews.items",
                value=reviews,
                locator="div.review-item",
                confidence=0.78,
                metadata=self._metadata(
                    source_field="tabiturient.reviews",
                    external_id=slug,
                ),
            )

        # ── User rating from header block ─────────────────────────────────────
        rating = self._extract_rating(decoded)
        if rating is not None:
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="reviews.rating",
                value=rating,
                locator="div.rating-score",
                confidence=0.92,
                metadata=self._metadata(
                    source_field="tabiturient.user_rating",
                    external_id=slug,
                ),
            )

        return fragments

    @staticmethod
    def _decode_content(artifact: FetchedArtifact) -> str:
        if artifact.content is None:
            raise ValueError(
                "Fetched artifact content is required for tabiturient HTML extraction."
            )
        return artifact.content.decode("utf-8", errors="replace")

    @staticmethod
    def _first_text_for_itemprop(nodes: list[HtmlNode], itemprop: str) -> str | None:
        node = next(
            (value for value in nodes if value.attrs.get("itemprop") == itemprop and value.text),
            None,
        )
        return node.text if node is not None else None

    @staticmethod
    def _first_locator_for_itemprop(nodes: list[HtmlNode], itemprop: str) -> str | None:
        node = next(
            (value for value in nodes if value.attrs.get("itemprop") == itemprop),
            None,
        )
        return node.locator() if node is not None else None

    @staticmethod
    def _official_website(nodes: list[HtmlNode]) -> str | None:
        for node in nodes:
            if node.tag != "a":
                continue
            if node.attrs.get("itemprop") != "sameAs":
                continue
            href = node.attrs.get("href", "").strip()
            if not href:
                continue
            parsed = urlparse(href)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                continue
            return href.rstrip("/")
        return None

    @staticmethod
    def _aliases(*, alias: str | None, canonical_name: str | None) -> list[str]:
        if alias is None:
            return []
        values = unique_preserve_order([alias])
        if canonical_name is not None:
            values = [value for value in values if value != canonical_name]
        return values

    @staticmethod
    def _endpoint_slug(endpoint_url: str) -> str | None:
        path_parts = [value for value in urlparse(endpoint_url).path.split("/") if value]
        if len(path_parts) >= 2 and path_parts[0] == "vuzu":
            return path_parts[1]
        return None

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
        source_field: str,
        external_id: str | None = None,
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "provider_name": "Tabiturient",
            "source_field": source_field,
        }
        if external_id is not None:
            metadata["external_id"] = external_id
        return metadata

    @staticmethod
    def _extract_rating(html: str) -> float | None:
        m = _RATING_PATTERN.search(html)
        if m:
            try:
                return float(m.group(1).replace(",", "."))
            except ValueError:
                pass
        return None

    @staticmethod
    def _extract_reviews(html: str) -> list[dict[str, Any]]:
        """
        Extract reviews from raw HTML using date-string anchors.
        Each Russian date found marks a review; surrounding text is the body.
        """
        reviews: list[dict[str, Any]] = []
        date_positions = list(_DATE_PATTERN.finditer(html))
        if not date_positions:
            return reviews

        for date_match in date_positions:
            window_start = max(0, date_match.start() - 3000)
            context_before = html[window_start:date_match.start()]

            date_str = (
                f"{date_match.group(1)} {date_match.group(2)} {date_match.group(3)}"
            )

            author_match = _AUTHOR_TYPE_PATTERN.search(context_before[-800:])
            author_type: str | None = (
                normalize_text(author_match.group(0)) if author_match else None
            )

            clean = re.sub(r"<[^>]+>", " ", context_before)
            review_text = normalize_text(clean)
            review_text = review_text[-1500:].strip() if len(review_text) > 1500 else review_text.strip()

            if len(review_text) < 50:
                continue

            reviews.append({"date": date_str, "text": review_text, "author_type": author_type})
            if len(reviews) >= 30:
                break

        return reviews
