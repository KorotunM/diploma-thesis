from __future__ import annotations

import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlparse

from libs.source_sdk import ExtractedFragment, FetchContext, FetchedArtifact

from .base import AggregatorFragmentExtractor

_WHITESPACE = re.compile(r"\s+")

# ── Regex patterns for stats ─────────────────────────────────────────────────

# e.g. "7.7 /10" or "7.7/10" or "Рейтинг: 7.7"
_RATING_PATTERN = re.compile(r"(\d+[.,]\d+)\s*/\s*10")

# e.g. "4 983 оценок" or "4983 оценок" or "оценки"
_RATING_COUNT_PATTERN = re.compile(r"([\d\s]+)\s+оцен[ок]+")

# e.g. "65 образовательных программ" or "65 программ"
_PROGRAMS_COUNT_PATTERN = re.compile(r"(\d+)\s+(?:образовательных\s+)?программ")

# e.g. "2 033 бюджетных мест" or "2033 бюджетных мест"
_BUDGET_PLACES_PATTERN = re.compile(r"([\d\s]+)\s+бюджетных\s+мест")

# e.g. "76.5 средневзвешенный" or "средневзвешенный проходной балл\n76.5"
_AVG_SCORE_PATTERN = re.compile(
    r"(?:средневзвешенный[^\d]{0,40}|средний[^\d]{0,40}проходной[^\d]{0,30})(\d{2,3}[.,]\d)"
    r"|(\d{2,3}[.,]\d)\s*(?:–|—|-|\s)?(?:средневзвешенный|средний)",
    re.IGNORECASE | re.DOTALL,
)

# Logo: img with /logovuz/ in src
_LOGO_SRC_PATTERN = re.compile(r"/logovuz/[^\"'>\s]+", re.IGNORECASE)

# Category letter A/B/C/D/E near globalrating
_CATEGORY_PATTERN = re.compile(r"\bкатегория[:\s]*([A-E])\b|\bкатегори[яи]\s*«?([A-E])»?", re.IGNORECASE)

# University type
_GOV_TYPE_PATTERN = re.compile(r"(?:Гос|Государственн)[^\s,<]{0,8}\.?\s*вуз", re.IGNORECASE)
_PRIVATE_TYPE_PATTERN = re.compile(r"Негос[^\s,<]{0,8}\.?\s*вуз", re.IGNORECASE)

# Flagship
_FLAGSHIP_PATTERN = re.compile(r"\bГоловной\b", re.IGNORECASE)

# City — common Russian city patterns near location markers
_CITY_PATTERN = re.compile(
    r"(?:г\.\s*|город\s+|г\s+)([А-ЯЁа-яё][а-яё]+(?:-[А-ЯЁа-яё][а-яё]+)?)",
    re.UNICODE,
)


def _norm(text: str) -> str:
    return _WHITESPACE.sub(" ", text).strip()


def _parse_int(text: str) -> int | None:
    cleaned = re.sub(r"\s", "", text).replace(",", "")
    try:
        return int(cleaned)
    except ValueError:
        return None


def _parse_float(text: str) -> float | None:
    cleaned = text.strip().replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


@dataclass
class _Node:
    tag: str
    attrs: dict[str, str]
    text_parts: list[str] = field(default_factory=list)

    @property
    def text(self) -> str:
        return _norm(" ".join(self.text_parts))

    def attr(self, key: str) -> str:
        return self.attrs.get(key, "")

    def locator(self) -> str:
        itemprop = self.attrs.get("itemprop")
        if itemprop:
            return f'{self.tag}[itemprop="{itemprop}"]'
        cls = self.attrs.get("class")
        if cls:
            return f"{self.tag}.{cls.split()[0]}"
        return self.tag


class _AboutPageParser(HTMLParser):
    _CAPTURE_TAGS = {"h1", "h2", "h3", "h4", "span", "div", "p", "a", "b", "strong", "td", "li"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.nodes: list[_Node] = []
        self._stack: list[_Node] = []
        self.img_srcs: list[str] = []
        self._full_text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        norm = {k.lower(): (v or "") for k, v in attrs}
        if tag == "img":
            src = norm.get("src", "")
            if src:
                self.img_srcs.append(src)
        if tag in self._CAPTURE_TAGS:
            self._stack.append(_Node(tag=tag, attrs=norm))

    def handle_endtag(self, tag: str) -> None:
        if not self._stack or self._stack[-1].tag != tag:
            return
        node = self._stack.pop()
        if node.text or node.attrs:
            self.nodes.append(node)
        if self._stack and node.text:
            self._stack[-1].text_parts.append(node.text)

    def handle_data(self, data: str) -> None:
        cleaned = _norm(data)
        if cleaned:
            self._full_text_parts.append(cleaned)
        if cleaned and self._stack:
            self._stack[-1].text_parts.append(cleaned)

    @property
    def full_text(self) -> str:
        return " ".join(self._full_text_parts)


class TabiturientAboutHtmlExtractor(AggregatorFragmentExtractor):
    supported_parser_profiles = ("aggregator.tabiturient.about_html",)

    _BASE_URL = "https://tabiturient.ru"

    def extract(
        self,
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> list[ExtractedFragment]:
        html = self._decode(artifact)
        parser = _AboutPageParser()
        parser.feed(html)
        parser.close()

        nodes = parser.nodes
        full_text = parser.full_text
        slug = self._slug(context.endpoint_url)

        frags: list[ExtractedFragment] = []

        # ── Name & aliases ───────────────────────────────────────────────────
        name = self._first_itemprop(nodes, "name")
        alias = self._first_itemprop(nodes, "alternateName")
        self._add(frags, context, artifact, "canonical_name", name,
                  self._first_locator(nodes, "name"), 0.95,
                  {"source_field": "tabiturient.itemprop.name", "external_id": slug})
        if alias and alias != name:
            self._add(frags, context, artifact, "aliases", [alias],
                      self._first_locator(nodes, "alternateName"), 0.88,
                      {"source_field": "tabiturient.itemprop.alternateName", "external_id": slug})

        # ── Logo ─────────────────────────────────────────────────────────────
        logo_url = self._logo_url(parser.img_srcs)
        self._add(frags, context, artifact, "contacts.logo_url", logo_url,
                  "img[src*='/logovuz/']", 0.99,
                  {"source_field": "tabiturient.logo_img_src", "external_id": slug})

        # ── Description ──────────────────────────────────────────────────────
        description = self._description(nodes)
        self._add(frags, context, artifact, "description", description,
                  "p.about-text", 0.80,
                  {"source_field": "tabiturient.about.description", "external_id": slug})

        # ── Institutional type ───────────────────────────────────────────────
        inst_type = self._inst_type(full_text)
        self._add(frags, context, artifact, "institutional.type", inst_type,
                  "span.vuz-type", 0.90,
                  {"source_field": "tabiturient.about.vuz_type", "external_id": slug})

        # ── Category (A/B/C) ─────────────────────────────────────────────────
        category = self._category(nodes, full_text)
        self._add(frags, context, artifact, "institutional.category", category,
                  "a[href*='globalrating']", 0.85,
                  {"source_field": "tabiturient.about.category", "external_id": slug})

        # ── Flagship ─────────────────────────────────────────────────────────
        if _FLAGSHIP_PATTERN.search(full_text):
            self._add(frags, context, artifact, "institutional.is_flagship", True,
                      "span.flagship", 0.87,
                      {"source_field": "tabiturient.about.is_flagship", "external_id": slug})

        # ── City ─────────────────────────────────────────────────────────────
        city = self._city(nodes, full_text)
        self._add(frags, context, artifact, "location.city", city,
                  "span.city", 0.78,
                  {"source_field": "tabiturient.about.city", "external_id": slug})

        # ── Tabiturient user rating ──────────────────────────────────────────
        rating = self._rating(full_text)
        if rating is not None:
            self._add(frags, context, artifact, "ratings.tabiturient_user",
                      {"provider": "tabiturient", "year": 2025,
                       "metric": "user_rating", "value": str(rating)},
                      "div.rating-score", 0.92,
                      {"source_field": "tabiturient.about.user_rating", "external_id": slug})

        # ── Rating count (store in reviews.rating_count) ─────────────────────
        rating_count = self._rating_count(full_text)
        self._add(frags, context, artifact, "reviews.rating", rating,
                  "div.rating-score", 0.92,
                  {"source_field": "tabiturient.about.user_rating", "external_id": slug})
        self._add(frags, context, artifact, "reviews.rating_count", rating_count,
                  "div.rating-count", 0.92,
                  {"source_field": "tabiturient.about.rating_count", "external_id": slug})

        return frags

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _decode(artifact: FetchedArtifact) -> str:
        if artifact.content is None:
            raise ValueError("Content required for tabiturient about extraction.")
        return artifact.content.decode("utf-8", errors="replace")

    @staticmethod
    def _slug(endpoint_url: str) -> str | None:
        parts = [p for p in urlparse(endpoint_url).path.split("/") if p]
        if len(parts) >= 2 and parts[0] == "vuzu":
            return parts[1]
        return None

    @staticmethod
    def _first_itemprop(nodes: list[_Node], prop: str) -> str | None:
        node = next((n for n in nodes if n.attrs.get("itemprop") == prop and n.text), None)
        return node.text if node else None

    @staticmethod
    def _first_locator(nodes: list[_Node], prop: str) -> str | None:
        node = next((n for n in nodes if n.attrs.get("itemprop") == prop), None)
        return node.locator() if node else None

    def _logo_url(self, img_srcs: list[str]) -> str | None:
        for src in img_srcs:
            if "/logovuz/" in src.lower():
                if src.startswith("//"):
                    return "https:" + src
                if src.startswith("/"):
                    return self._BASE_URL + src
                if src.startswith("http"):
                    return src
        return None

    @staticmethod
    def _description(nodes: list[_Node]) -> str | None:
        candidates: list[str] = []
        for node in nodes:
            if node.tag not in {"p", "div"}:
                continue
            text = node.text
            # Minimum length and should look like a description (Cyrillic, sentence-like)
            if len(text) < 80:
                continue
            if not re.search(r"[А-ЯЁа-яё]{5,}", text):
                continue
            # Skip nav/menu-like text
            if re.search(r"(?:меню|навигация|шапка|footer|header)", text, re.IGNORECASE):
                continue
            candidates.append(text)
        if not candidates:
            return None
        # Return the longest candidate (most likely the actual description)
        return max(candidates, key=len)

    @staticmethod
    def _inst_type(full_text: str) -> str | None:
        if _PRIVATE_TYPE_PATTERN.search(full_text):
            return "Негосударственный"
        if _GOV_TYPE_PATTERN.search(full_text):
            return "Государственный"
        return None

    @staticmethod
    def _category(nodes: list[_Node], full_text: str) -> str | None:
        # Look for category in nodes that have links to globalrating
        for node in nodes:
            if "globalrating" in node.attr("href"):
                # The category letter should be near this link
                m = re.search(r"\b([A-E])\b", node.text)
                if m:
                    return m.group(1)
        # Fallback: search full text
        m = _CATEGORY_PATTERN.search(full_text)
        if m:
            return m.group(1) or m.group(2)
        return None

    @staticmethod
    def _city(nodes: list[_Node], full_text: str) -> str | None:
        # Look for itemprop=addressLocality
        node = next(
            (n for n in nodes if n.attrs.get("itemprop") in {"addressLocality", "addressRegion"} and n.text),
            None,
        )
        if node:
            return node.text
        # Fallback: regex on full text
        m = _CITY_PATTERN.search(full_text)
        if m:
            return m.group(1)
        return None

    @staticmethod
    def _rating(full_text: str) -> float | None:
        m = _RATING_PATTERN.search(full_text)
        if m:
            return _parse_float(m.group(1))
        return None

    @staticmethod
    def _rating_count(full_text: str) -> int | None:
        m = _RATING_COUNT_PATTERN.search(full_text)
        if m:
            return _parse_int(m.group(1))
        return None

    @staticmethod
    def _add(
        frags: list[ExtractedFragment],
        context: FetchContext,
        artifact: FetchedArtifact,
        field_name: str,
        value: Any,
        locator: str | None,
        confidence: float,
        metadata: dict[str, Any],
    ) -> None:
        if value is None or value == "" or value == []:
            return
        frags.append(
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
                    "provider_name": "Tabiturient",
                },
            )
        )
