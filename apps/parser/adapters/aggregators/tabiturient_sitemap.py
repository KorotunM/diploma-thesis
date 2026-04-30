from __future__ import annotations

import re
from dataclasses import dataclass
from xml.etree import ElementTree

PRIMARY_UNIVERSITY_URL_PATTERN = re.compile(
    r"^https://tabiturient\.ru/vuzu/[a-z0-9_-]+/?$",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class DiscoveredUniversityPage:
    url: str
    last_modified: str | None = None


class TabiturientSitemapDiscovery:
    def discover(self, payload: bytes | str) -> list[DiscoveredUniversityPage]:
        text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload
        text = text.lstrip()
        root = ElementTree.fromstring(text)

        discovered: list[DiscoveredUniversityPage] = []
        seen: set[str] = set()
        for url_element in root.findall(".//url"):
            location = self._child_text(url_element, "loc")
            if location is None:
                continue
            normalized_url = self._normalize_url(location)
            if not self._is_primary_university_url(normalized_url):
                continue
            if normalized_url in seen:
                continue
            seen.add(normalized_url)
            discovered.append(
                DiscoveredUniversityPage(
                    url=normalized_url,
                    last_modified=self._child_text(url_element, "lastmod"),
                )
            )
        return discovered

    @staticmethod
    def _normalize_url(url: str) -> str:
        return url.strip().rstrip("/")

    @classmethod
    def _is_primary_university_url(cls, url: str) -> bool:
        return bool(PRIMARY_UNIVERSITY_URL_PATTERN.fullmatch(url))

    @staticmethod
    def _child_text(element: ElementTree.Element, child_tag: str) -> str | None:
        child = element.find(child_tag)
        if child is None or child.text is None:
            return None
        text = child.text.strip()
        return text or None
