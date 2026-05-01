from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

from libs.source_sdk import ExtractedFragment, FetchContext, FetchedArtifact

from .base import OfficialSiteFragmentExtractor
from .html_extractor import EMAIL_PATTERN, PHONE_PATTERN, normalize_text, unique_preserve_order

TITLE_PATTERN = re.compile(r"<title>\s*(?P<title>.*?)\s*</title>", re.IGNORECASE | re.DOTALL)
FOOTER_CONTACT_BLOCK_PATTERN = re.compile(
    r"""id=['"]block-block-8['"][^>]*>.*?<div\s+class=['"]content\s+clearfix['"]>\s*
    (?P<body>.*?)\s*</div>\s*</div>\s*</div>""",
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)


class KubSUAbiturientHtmlExtractor(OfficialSiteFragmentExtractor):
    supported_parser_profiles = ("official_site.kubsu.abiturient_html",)

    def extract(
        self,
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> list[ExtractedFragment]:
        content = self._decode_content(artifact)
        fragments: list[ExtractedFragment] = []

        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="canonical_name",
            value=self._canonical_name(content),
            locator="title",
            confidence=0.98,
            metadata={"source_field": "page.title"},
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="contacts.website",
            value=self._website(context=context, artifact=artifact),
            locator='link[rel="canonical"]|endpoint_host',
            confidence=0.97,
            metadata={"source_field": "page.canonical_host"},
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="contacts.emails",
            value=self._emails(content),
            locator='div#block-block-8 .icons.email',
            confidence=0.96,
            metadata={"source_field": "footer.admission_email"},
        )
        self._append_fragment(
            fragments,
            context=context,
            artifact=artifact,
            field_name="contacts.phones",
            value=self._phones(content),
            locator='div#block-block-8 .icons.phone',
            confidence=0.96,
            metadata={"source_field": "footer.admission_phone"},
        )
        return fragments

    @staticmethod
    def _decode_content(artifact: FetchedArtifact) -> str:
        if artifact.content is None:
            raise ValueError("Fetched artifact content is required for KubSU extraction.")
        return artifact.content.decode("utf-8", errors="replace")

    @staticmethod
    def _canonical_name(content: str) -> str | None:
        match = TITLE_PATTERN.search(content)
        if match is None:
            return None
        title = normalize_text(match.group("title"))
        if not title:
            return None
        parts = [normalize_text(part) for part in title.split("|")]
        return next((part for part in reversed(parts) if part), None)

    @staticmethod
    def _website(
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> str:
        parsed = urlparse(artifact.final_url or artifact.source_url or context.endpoint_url)
        if parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        parsed = urlparse(context.endpoint_url)
        if parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return context.endpoint_url

    @staticmethod
    def _emails(content: str) -> list[str]:
        footer_block = KubSUAbiturientHtmlExtractor._footer_contact_block(content)
        values = unique_preserve_order(EMAIL_PATTERN.findall(footer_block))
        if values:
            return [values[0]]
        return []

    @staticmethod
    def _footer_contact_block(content: str) -> str:
        match = FOOTER_CONTACT_BLOCK_PATTERN.search(content)
        if match is None:
            return ""
        return match.group("body")

    @staticmethod
    def _phones(content: str) -> list[str]:
        footer_block = KubSUAbiturientHtmlExtractor._footer_contact_block(content)
        values = unique_preserve_order(
            [normalize_text(value) for value in PHONE_PATTERN.findall(footer_block)]
        )
        if values:
            return [values[0]]
        return []

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
                    "adapter_family": "official_sites",
                },
            )
        )
