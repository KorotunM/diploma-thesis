from __future__ import annotations

import logging
import re
from io import BytesIO
from typing import Any
from urllib.parse import urlparse

from PyPDF2 import PdfReader

from libs.source_sdk import ExtractedFragment, FetchContext, FetchedArtifact

from .base import OfficialSiteFragmentExtractor
from .kubsu_programs_html_extractor import slugify
from .html_extractor import normalize_text

_log = logging.getLogger(__name__)

YEAR_PATTERN = re.compile(r"(?P<year>20\d{2})")
PROGRAM_LINE_PATTERN = re.compile(
    r"^(?P<code>\d{2}\.\d{2}\.\d{2})\s+(?P<name>.+?)\s+(?P<budget_places>\d+)$"
)


class KubSUPlacesPdfExtractor(OfficialSiteFragmentExtractor):
    supported_parser_profiles = ("official_site.kubsu.places_pdf",)

    def extract(
        self,
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> list[ExtractedFragment]:
        lines = self._extract_lines(artifact)
        year = self._resolve_year(lines=lines, context=context, artifact=artifact)

        website = self._website(context=context, artifact=artifact)
        fragments: list[ExtractedFragment] = []
        current_faculty: str | None = None

        for line in lines:
            if self._looks_like_faculty(line):
                current_faculty = line
                continue

            match = PROGRAM_LINE_PATTERN.match(line)
            if match is None:
                continue

            code = match.group("code")
            name = normalize_text(match.group("name"))
            budget_places = int(match.group("budget_places"))
            faculty = current_faculty or "Admissions quotas"
            merge_key = self._program_merge_key(code=code, name=name, year=year)
            record_group_key = f"pdf:{merge_key}"

            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="contacts.website",
                value=website,
                locator="endpoint_host",
                confidence=0.97,
                metadata={
                    "record_group_key": record_group_key,
                    "source_field": "page.canonical_host",
                },
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="programs.faculty",
                value=faculty,
                locator=self._locator(merge_key, "faculty"),
                confidence=0.76,
                metadata=self._metadata(
                    merge_key=merge_key,
                    faculty=faculty,
                    code=code,
                    name=name,
                    year=year,
                    source_field="programs.faculty",
                ),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="programs.code",
                value=code,
                locator=self._locator(merge_key, "code"),
                confidence=0.9,
                metadata=self._metadata(
                    merge_key=merge_key,
                    faculty=faculty,
                    code=code,
                    name=name,
                    year=year,
                    source_field="programs.code",
                ),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="programs.name",
                value=name,
                locator=self._locator(merge_key, "name"),
                confidence=0.82,
                metadata=self._metadata(
                    merge_key=merge_key,
                    faculty=faculty,
                    code=code,
                    name=name,
                    year=year,
                    source_field="programs.name",
                ),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="programs.budget_places",
                value=budget_places,
                locator=self._locator(merge_key, "budget_places"),
                confidence=1.0,
                metadata=self._metadata(
                    merge_key=merge_key,
                    faculty=faculty,
                    code=code,
                    name=name,
                    year=year,
                    source_field="programs.budget_places",
                ),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="programs.year",
                value=year,
                locator="pdf.header.year",
                confidence=0.94,
                metadata=self._metadata(
                    merge_key=merge_key,
                    faculty=faculty,
                    code=code,
                    name=name,
                    year=year,
                    source_field="programs.year",
                ),
            )
        if not fragments:
            _log.warning(
                "KubSU PDF extractor: no admissions rows matched — "
                "PDF structure may have changed at %s",
                context.endpoint_url if context else "(unknown)",
            )
        return fragments

    @staticmethod
    def _extract_lines(artifact: FetchedArtifact) -> list[str]:
        if artifact.content is None:
            raise ValueError("Fetched artifact content is required for KubSU PDF extraction.")
        reader = PdfReader(BytesIO(artifact.content))
        lines: list[str] = []
        for page in reader.pages:
            extracted = page.extract_text() or ""
            for line in extracted.splitlines():
                normalized = normalize_text(line)
                if normalized:
                    lines.append(normalized)
        return lines

    @staticmethod
    def _looks_like_faculty(line: str) -> bool:
        if PROGRAM_LINE_PATTERN.match(line):
            return False
        return bool(re.search(r"[A-Za-zА-Яа-я]", line)) and not re.search(r"\d{2}\.\d{2}\.\d{2}", line)

    @staticmethod
    def _resolve_year(
        *,
        lines: list[str],
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> int:
        for value in [
            *lines,
            artifact.final_url or "",
            artifact.source_url,
            context.endpoint_url,
        ]:
            match = YEAR_PATTERN.search(value)
            if match is not None:
                return int(match.group("year"))
        raise ValueError("Could not determine admissions year from KubSU places PDF.")

    @staticmethod
    def _website(*, context: FetchContext, artifact: FetchedArtifact) -> str:
        parsed = urlparse(artifact.final_url or artifact.source_url or context.endpoint_url)
        if parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        parsed = urlparse(context.endpoint_url)
        if parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return context.endpoint_url

    @staticmethod
    def _program_merge_key(*, code: str, name: str, year: int) -> str:
        return f"{code}:{year}:{slugify(name)}"

    @staticmethod
    def _locator(merge_key: str, field_name: str) -> str:
        return f'pdf.program row[key="{merge_key}"].{field_name}'

    @staticmethod
    def _metadata(
        *,
        merge_key: str,
        faculty: str,
        code: str,
        name: str,
        year: int,
        source_field: str,
    ) -> dict[str, Any]:
        return {
            "record_group_key": f"pdf:{merge_key}",
            "program_merge_key": merge_key,
            "entity_type": "admission_program",
            "faculty": faculty,
            "program_code": code,
            "program_name": name,
            "program_year": year,
            "source_field": source_field,
        }

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
                    "endpoint_host": urlparse(artifact.source_url).netloc,
                },
            )
        )
