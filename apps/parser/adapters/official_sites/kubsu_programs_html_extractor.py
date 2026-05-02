from __future__ import annotations

import re
from dataclasses import dataclass
from html import unescape
from typing import Any

from libs.source_sdk import ExtractedFragment, FetchContext, FetchedArtifact

from .base import OfficialSiteFragmentExtractor
from .html_extractor import normalize_text

TABLE_PATTERN = re.compile(r"<table>(?P<table>.*?)</table>", re.IGNORECASE | re.DOTALL)
ROW_PATTERN = re.compile(r"<tr>(?P<row>.*?)</tr>", re.IGNORECASE | re.DOTALL)
CELL_PATTERN = re.compile(
    r"<t(?P<kind>h|d)(?P<attrs>[^>]*)>(?P<cell>.*?)</t(?:h|d)>",
    re.IGNORECASE | re.DOTALL,
)
PASSING_YEAR_PATTERN = re.compile(
    r"проходные\s+баллы\s+(?P<year>20\d{2})\s+года",
    re.IGNORECASE,
)
PROGRAM_CODE_PATTERN = re.compile(r"^(?P<code>\d{2}\.\d{2}\.\d{2})\s+(?P<name>.+)$")
TAG_PATTERN = re.compile(r"<[^>]+>")
WHITESPACE_PATTERN = re.compile(r"\s+")


@dataclass(frozen=True)
class ProgramRow:
    row_key: str
    faculty: str
    code: str
    name: str
    budget_places: int
    passing_score: int
    year: int


@dataclass(frozen=True)
class _Cell:
    text: str
    colspan: int = 1


def slugify(value: str) -> str:
    normalized = normalize_text(value).casefold()
    return (
        normalized.replace(" ", "-")
        .replace(",", "")
        .replace(".", "")
        .replace("(", "")
        .replace(")", "")
        .replace("/", "-")
    )


def _strip_tags(value: str) -> str:
    return WHITESPACE_PATTERN.sub(" ", unescape(TAG_PATTERN.sub(" ", value))).strip()


def _parse_int(value: str) -> int | None:
    cleaned = normalize_text(value)
    if not cleaned.isdigit():
        return None
    return int(cleaned)


class KubSUProgramsHtmlExtractor(OfficialSiteFragmentExtractor):
    supported_parser_profiles = ("official_site.kubsu.programs_html",)

    def extract(
        self,
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> list[ExtractedFragment]:
        content = self._decode_content(artifact)
        rows = self._parse_rows(content)

        fragments: list[ExtractedFragment] = []
        for row in rows:
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="programs.faculty",
                value=row.faculty,
                locator=self._locator(row, "faculty"),
                confidence=0.99,
                metadata=self._metadata(row=row, source_field="programs.faculty"),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="programs.code",
                value=row.code,
                locator=self._locator(row, "code"),
                confidence=0.99,
                metadata=self._metadata(row=row, source_field="programs.code"),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="programs.name",
                value=row.name,
                locator=self._locator(row, "name"),
                confidence=0.98,
                metadata=self._metadata(row=row, source_field="programs.name"),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="programs.budget_places",
                value=row.budget_places,
                locator=self._locator(row, "budget_places"),
                confidence=0.99,
                metadata=self._metadata(
                    row=row,
                    source_field="programs.budget_places",
                ),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="programs.passing_score",
                value=row.passing_score,
                locator=self._locator(row, "passing_score"),
                confidence=0.99,
                metadata=self._metadata(
                    row=row,
                    source_field="programs.passing_score",
                ),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="programs.year",
                value=row.year,
                locator="table.programs.header.passing_year",
                confidence=0.99,
                metadata=self._metadata(row=row, source_field="programs.year"),
            )
        return fragments

    @staticmethod
    def _decode_content(artifact: FetchedArtifact) -> str:
        if artifact.content is None:
            raise ValueError("Fetched artifact content is required for KubSU programs extraction.")
        return artifact.content.decode("utf-8", errors="replace")

    def _parse_rows(self, content: str) -> list[ProgramRow]:
        table_match = TABLE_PATTERN.search(content)
        if table_match is None:
            raise ValueError("Could not locate programs table in KubSU programs page.")
        table_html = table_match.group("table")
        year = self._parse_passing_year(table_html)

        rows: list[ProgramRow] = []
        current_faculty: str | None = None
        program_index = 0

        for row_match in ROW_PATTERN.finditer(table_html):
            cells = self._parse_cells(row_match.group("row"))
            if not cells:
                continue
            texts = [cell.text for cell in cells]
            if len(cells) == 1 and cells[0].colspan >= 3:
                current_faculty = texts[0]
                continue
            if len(texts) != 3 or current_faculty is None:
                continue
            program = self._program_row(
                texts=texts,
                faculty=current_faculty,
                year=year,
                program_index=program_index,
            )
            if program is None:
                continue
            rows.append(program)
            program_index += 1

        if not rows:
            raise ValueError("No program rows were detected in KubSU programs table.")
        return rows

    @staticmethod
    def _parse_passing_year(table_html: str) -> int:
        match = PASSING_YEAR_PATTERN.search(normalize_text(_strip_tags(table_html)))
        if match is None:
            raise ValueError("Could not determine passing-score year from KubSU programs page.")
        return int(match.group("year"))

    @staticmethod
    def _parse_cells(row_html: str) -> list[_Cell]:
        cells: list[_Cell] = []
        for match in CELL_PATTERN.finditer(row_html):
            attrs = match.group("attrs") or ""
            colspan_match = re.search(r"""colspan=["']?(?P<value>\d+)""", attrs, re.IGNORECASE)
            colspan = int(colspan_match.group("value")) if colspan_match else 1
            text = normalize_text(_strip_tags(match.group("cell")))
            cells.append(_Cell(text=text, colspan=colspan))
        return cells

    @staticmethod
    def _program_row(
        *,
        texts: list[str],
        faculty: str,
        year: int,
        program_index: int,
    ) -> ProgramRow | None:
        code_match = PROGRAM_CODE_PATTERN.match(texts[0])
        if code_match is None:
            return None
        budget_places = _parse_int(texts[1])
        passing_score = _parse_int(texts[2])
        if budget_places is None or passing_score is None:
            return None
        code = code_match.group("code")
        name = normalize_text(code_match.group("name"))
        row_key = f"{slugify(faculty)}:{code}:{program_index}"
        return ProgramRow(
            row_key=row_key,
            faculty=faculty,
            code=code,
            name=name,
            budget_places=budget_places,
            passing_score=passing_score,
            year=year,
        )

    @staticmethod
    def _locator(row: ProgramRow, field_name: str) -> str:
        return f'table.programs row[key="{row.row_key}"].{field_name}'

    @staticmethod
    def _metadata(
        *,
        row: ProgramRow,
        source_field: str,
    ) -> dict[str, Any]:
        program_merge_key = f"{row.code}:{row.year}:{slugify(row.name)}"
        return {
            "record_group_key": row.row_key,
            "program_merge_key": program_merge_key,
            "entity_type": "admission_program",
            "faculty": row.faculty,
            "program_code": row.code,
            "program_name": row.name,
            "program_year": row.year,
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
                },
            )
        )
