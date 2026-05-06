from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlparse

from libs.source_sdk import ExtractedFragment, FetchContext, FetchedArtifact

from .base import AggregatorFragmentExtractor

_WHITESPACE = re.compile(r"\s+")

# Program code: "01.03.02" format
_CODE_PATTERN = re.compile(r"\b(\d{2}\.\d{2}\.\d{2})\b")

# Level from code second segment
_BACHELOR_CODES = {"03"}
_SPECIALIST_CODES = {"05"}
_MASTER_CODES = {"04"}
_PHD_CODES = {"06", "07"}

# Passing score: integer 100-400
_SCORE_PATTERN = re.compile(r"\b(\d{3})\b")

# Budget places: number before "мест" or "бюджет"
_BUDGET_PATTERN = re.compile(r"(\d+)\s*(?:бюдж|мест|б\.м\.)", re.IGNORECASE)

# Study form keywords
_FORMS = {
    "очно": "full_time",
    "вечер": "evening",
    "заочно": "distance",
    "заочн": "distance",
    "очно-заочно": "mixed",
    "очно-заочн": "mixed",
}


def _norm(text: str) -> str:
    return _WHITESPACE.sub(" ", text).strip()


def _level_from_code(code: str) -> str:
    parts = code.split(".")
    if len(parts) < 2:
        return "Бакалавриат"
    seg = parts[1]
    if seg in _MASTER_CODES:
        return "Магистратура"
    if seg in _SPECIALIST_CODES:
        return "Специалитет"
    if seg in _PHD_CODES:
        return "Аспирантура"
    return "Бакалавриат"


def _study_form(text: str) -> str | None:
    lower = text.lower()
    for keyword, form in _FORMS.items():
        if keyword in lower:
            return form
    return None


@dataclass
class _ProgramBlock:
    name: str | None = None
    code: str | None = None
    faculty: str | None = None
    passing_score: int | None = None
    budget_places: int | None = None
    study_form: str | None = None
    level: str | None = None
    raw_text: str = ""


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


class _ProxodnoiPageParser(HTMLParser):
    _CAPTURE_TAGS = {"div", "span", "td", "tr", "th", "h1", "h2", "h3", "h4", "b", "strong", "a", "p", "li"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.nodes: list[_Node] = []
        self._stack: list[_Node] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in self._CAPTURE_TAGS:
            norm = {k.lower(): (v or "") for k, v in attrs}
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
        if cleaned and self._stack:
            self._stack[-1].text_parts.append(cleaned)


def _extract_programs_from_nodes(nodes: list[_Node]) -> list[_ProgramBlock]:
    """
    Walk nodes and group them into program blocks.

    Strategy: a new block starts when we find a node containing a program code.
    All text nodes between two consecutive codes belong to the same block.
    Faculty/section headers are recognized by being standalone h3/h4/strong
    text without a code, appearing before a code block.
    """
    programs: list[_ProgramBlock] = []
    current_faculty: str | None = None
    current: _ProgramBlock | None = None
    pending_texts: list[str] = []

    for node in nodes:
        text = node.text
        if not text:
            continue

        code_match = _CODE_PATTERN.search(text)

        # Section/faculty header detection (h2/h3/h4/strong without code)
        if node.tag in {"h2", "h3", "h4"} and not code_match:
            if len(text) > 4 and re.search(r"[А-ЯЁа-яё]{3,}", text):
                current_faculty = text
            continue

        if code_match:
            # Save previous block
            if current is not None:
                _finalize(current, pending_texts)
                if current.code:
                    programs.append(current)
                pending_texts = []

            code = code_match.group(1)
            current = _ProgramBlock(
                code=code,
                faculty=current_faculty,
                level=_level_from_code(code),
            )

            # Extract name: text before the code in this node
            name_part = text[: code_match.start()].strip(" |•–")
            # Also check if code pattern is embedded like "Бакалавриат | 01.03.02"
            name_part = re.sub(r"\b(бакалавриат|магистратура|специалитет|аспирантура)\b", "", name_part, flags=re.IGNORECASE).strip(" |•–")
            if len(name_part) > 5:
                current.name = name_part

            # Rest of text after code
            rest = text[code_match.end():].strip()
            if rest:
                pending_texts.append(rest)
        else:
            # Accumulate text for current block
            if current is not None:
                pending_texts.append(text)

    # Flush last block
    if current is not None:
        _finalize(current, pending_texts)
        if current.code:
            programs.append(current)

    return programs


def _finalize(block: _ProgramBlock, texts: list[str]) -> None:
    combined = " ".join(texts)
    block.raw_text = combined

    # Extract name if not set yet (look for longest alphabetic-heavy text)
    if block.name is None:
        for text in texts:
            if len(text) > 8 and re.search(r"[А-ЯЁа-яё]{5,}", text):
                if not _CODE_PATTERN.search(text):
                    block.name = text
                    break

    # Extract passing score: find first 3-digit number in range 100-399
    for m in _SCORE_PATTERN.finditer(combined):
        val = int(m.group(1))
        if 100 <= val <= 399:
            block.passing_score = val
            break

    # Extract budget places
    bm = _BUDGET_PATTERN.search(combined)
    if bm:
        try:
            block.budget_places = int(bm.group(1))
        except ValueError:
            pass

    # Extract study form
    block.study_form = _study_form(combined)


def _program_field_name(block: _ProgramBlock, index: int) -> str:
    key = f"{block.code or ''}:{block.name or ''}:{index}"
    digest = hashlib.sha1(key.encode()).hexdigest()[:8]
    return f"programs.{digest}"


class TabiturientProxodnoiHtmlExtractor(AggregatorFragmentExtractor):
    supported_parser_profiles = ("aggregator.tabiturient.proxodnoi_html",)

    def extract(
        self,
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> list[ExtractedFragment]:
        html = self._decode(artifact)
        parser = _ProxodnoiPageParser()
        parser.feed(html)
        parser.close()

        programs = _extract_programs_from_nodes(parser.nodes)
        slug = self._slug(context.endpoint_url)

        frags: list[ExtractedFragment] = []
        for i, prog in enumerate(programs):
            value: dict[str, Any] = {
                "faculty": prog.faculty,
                "code": prog.code,
                "name": prog.name,
                "budget_places": prog.budget_places,
                "passing_score": prog.passing_score,
                "study_form": prog.study_form,
                "level": prog.level,
                "year": 2025,
            }
            field_name = _program_field_name(prog, i)
            frags.append(
                ExtractedFragment(
                    raw_artifact_id=artifact.raw_artifact_id,
                    source_key=context.source_key,
                    source_url=artifact.source_url,
                    field_name=field_name,
                    value=value,
                    locator=f"program[code='{prog.code}']",
                    confidence=0.82,
                    metadata={
                        "parser_profile": context.parser_profile,
                        "adapter_family": "aggregators",
                        "provider_name": "Tabiturient",
                        "source_field": "tabiturient.proxodnoi.program",
                        "external_id": slug,
                        "program_code": prog.code,
                    },
                )
            )
        return frags

    @staticmethod
    def _decode(artifact: FetchedArtifact) -> str:
        if artifact.content is None:
            raise ValueError("Content required for tabiturient proxodnoi extraction.")
        return artifact.content.decode("utf-8", errors="replace")

    @staticmethod
    def _slug(endpoint_url: str) -> str | None:
        parts = [p for p in urlparse(endpoint_url).path.split("/") if p]
        if len(parts) >= 2 and parts[0] == "vuzu":
            return parts[1]
        return None
