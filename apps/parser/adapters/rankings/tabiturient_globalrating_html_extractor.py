from __future__ import annotations

import re
from dataclasses import dataclass
from html import unescape
from typing import Any

from libs.source_sdk import ExtractedFragment, FetchContext, FetchedArtifact

from .base import RankingFragmentExtractor

WHITESPACE_PATTERN = re.compile(r"\s+")
YEAR_PATTERN = re.compile(
    r"рейтинг\s+вузов\s+россии\s+(?P<year>20\d{2})",
    re.IGNORECASE,
)
ROW_PATTERN = re.compile(
    r"<a\s+onclick=\"popup\('8'\);\s*showmoreinfof3\('https://tabiturient\.ru','(?P<entity_id>\d+)'\);\">"
    r"(?P<body>.*?)</a>",
    re.IGNORECASE | re.DOTALL,
)
RANK_PATTERN = re.compile(
    r"<center><span[^>]*><b>#(?P<rank>\d+)</b></span></center>",
    re.IGNORECASE | re.DOTALL,
)
CHANGE_PATTERN = re.compile(
    r"rating/2022/(?P<direction>up|down)\.gif.*?<td><span[^>]*>(?P<delta>[+-]?\d+)</span></td>",
    re.IGNORECASE | re.DOTALL,
)
ALIAS_PATTERN = re.compile(
    r"</div>\s*<span[^>]*>\s*<b>(?P<alias>[^<]+)</b>\s*</span>\s*<span[^>]*>\s*<br>\s*(?P<name>[^<]+)</span>",
    re.IGNORECASE | re.DOTALL,
)
VALUE_PATTERN = re.compile(
    r"<span[^>]*><b>(?P<value>\d+(?:\.\d+)?)</b>\s*</span>\s*<span[^>]*><br>\s*оценка\s*</span>",
    re.IGNORECASE | re.DOTALL,
)
CATEGORY_PATTERN = re.compile(
    r"<span[^>]*><b>\s*(?P<category>[A-Z][+-]?)\s*</b>\s*</span>\s*<span[^>]*><br>\s*категория\s*</span>",
    re.IGNORECASE | re.DOTALL,
)

PROVIDER_KEY = "tabiturient-globalrating"
PROVIDER_NAME = "Tabiturient"
RATING_METRIC = "russia_overall"
RATING_SCALE = "russia"


def normalize_text(value: str) -> str:
    return WHITESPACE_PATTERN.sub(" ", unescape(value)).strip()


def slugify(value: str) -> str:
    normalized = normalize_text(value).lower()
    translated = (
        normalized.replace(" ", "-")
        .replace("/", "-")
        .replace("_", "-")
        .replace("(", "")
        .replace(")", "")
        .replace(",", "")
        .replace(".", "")
    )
    return translated


@dataclass(frozen=True)
class RankingRow:
    entity_id: str
    rank: int
    alias: str
    canonical_name: str
    current_value: str
    category: str
    change_direction: str
    change_delta: int

    @property
    def rank_display(self) -> str:
        return f"#{self.rank}"


class TabiturientGlobalRatingHtmlExtractor(RankingFragmentExtractor):
    supported_parser_profiles = ("ranking.tabiturient.globalrating_html",)

    def extract(
        self,
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> list[ExtractedFragment]:
        content = self._decode_content(artifact)
        rating_year = self._parse_rating_year(content)
        rows = self._parse_rows(content)

        fragments: list[ExtractedFragment] = []
        for row in rows:
            rating_item_key = self._rating_item_key(row=row, rating_year=rating_year)
            group_key = rating_item_key
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="canonical_name",
                value=row.canonical_name,
                locator=self._locator(row, "canonical_name"),
                confidence=0.96,
                metadata=self._metadata(
                    source_field="tabiturient.globalrating.full_name",
                    rating_item_key=rating_item_key,
                    record_group_key=group_key,
                    external_id=row.entity_id,
                    rank_display=row.rank_display,
                    rank_position=row.rank,
                    category=row.category,
                    change_direction=row.change_direction,
                    change_delta=row.change_delta,
                ),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="aliases",
                value=[row.alias] if row.alias != row.canonical_name else [],
                locator=self._locator(row, "alias"),
                confidence=0.95,
                metadata=self._metadata(
                    source_field="tabiturient.globalrating.alias",
                    rating_item_key=rating_item_key,
                    record_group_key=group_key,
                    external_id=row.entity_id,
                ),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="ratings.provider",
                value=PROVIDER_NAME,
                locator=self._locator(row, "provider"),
                confidence=0.99,
                metadata=self._metadata(
                    source_field="tabiturient.globalrating.provider",
                    rating_item_key=rating_item_key,
                    record_group_key=group_key,
                    external_id=row.entity_id,
                ),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="ratings.year",
                value=rating_year,
                locator="page.heading.year",
                confidence=0.99,
                metadata=self._metadata(
                    source_field="tabiturient.globalrating.year",
                    rating_item_key=rating_item_key,
                    record_group_key=group_key,
                    external_id=row.entity_id,
                ),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="ratings.metric",
                value=RATING_METRIC,
                locator="page.heading.metric",
                confidence=0.98,
                metadata=self._metadata(
                    source_field="tabiturient.globalrating.metric",
                    rating_item_key=rating_item_key,
                    record_group_key=group_key,
                    external_id=row.entity_id,
                    scale=RATING_SCALE,
                ),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="ratings.rank",
                value=row.rank,
                locator=self._locator(row, "rank"),
                confidence=0.97,
                metadata=self._metadata(
                    source_field="tabiturient.globalrating.rank",
                    rating_item_key=rating_item_key,
                    record_group_key=group_key,
                    external_id=row.entity_id,
                    rank_display=row.rank_display,
                ),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="ratings.value",
                value=row.current_value,
                locator=self._locator(row, "value"),
                confidence=0.97,
                metadata=self._metadata(
                    source_field="tabiturient.globalrating.value",
                    rating_item_key=rating_item_key,
                    record_group_key=group_key,
                    external_id=row.entity_id,
                    rank_display=row.rank_display,
                    scale=RATING_SCALE,
                    category=row.category,
                    change_direction=row.change_direction,
                    change_delta=row.change_delta,
                ),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="ratings.category",
                value=row.category,
                locator=self._locator(row, "category"),
                confidence=0.95,
                metadata=self._metadata(
                    source_field="tabiturient.globalrating.category",
                    rating_item_key=rating_item_key,
                    record_group_key=group_key,
                    external_id=row.entity_id,
                ),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="ratings.change.direction",
                value=row.change_direction,
                locator=self._locator(row, "change.direction"),
                confidence=0.93,
                metadata=self._metadata(
                    source_field="tabiturient.globalrating.change.direction",
                    rating_item_key=rating_item_key,
                    record_group_key=group_key,
                    external_id=row.entity_id,
                ),
            )
            self._append_fragment(
                fragments,
                context=context,
                artifact=artifact,
                field_name="ratings.change.delta",
                value=row.change_delta,
                locator=self._locator(row, "change.delta"),
                confidence=0.93,
                metadata=self._metadata(
                    source_field="tabiturient.globalrating.change.delta",
                    rating_item_key=rating_item_key,
                    record_group_key=group_key,
                    external_id=row.entity_id,
                ),
            )
        return fragments

    @staticmethod
    def _decode_content(artifact: FetchedArtifact) -> str:
        if artifact.content is None:
            raise ValueError(
                "Fetched artifact content is required for tabiturient global rating extraction."
            )
        return artifact.content.decode("utf-8", errors="replace")

    @staticmethod
    def _parse_rating_year(content: str) -> int:
        match = YEAR_PATTERN.search(content)
        if match is None:
            raise ValueError("Could not determine tabiturient global rating year from page.")
        return int(match.group("year"))

    @classmethod
    def _parse_rows(cls, content: str) -> list[RankingRow]:
        rows: list[RankingRow] = []
        for match in ROW_PATTERN.finditer(content):
            body = match.group("body")
            row = cls._row_from_body(entity_id=match.group("entity_id"), body=body)
            if row is not None:
                rows.append(row)
        if not rows:
            raise ValueError("No ranking rows were detected in tabiturient global rating page.")
        return rows

    @classmethod
    def _row_from_body(cls, *, entity_id: str, body: str) -> RankingRow | None:
        rank_match = RANK_PATTERN.search(body)
        alias_match = ALIAS_PATTERN.search(body)
        value_match = VALUE_PATTERN.search(body)
        category_match = CATEGORY_PATTERN.search(body)
        if (
            rank_match is None
            or alias_match is None
            or value_match is None
            or category_match is None
        ):
            return None

        change_match = CHANGE_PATTERN.search(body)
        if change_match is None:
            change_direction = "same"
            change_delta = 0
        else:
            raw_direction = change_match.group("direction").lower()
            change_direction = "up" if raw_direction == "up" else "down"
            change_delta = abs(int(change_match.group("delta")))

        return RankingRow(
            entity_id=entity_id,
            rank=int(rank_match.group("rank")),
            alias=normalize_text(alias_match.group("alias")),
            canonical_name=normalize_text(alias_match.group("name")),
            current_value=normalize_text(value_match.group("value")),
            category=normalize_text(category_match.group("category")),
            change_direction=change_direction,
            change_delta=change_delta,
        )

    @staticmethod
    def _rating_item_key(*, row: RankingRow, rating_year: int) -> str:
        return f"{PROVIDER_KEY}:{rating_year}:{RATING_METRIC}:{row.entity_id or slugify(row.alias)}"

    @staticmethod
    def _locator(row: RankingRow, field_name: str) -> str:
        return f'row[provider_entity_id="{row.entity_id}"].{field_name}'

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
                    "adapter_family": "rankings",
                },
            )
        )

    @staticmethod
    def _metadata(
        *,
        source_field: str,
        rating_item_key: str,
        record_group_key: str,
        external_id: str,
        rank_display: str | None = None,
        rank_position: int | None = None,
        category: str | None = None,
        change_direction: str | None = None,
        change_delta: int | None = None,
        scale: str | None = None,
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "provider_name": PROVIDER_NAME,
            "provider_key": PROVIDER_KEY,
            "source_field": source_field,
            "rating_item_key": rating_item_key,
            "record_group_key": record_group_key,
            "external_id": external_id,
        }
        if rank_display is not None:
            metadata["rank_display"] = rank_display
        if rank_position is not None:
            metadata["rank_position"] = rank_position
        if category is not None:
            metadata["category"] = category
        if change_direction is not None:
            metadata["change_direction"] = change_direction
        if change_delta is not None:
            metadata["change_delta"] = change_delta
        if scale is not None:
            metadata["scale"] = scale
        return metadata
