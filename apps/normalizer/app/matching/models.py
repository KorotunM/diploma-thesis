from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from apps.normalizer.app.universities.models import (
    UniversityRecord,
    UniversitySimilarityCandidate,
)

MatchField = Literal["canonical_domain", "canonical_name"]
MatchStatus = Literal["matched", "review_required", "unmatched"]
MatchStrategy = Literal["exact", "trigram"]


class UniversityMatchCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    canonical_domain: str | None = None
    canonical_name: str | None = None


class UniversityMatchDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: MatchStatus
    university: UniversityRecord | None = None
    matched_by: MatchField | None = None
    matched_value: str | None = None
    strategy: MatchStrategy | None = None
    similarity_score: float | None = None
    review_candidates: list[UniversitySimilarityCandidate] = Field(default_factory=list)
