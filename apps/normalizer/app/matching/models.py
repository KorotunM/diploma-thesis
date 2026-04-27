from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

from apps.normalizer.app.universities.models import UniversityRecord

MatchField = Literal["canonical_domain", "canonical_name"]


class UniversityExactMatchCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    canonical_domain: str | None = None
    canonical_name: str | None = None


class UniversityExactMatchResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university: UniversityRecord
    matched_by: MatchField
    matched_value: str
    strategy: Literal["exact"] = "exact"
