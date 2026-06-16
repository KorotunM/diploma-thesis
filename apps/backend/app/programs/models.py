from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class ProgramDirectoryItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    name: str
    level: str | None = None
    description: str | None = None
    university_count: int = 0
    budget_places: int = 0
    paid_places: int = 0
    avg_passing_score: float | None = None
    min_tuition_per_year: int | None = None
    ege_subjects: list[str] = Field(default_factory=list)


class ProgramDirectoryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total: int
    items: list[ProgramDirectoryItem] = Field(default_factory=list)
