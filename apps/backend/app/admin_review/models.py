from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

ReviewCaseStatus = Literal["open", "resolved", "dismissed"]
ReviewCasePriority = Literal["high", "normal"]


class ReviewCaseItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    review_case_id: UUID
    status: ReviewCaseStatus
    reason: str
    priority: ReviewCasePriority
    university_id: UUID | None = None
    canonical_name: str | None = None
    evidence_ids: list[UUID] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime
    resolved_at: datetime | None = None
    resolved_by: UUID | None = None
    resolution: str | None = None
    note: str | None = None


class ReviewCaseListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total: int
    items: list[ReviewCaseItem] = Field(default_factory=list)


class ReviewCaseResolveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resolution: Literal["accepted", "rejected", "merged", "ignored"]
    note: str | None = Field(default=None, max_length=1000)
