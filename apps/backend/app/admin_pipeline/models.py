from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class PipelineRerunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_key: str | None = None
    priority: Literal["high", "bulk"] = "high"


class PipelineRerunResultItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    source_key: str
    endpoint_id: UUID
    endpoint_url: str
    crawl_run_id: UUID
    status: Literal["published", "failed"]
    detail: str | None = None


class PipelineRerunResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    triggered: int
    failed: int
    scope: Literal["all", "source"]
    items: list[PipelineRerunResultItem] = Field(default_factory=list)


class PipelineRunItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    run_id: UUID
    run_type: str
    status: str
    trigger_type: str
    source_key: str | None = None
    started_at: datetime
    finished_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PipelineRunsResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    total: int
    items: list[PipelineRunItem] = Field(default_factory=list)


class PipelineSourceItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    source_key: str
    source_type: str
    trust_tier: str
    is_active: bool = True


class PipelineSourcesResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    items: list[PipelineSourceItem] = Field(default_factory=list)
