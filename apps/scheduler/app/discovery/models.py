from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


def utc_now() -> datetime:
    return datetime.now(UTC)


class DiscoveryMaterializationAction(StrEnum):
    CREATED = "created"
    UPDATED = "updated"
    EXISTING = "existing"
    DRY_RUN = "dry_run"


class DiscoveryMaterializationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    discovery_run_id: UUID = Field(default_factory=uuid4)
    source_key: str = "tabiturient-aggregator"
    dry_run: bool = False
    limit: int | None = Field(default=None, ge=1, le=5000)


class DiscoveryMaterializationResultItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    endpoint_url: str
    parser_profile: str
    action: DiscoveryMaterializationAction


class DiscoveryMaterializationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    discovery_run_id: UUID
    source_key: str
    parent_endpoint_url: str
    fetched_at: datetime = Field(default_factory=utc_now)
    dry_run: bool
    discovered_total: int
    materialized_count: int
    existing_count: int
    items: list[DiscoveryMaterializationResultItem]
