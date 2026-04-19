from __future__ import annotations

from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

SOURCE_KEY_PATTERN = r"^[a-z0-9][a-z0-9._-]{1,62}[a-z0-9]$"


class SourceType(StrEnum):
    OFFICIAL_SITE = "official_site"
    AGGREGATOR = "aggregator"
    RANKING = "ranking"
    GOVERNMENT_REGISTRY = "government_registry"
    MANUAL_SEED = "manual_seed"


class SourceTrustTier(StrEnum):
    AUTHORITATIVE = "authoritative"
    TRUSTED = "trusted"
    AUXILIARY = "auxiliary"
    EXPERIMENTAL = "experimental"


class SourceRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: UUID
    source_key: str = Field(pattern=SOURCE_KEY_PATTERN)
    source_type: SourceType
    trust_tier: SourceTrustTier
    is_active: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


class CreateSourceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: UUID = Field(default_factory=uuid4)
    source_key: str = Field(pattern=SOURCE_KEY_PATTERN)
    source_type: SourceType
    trust_tier: SourceTrustTier
    is_active: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


class UpdateSourceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_type: SourceType | None = None
    trust_tier: SourceTrustTier | None = None
    is_active: bool | None = None
    metadata: dict[str, Any] | None = None

    @model_validator(mode="after")
    def require_at_least_one_field(self) -> UpdateSourceRequest:
        if not self.model_fields_set:
            raise ValueError("At least one source field must be provided for update.")
        return self


class SourceResponse(SourceRecord):
    pass


class SourceListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total: int
    limit: int
    offset: int
    items: list[SourceResponse]
