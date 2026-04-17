from datetime import datetime, timezone
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ArtifactPointer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    raw_artifact_id: UUID = Field(default_factory=uuid4)
    bucket: str
    object_key: str
    sha256: str
    source_url: str
    content_type: str
    captured_at: datetime = Field(default_factory=utc_now)


class ClaimEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    evidence_id: UUID = Field(default_factory=uuid4)
    source_key: str
    source_url: str
    raw_artifact_id: UUID
    fragment_id: UUID | None = None
    captured_at: datetime = Field(default_factory=utc_now)


class ProvenanceActivity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    activity_id: UUID = Field(default_factory=uuid4)
    activity_type: Literal["crawl", "parse", "normalize", "review", "llm-assist"]
    version: str
    executed_at: datetime = Field(default_factory=utc_now)


class ProvenanceAgent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_id: UUID = Field(default_factory=uuid4)
    agent_type: Literal["service", "human", "model"]
    name: str
    version: str | None = None
