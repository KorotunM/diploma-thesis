from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Sequence
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from libs.contracts.events import CrawlRequestPayload


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class FetchedArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    raw_artifact_id: UUID = Field(default_factory=uuid4)
    source_url: str
    content_type: str
    sha256: str
    fetched_at: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExtractedFragment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fragment_id: UUID = Field(default_factory=uuid4)
    field_name: str
    value: Any
    locator: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class IntermediateRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entity_hint: str | None = None
    claims: list[dict[str, Any]] = Field(default_factory=list)


class SourceAdapter(ABC):
    source_key: str
    adapter_version: str = "0.1.0"

    @abstractmethod
    async def fetch(self, request: CrawlRequestPayload) -> FetchedArtifact:
        raise NotImplementedError

    @abstractmethod
    async def extract(self, artifact: FetchedArtifact) -> Sequence[ExtractedFragment]:
        raise NotImplementedError

    @abstractmethod
    async def map_to_intermediate(
        self,
        fragments: Sequence[ExtractedFragment],
    ) -> Sequence[IntermediateRecord]:
        raise NotImplementedError
