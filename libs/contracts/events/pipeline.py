from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


def utc_now() -> datetime:
    return datetime.now(UTC)


class EventHeader(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID = Field(default_factory=uuid4)
    trace_id: UUID | None = None
    producer: str
    occurred_at: datetime = Field(default_factory=utc_now)
    schema_version: int = 1


class CrawlRequestPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    crawl_run_id: UUID = Field(default_factory=uuid4)
    source_key: str
    endpoint_url: str
    priority: Literal["high", "bulk"] = "bulk"
    trigger: Literal["schedule", "manual", "replay"] = "schedule"
    parser_profile: str = "default"
    requested_at: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


class CrawlRequestEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_name: Literal["crawl.request.v1"] = "crawl.request.v1"
    header: EventHeader
    payload: CrawlRequestPayload


class ParseCompletedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    crawl_run_id: UUID
    source_key: str
    endpoint_url: str
    raw_artifact_id: UUID = Field(default_factory=uuid4)
    parsed_document_id: UUID = Field(default_factory=uuid4)
    parser_version: str
    raw_bucket: str = "raw-html"
    parsed_bucket: str = "parsed-snapshots"
    extracted_fragments: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)


class ParseCompletedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_name: Literal["parse.completed.v1"] = "parse.completed.v1"
    header: EventHeader
    payload: ParseCompletedPayload


class NormalizeRequestPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    crawl_run_id: UUID
    source_key: str
    parsed_document_id: UUID
    parser_version: str
    normalizer_version: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class NormalizeRequestEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_name: Literal["normalize.request.v1"] = "normalize.request.v1"
    header: EventHeader
    payload: NormalizeRequestPayload


class CardUpdatedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university_id: UUID
    card_version: int
    delivery_table: str = "delivery.university_card"
    updated_fields: list[str] = Field(default_factory=list)
    generated_at: datetime = Field(default_factory=utc_now)


class CardUpdatedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_name: Literal["card.updated.v1"] = "card.updated.v1"
    header: EventHeader
    payload: CardUpdatedPayload


class ReviewRequiredPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    review_case_id: UUID = Field(default_factory=uuid4)
    reason: str
    priority: Literal["high", "normal"] = "normal"
    university_id: UUID | None = None
    evidence_ids: list[UUID] = Field(default_factory=list)
    queue_name: str = "review.required"
    metadata: dict[str, Any] = Field(default_factory=dict)


class ReviewRequiredEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_name: Literal["review.required.v1"] = "review.required.v1"
    header: EventHeader
    payload: ReviewRequiredPayload
