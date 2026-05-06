from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class FieldAttribution(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_key: str
    source_url: str
    evidence_ids: list[UUID] = Field(default_factory=list)


class ConfidenceValue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value: str | int | float | None
    confidence: float
    sources: list[FieldAttribution] = Field(default_factory=list)


class LocationInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    country: str | None = None
    city: str | None = None
    address: str | None = None
    geo: dict[str, float] | None = None


class ContactsInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    website: str | None = None
    logo_url: str | None = None
    emails: list[str] = Field(default_factory=list)
    phones: list[str] = Field(default_factory=list)


class InstitutionalInfo(BaseModel):
    institution_type: str | None = Field(default=None, alias="type")
    founded_year: int | None = None
    category: str | None = None
    is_flagship: bool | None = None

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class RatingItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: str
    year: int
    metric: str
    value: str


class ReviewSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: str | None = None
    rating: float | None = None
    rating_count: int | None = None
    items: list[dict[str, Any]] = Field(default_factory=list)


class CardVersionInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    card_version: int
    generated_at: datetime = Field(default_factory=utc_now)


class UniversityCard(BaseModel):
    model_config = ConfigDict(extra="forbid")

    university_id: UUID = Field(default_factory=uuid4)
    canonical_name: ConfidenceValue
    aliases: list[str] = Field(default_factory=list)
    description: str | None = None
    location: LocationInfo = Field(default_factory=LocationInfo)
    contacts: ContactsInfo = Field(default_factory=ContactsInfo)
    institutional: InstitutionalInfo = Field(
        default_factory=lambda: InstitutionalInfo.model_validate({"type": None, "founded_year": None})
    )
    programs: list[dict[str, Any]] = Field(default_factory=list)
    tuition: list[dict[str, Any]] = Field(default_factory=list)
    ratings: list[RatingItem] = Field(default_factory=list)
    dormitory: dict[str, Any] = Field(default_factory=dict)
    reviews: ReviewSummary = Field(default_factory=ReviewSummary)
    sources: list[FieldAttribution] = Field(default_factory=list)
    version: CardVersionInfo

    @classmethod
    def sample(cls) -> "UniversityCard":
        return cls(
            canonical_name=ConfidenceValue(
                value="Example State University",
                confidence=0.98,
                sources=[
                    FieldAttribution(
                        source_key="official-site",
                        source_url="https://example.edu",
                    )
                ],
            ),
            aliases=["ESU"],
            location=LocationInfo(country="Russia", city="Moscow"),
            contacts=ContactsInfo(website="https://example.edu", emails=["admissions@example.edu"]),
            institutional=InstitutionalInfo.model_validate({"type": "public", "founded_year": 1965}),
            ratings=[RatingItem(provider="Example Ranking", year=2025, metric="national", value="12")],
            sources=[
                FieldAttribution(
                    source_key="official-site",
                    source_url="https://example.edu",
                )
            ],
            version=CardVersionInfo(card_version=1),
        )
