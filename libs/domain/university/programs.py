"""Typed domain models for university programs, admissions, and legal info.

These mirror the typed tables added in migration `20260504_0002`. They live
alongside the existing `UniversityCard` Pydantic model in this package and are
used by the normalizer's projection layer to populate the new core.* tables in
addition to the embedded JSON in delivery.university_card.card_json.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

EDUCATION_LEVELS = ("bachelor", "master", "specialist", "phd")
STUDY_FORMS = ("full_time", "part_time", "distance", "mixed")
EGE_SUBJECTS = (
    "russian",
    "math",
    "physics",
    "informatics",
    "chemistry",
    "biology",
    "english",
    "history",
    "social_studies",
    "literature",
    "geography",
    "foreign_language",
)


class EducationLevel(StrEnum):
    BACHELOR = "bachelor"
    MASTER = "master"
    SPECIALIST = "specialist"
    PHD = "phd"


class StudyForm(StrEnum):
    FULL_TIME = "full_time"
    PART_TIME = "part_time"
    DISTANCE = "distance"
    MIXED = "mixed"


class Faculty(BaseModel):
    """Academic unit within a university (e.g., Faculty of Computer Science)."""

    model_config = ConfigDict(extra="forbid")

    faculty_id: UUID
    university_id: UUID
    name: str = Field(min_length=1, max_length=512)
    slug: str = Field(min_length=1, max_length=128, pattern=r"^[a-z0-9-]+$")
    metadata: dict = Field(default_factory=dict)


class Program(BaseModel):
    """Specialty offered by a university."""

    model_config = ConfigDict(extra="forbid")

    program_id: UUID
    university_id: UUID
    faculty_id: UUID | None = None
    code: str = Field(
        min_length=1,
        max_length=32,
        description="Russian specialty classifier code, e.g. '09.03.01'.",
    )
    name: str = Field(min_length=1, max_length=512)
    level: EducationLevel
    form: StudyForm = StudyForm.FULL_TIME
    duration_years: int | None = Field(default=None, ge=1, le=10)
    language: str = Field(default="ru", max_length=8)
    metadata: dict = Field(default_factory=dict)


class AdmissionExam(BaseModel):
    """One required EGE subject for a program-year."""

    model_config = ConfigDict(extra="forbid")

    program_id: UUID
    year: int = Field(ge=1900, le=2100)
    subject: Literal[EGE_SUBJECTS]  # type: ignore[valid-type]
    min_score: int | None = Field(default=None, ge=0, le=100)
    is_required: bool = True
    metadata: dict = Field(default_factory=dict)


class AdmissionYear(BaseModel):
    """Admission statistics for a program in a single year."""

    model_config = ConfigDict(extra="forbid")

    program_id: UUID
    year: int = Field(ge=1900, le=2100)
    budget_seats: int | None = Field(default=None, ge=0)
    paid_seats: int | None = Field(default=None, ge=0)
    min_score: int | None = Field(
        default=None, ge=0, le=500,
        description="Total EGE score for the cutoff applicant.",
    )
    tuition_cost_rub: Decimal | None = Field(default=None, ge=0)
    exams: list[AdmissionExam] = Field(default_factory=list)
    metadata: dict = Field(default_factory=dict)


class LegalInfo(BaseModel):
    """Legal/regulatory information about a university."""

    model_config = ConfigDict(extra="forbid")

    university_id: UUID
    inn: str | None = Field(default=None, pattern=r"^\d{10}(\d{2})?$")
    ogrn: str | None = Field(default=None, pattern=r"^\d{13}(\d{2})?$")
    accreditation_status: Literal["active", "suspended", "revoked", "unknown"] | None = None
    accreditation_valid_until: date | None = None
    founded_year: int | None = Field(default=None, ge=1000, le=2100)
    institution_type: (
        Literal["state", "municipal", "private", "autonomous", "federal", "research"] | None
    ) = None
    metadata: dict = Field(default_factory=dict)


class LocationDetail(BaseModel):
    """Region/address/geo coordinates for a university campus."""

    model_config = ConfigDict(extra="forbid")

    university_id: UUID
    region_code: str | None = Field(default=None, max_length=8)
    region_name: str | None = Field(default=None, max_length=128)
    full_address: str | None = Field(default=None, max_length=1024)
    latitude: Decimal | None = Field(default=None, ge=-90, le=90)
    longitude: Decimal | None = Field(default=None, ge=-180, le=180)
    metadata: dict = Field(default_factory=dict)


class StatisticsYearly(BaseModel):
    """Yearly statistics: enrollment, faculty staff."""

    model_config = ConfigDict(extra="forbid")

    university_id: UUID
    year: int = Field(ge=1900, le=2100)
    students_count: int | None = Field(default=None, ge=0)
    faculty_staff_count: int | None = Field(default=None, ge=0)
    recorded_at: datetime | None = None
    metadata: dict = Field(default_factory=dict)


__all__ = [
    "EDUCATION_LEVELS",
    "STUDY_FORMS",
    "EGE_SUBJECTS",
    "EducationLevel",
    "StudyForm",
    "Faculty",
    "Program",
    "AdmissionExam",
    "AdmissionYear",
    "LegalInfo",
    "LocationDetail",
    "StatisticsYearly",
]
