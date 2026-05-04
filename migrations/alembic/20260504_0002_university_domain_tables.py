"""university domain tables: faculty, program, admission_year, admission_exam, legal_info, location_detail, statistics_yearly

Adds typed tables in the `core` schema for the university domain (faculties,
programs, admission years, admission exams, legal/regulatory info, location
details, yearly statistics). Until now the only place this data could live was
the JSON blob inside `delivery.university_card.card_json`, which can't be
queried, indexed, or constrained. The normalizer's projection layer is
responsible for keeping both the typed tables and the card JSON consistent.

Revision ID: 20260504_0002
Revises: 20260503_0001
Create Date: 2026-05-04
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "20260504_0002"
down_revision: Union[str, None] = "20260503_0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "faculty",
        sa.Column("faculty_id", postgresql.UUID(), primary_key=True),
        sa.Column("university_id", postgresql.UUID(), nullable=False),
        sa.Column("name", postgresql.CITEXT(), nullable=False),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.ForeignKeyConstraint(["university_id"], ["core.university.university_id"]),
        sa.UniqueConstraint("university_id", "slug", name="uq_faculty_university_slug"),
        schema="core",
    )

    op.create_table(
        "program",
        sa.Column("program_id", postgresql.UUID(), primary_key=True),
        sa.Column("university_id", postgresql.UUID(), nullable=False),
        sa.Column("faculty_id", postgresql.UUID(), nullable=True),
        sa.Column("code", postgresql.CITEXT(), nullable=False),
        sa.Column("name", postgresql.CITEXT(), nullable=False),
        sa.Column("level", sa.Text(), nullable=False),
        sa.Column("form", sa.Text(), nullable=False, server_default=sa.text("'full_time'")),
        sa.Column("duration_years", sa.Integer(), nullable=True),
        sa.Column("language", sa.Text(), nullable=False, server_default=sa.text("'ru'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.ForeignKeyConstraint(["university_id"], ["core.university.university_id"]),
        sa.ForeignKeyConstraint(["faculty_id"], ["core.faculty.faculty_id"], ondelete="SET NULL"),
        sa.UniqueConstraint("university_id", "code", "level", "form", name="uq_program_university_code_level_form"),
        sa.CheckConstraint(
            "level IN ('bachelor','master','specialist','phd')",
            name="ck_program_level",
        ),
        sa.CheckConstraint(
            "form IN ('full_time','part_time','distance','mixed')",
            name="ck_program_form",
        ),
        schema="core",
    )

    op.create_table(
        "admission_year",
        sa.Column("program_id", postgresql.UUID(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("budget_seats", sa.Integer(), nullable=True),
        sa.Column("paid_seats", sa.Integer(), nullable=True),
        sa.Column("min_score", sa.Integer(), nullable=True),
        sa.Column("tuition_cost_rub", sa.Numeric(12, 2), nullable=True),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["program_id"], ["core.program.program_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("program_id", "year", name="pk_admission_year"),
        sa.CheckConstraint("year BETWEEN 1900 AND 2100", name="ck_admission_year_range"),
        schema="core",
    )

    op.create_table(
        "admission_exam",
        sa.Column("program_id", postgresql.UUID(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("subject", sa.Text(), nullable=False),
        sa.Column("min_score", sa.Integer(), nullable=True),
        sa.Column("is_required", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.ForeignKeyConstraint(
            ["program_id", "year"],
            ["core.admission_year.program_id", "core.admission_year.year"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("program_id", "year", "subject", name="pk_admission_exam"),
        schema="core",
    )

    op.create_table(
        "legal_info",
        sa.Column("university_id", postgresql.UUID(), primary_key=True),
        sa.Column("inn", sa.Text(), nullable=True),
        sa.Column("ogrn", sa.Text(), nullable=True),
        sa.Column("accreditation_status", sa.Text(), nullable=True),
        sa.Column("accreditation_valid_until", sa.Date(), nullable=True),
        sa.Column("founded_year", sa.Integer(), nullable=True),
        sa.Column("institution_type", sa.Text(), nullable=True),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(
            ["university_id"], ["core.university.university_id"], ondelete="CASCADE"
        ),
        sa.CheckConstraint(
            "founded_year IS NULL OR founded_year BETWEEN 1000 AND 2100",
            name="ck_legal_info_founded_year",
        ),
        schema="core",
    )

    op.create_table(
        "location_detail",
        sa.Column("university_id", postgresql.UUID(), primary_key=True),
        sa.Column("region_code", sa.Text(), nullable=True),
        sa.Column("region_name", sa.Text(), nullable=True),
        sa.Column("full_address", sa.Text(), nullable=True),
        sa.Column("latitude", sa.Numeric(9, 6), nullable=True),
        sa.Column("longitude", sa.Numeric(9, 6), nullable=True),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(
            ["university_id"], ["core.university.university_id"], ondelete="CASCADE"
        ),
        sa.CheckConstraint(
            "(latitude IS NULL AND longitude IS NULL) "
            "OR (latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180)",
            name="ck_location_detail_geo_range",
        ),
        schema="core",
    )

    op.create_table(
        "statistics_yearly",
        sa.Column("university_id", postgresql.UUID(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("students_count", sa.Integer(), nullable=True),
        sa.Column("faculty_staff_count", sa.Integer(), nullable=True),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(
            ["university_id"], ["core.university.university_id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("university_id", "year", name="pk_statistics_yearly"),
        sa.CheckConstraint("year BETWEEN 1900 AND 2100", name="ck_statistics_yearly_range"),
        schema="core",
    )

    # Indexes for typical query patterns
    op.create_index(
        "idx_faculty_university",
        "faculty",
        ["university_id"],
        schema="core",
    )
    op.create_index(
        "idx_program_university",
        "program",
        ["university_id"],
        schema="core",
    )
    op.create_index(
        "idx_program_code",
        "program",
        ["code"],
        schema="core",
    )
    op.create_index(
        "idx_program_level_form",
        "program",
        ["level", "form"],
        schema="core",
    )
    op.create_index(
        "idx_program_name_trgm",
        "program",
        ["name"],
        schema="core",
        postgresql_using="gin",
        postgresql_ops={"name": "gin_trgm_ops"},
    )
    op.create_index(
        "idx_admission_year_year",
        "admission_year",
        ["year"],
        schema="core",
    )
    op.create_index(
        "idx_admission_year_min_score",
        "admission_year",
        ["min_score"],
        schema="core",
    )
    op.create_index(
        "idx_legal_info_inn",
        "legal_info",
        ["inn"],
        schema="core",
    )
    op.create_index(
        "idx_location_detail_region",
        "location_detail",
        ["region_code"],
        schema="core",
    )


def downgrade() -> None:
    op.drop_index("idx_location_detail_region", table_name="location_detail", schema="core")
    op.drop_index("idx_legal_info_inn", table_name="legal_info", schema="core")
    op.drop_index("idx_admission_year_min_score", table_name="admission_year", schema="core")
    op.drop_index("idx_admission_year_year", table_name="admission_year", schema="core")
    op.drop_index("idx_program_name_trgm", table_name="program", schema="core")
    op.drop_index("idx_program_level_form", table_name="program", schema="core")
    op.drop_index("idx_program_code", table_name="program", schema="core")
    op.drop_index("idx_program_university", table_name="program", schema="core")
    op.drop_index("idx_faculty_university", table_name="faculty", schema="core")

    op.drop_table("statistics_yearly", schema="core")
    op.drop_table("location_detail", schema="core")
    op.drop_table("legal_info", schema="core")
    op.drop_table("admission_exam", schema="core")
    op.drop_table("admission_year", schema="core")
    op.drop_table("program", schema="core")
    op.drop_table("faculty", schema="core")
