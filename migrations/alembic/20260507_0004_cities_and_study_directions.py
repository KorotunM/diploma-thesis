"""cities and study_directions reference tables

Adds two reference/lookup tables to the core schema:
  - core.city — canonical city names discovered from university location claims
  - core.study_direction — canonical study direction names from UGNS classifier

Revision ID: 20260507_0004
Revises: 20260506_0003
Create Date: 2026-05-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260507_0004"
down_revision: Union[str, None] = "20260506_0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "city",
        sa.Column("city_id", postgresql.UUID(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("name_normalized", sa.Text(), nullable=False),
        sa.Column("region", sa.Text(), nullable=True),
        sa.Column("country_code", sa.Text(), nullable=False, server_default="RU"),
        sa.Column("university_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("name_normalized", name="uq_city_name_normalized"),
        schema="core",
    )
    op.create_index("idx_city_name_normalized", "city", ["name_normalized"], schema="core")

    op.create_table(
        "study_direction",
        sa.Column("direction_id", postgresql.UUID(), primary_key=True),
        sa.Column("code", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("level", sa.Text(), nullable=False, server_default="Бакалавриат"),
        sa.Column("ugns_group", sa.Text(), nullable=True),
        sa.Column("university_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("code", name="uq_study_direction_code"),
        schema="core",
    )
    op.create_index("idx_study_direction_code", "study_direction", ["code"], schema="core")
    op.create_index("idx_study_direction_ugns_group", "study_direction", ["ugns_group"], schema="core")


def downgrade() -> None:
    op.drop_index("idx_study_direction_ugns_group", table_name="study_direction", schema="core")
    op.drop_index("idx_study_direction_code", table_name="study_direction", schema="core")
    op.drop_table("study_direction", schema="core")

    op.drop_index("idx_city_name_normalized", table_name="city", schema="core")
    op.drop_table("city", schema="core")
