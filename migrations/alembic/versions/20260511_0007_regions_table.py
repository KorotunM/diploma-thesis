"""regions reference table and region_name in search_doc

Adds:
  - core.region — canonical region names derived from university location claims
  - delivery.university_search_doc.region_name — denormalized for filter queries

Revision ID: 20260511_0007
Revises: 20260509_0006
Create Date: 2026-05-11
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260511_0007"
down_revision: Union[str, None] = "20260509_0006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "region",
        sa.Column("region_id", postgresql.UUID(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("name_normalized", sa.Text(), nullable=False),
        sa.Column("country_code", sa.Text(), nullable=False, server_default="RU"),
        sa.Column("university_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("name_normalized", name="uq_region_name_normalized"),
        schema="core",
    )
    op.create_index("idx_region_name_normalized", "region", ["name_normalized"], schema="core")

    op.add_column(
        "university_search_doc",
        sa.Column("region_name", sa.Text(), nullable=True),
        schema="delivery",
    )
    op.create_index(
        "idx_university_search_doc_region",
        "university_search_doc",
        ["region_name"],
        schema="delivery",
    )
    op.drop_index("idx_university_search_doc_filters", table_name="university_search_doc", schema="delivery")
    op.create_index(
        "idx_university_search_doc_filters",
        "university_search_doc",
        ["country_code", "city_name", "region_name"],
        schema="delivery",
    )


def downgrade() -> None:
    op.drop_index("idx_university_search_doc_filters", table_name="university_search_doc", schema="delivery")
    op.create_index(
        "idx_university_search_doc_filters",
        "university_search_doc",
        ["country_code", "city_name"],
        schema="delivery",
    )
    op.drop_index("idx_university_search_doc_region", table_name="university_search_doc", schema="delivery")
    op.drop_column("university_search_doc", "region_name", schema="delivery")

    op.drop_index("idx_region_name_normalized", table_name="region", schema="core")
    op.drop_table("region", schema="core")
