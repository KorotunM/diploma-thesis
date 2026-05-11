"""university ranking history

Revision ID: 20260512_0008
Revises: 20260511_0007
Create Date: 2026-05-12
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260512_0008"
down_revision: str | None = "20260511_0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "university_ranking_history",
        sa.Column(
            "history_id",
            postgresql.UUID(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("university_id", postgresql.UUID(), nullable=True),
        sa.Column("source_key", sa.Text(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("score", sa.Numeric(precision=8, scale=2), nullable=True),
        sa.Column("category", sa.Text(), nullable=True),
        sa.Column("change_direction", sa.Text(), nullable=True),
        sa.Column("change_delta", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("canonical_name", sa.Text(), nullable=False),
        sa.Column("external_id", sa.Text(), nullable=True),
        sa.Column(
            "captured_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["university_id"],
            ["core.university.university_id"],
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint("source_key", "year", "rank", name="uq_ranking_history_source_year_rank"),
        schema="core",
    )
    op.create_index(
        "idx_ranking_history_university",
        "university_ranking_history",
        ["university_id", "source_key"],
        schema="core",
    )
    op.create_index(
        "idx_ranking_history_source_year",
        "university_ranking_history",
        ["source_key", "year"],
        schema="core",
    )


def downgrade() -> None:
    op.drop_index(
        "idx_ranking_history_source_year",
        table_name="university_ranking_history",
        schema="core",
    )
    op.drop_index(
        "idx_ranking_history_university",
        table_name="university_ranking_history",
        schema="core",
    )
    op.drop_table("university_ranking_history", schema="core")
