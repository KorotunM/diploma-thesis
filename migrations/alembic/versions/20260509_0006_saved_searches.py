"""saved searches

Revision ID: 20260509_0006
Revises: 20260507_0005
Create Date: 2026-05-09
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260509_0006"
down_revision: Union[str, None] = "20260507_0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "saved_search",
        sa.Column("saved_search_id", postgresql.UUID(), primary_key=True),
        sa.Column("user_id", postgresql.UUID(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("query", sa.Text(), nullable=False, server_default=""),
        sa.Column("filters", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("page_size", sa.Integer(), nullable=False, server_default="20"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["user_id"], ["core.user.user_id"], ondelete="CASCADE"),
        schema="core",
    )
    op.create_index("idx_saved_search_user", "saved_search", ["user_id"], schema="core")
    op.create_index(
        "idx_saved_search_user_updated",
        "saved_search",
        ["user_id", "updated_at"],
        schema="core",
    )


def downgrade() -> None:
    op.drop_index("idx_saved_search_user_updated", table_name="saved_search", schema="core")
    op.drop_index("idx_saved_search_user", table_name="saved_search", schema="core")
    op.drop_table("saved_search", schema="core")
