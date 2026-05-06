"""users, favorites, comparisons

Adds user authentication tables and user-action tables to the core schema:
  - core.user — registered accounts with hashed passwords
  - core.user_session — opaque bearer tokens tied to a user
  - core.favorite — per-user bookmarked universities
  - core.comparison — per-user comparison list

Revision ID: 20260506_0004
Revises: 20260505_0003
Create Date: 2026-05-06
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260506_0004"
down_revision: Union[str, None] = "20260505_0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "user",
        sa.Column("user_id", postgresql.UUID(), primary_key=True),
        sa.Column("email", postgresql.CITEXT(), nullable=False),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("email", name="uq_user_email"),
        schema="core",
    )

    op.create_table(
        "user_session",
        sa.Column("session_id", postgresql.UUID(), primary_key=True),
        sa.Column("user_id", postgresql.UUID(), nullable=False),
        sa.Column("token", postgresql.UUID(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["core.user.user_id"], ondelete="CASCADE"),
        sa.UniqueConstraint("token", name="uq_user_session_token"),
        schema="core",
    )

    op.create_table(
        "favorite",
        sa.Column("user_id", postgresql.UUID(), nullable=False),
        sa.Column("university_id", postgresql.UUID(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["user_id"], ["core.user.user_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["university_id"], ["core.university.university_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "university_id", name="pk_favorite"),
        schema="core",
    )

    op.create_table(
        "comparison",
        sa.Column("user_id", postgresql.UUID(), nullable=False),
        sa.Column("university_id", postgresql.UUID(), nullable=False),
        sa.Column("added_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["user_id"], ["core.user.user_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["university_id"], ["core.university.university_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "university_id", name="pk_comparison"),
        schema="core",
    )

    op.create_index("idx_user_session_token", "user_session", ["token"], schema="core")
    op.create_index("idx_user_session_user", "user_session", ["user_id"], schema="core")
    op.create_index("idx_favorite_user", "favorite", ["user_id"], schema="core")
    op.create_index("idx_comparison_user", "comparison", ["user_id"], schema="core")


def downgrade() -> None:
    op.drop_index("idx_comparison_user", table_name="comparison", schema="core")
    op.drop_index("idx_favorite_user", table_name="favorite", schema="core")
    op.drop_index("idx_user_session_user", table_name="user_session", schema="core")
    op.drop_index("idx_user_session_token", table_name="user_session", schema="core")

    op.drop_table("comparison", schema="core")
    op.drop_table("favorite", schema="core")
    op.drop_table("user_session", schema="core")
    op.drop_table("user", schema="core")
