"""review cases and ai usage limits

Revision ID: 20260616_0011
Revises: 20260513_0010
Create Date: 2026-06-16
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260616_0011"
down_revision: str | None = "20260513_0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "review_case",
        sa.Column(
            "review_case_id",
            postgresql.UUID(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("status", sa.Text(), nullable=False, server_default="open"),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("priority", sa.Text(), nullable=False, server_default="normal"),
        sa.Column("university_id", postgresql.UUID(), nullable=True),
        sa.Column(
            "evidence_ids",
            postgresql.ARRAY(postgresql.UUID()),
            nullable=False,
            server_default="{}",
        ),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default="{}"),
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
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_by", postgresql.UUID(), nullable=True),
        sa.Column("resolution", sa.Text(), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "status IN ('open', 'resolved', 'dismissed')",
            name="ck_review_case_status",
        ),
        sa.CheckConstraint("priority IN ('high', 'normal')", name="ck_review_case_priority"),
        sa.ForeignKeyConstraint(
            ["university_id"],
            ["core.university.university_id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(["resolved_by"], ["core.user.user_id"], ondelete="SET NULL"),
        schema="ops",
    )
    op.create_index("idx_review_case_status", "review_case", ["status"], schema="ops")
    op.create_index("idx_review_case_priority", "review_case", ["priority"], schema="ops")
    op.create_index("idx_review_case_created_at", "review_case", ["created_at"], schema="ops")

    op.create_table(
        "ai_chat_usage",
        sa.Column(
            "usage_id",
            postgresql.UUID(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("user_id", postgresql.UUID(), nullable=True),
        sa.Column("client_id", sa.Text(), nullable=True),
        sa.Column("used_on", sa.Date(), nullable=False, server_default=sa.text("CURRENT_DATE")),
        sa.Column("request_count", sa.Integer(), nullable=False, server_default="0"),
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
        sa.CheckConstraint(
            "user_id IS NOT NULL OR client_id IS NOT NULL",
            name="ck_ai_chat_usage_identity",
        ),
        sa.ForeignKeyConstraint(["user_id"], ["core.user.user_id"], ondelete="CASCADE"),
        schema="core",
    )
    op.create_unique_constraint(
        "uq_ai_chat_usage_user_day",
        "ai_chat_usage",
        ["user_id", "used_on"],
        schema="core",
    )
    op.create_unique_constraint(
        "uq_ai_chat_usage_client_day",
        "ai_chat_usage",
        ["client_id", "used_on"],
        schema="core",
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_ai_chat_usage_client_day",
        "ai_chat_usage",
        schema="core",
        type_="unique",
    )
    op.drop_constraint("uq_ai_chat_usage_user_day", "ai_chat_usage", schema="core", type_="unique")
    op.drop_table("ai_chat_usage", schema="core")
    op.drop_index("idx_review_case_created_at", table_name="review_case", schema="ops")
    op.drop_index("idx_review_case_priority", table_name="review_case", schema="ops")
    op.drop_index("idx_review_case_status", table_name="review_case", schema="ops")
    op.drop_table("review_case", schema="ops")
