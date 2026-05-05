"""add app_user table for admin authentication

Revision ID: 20260505_0003
Revises: 20260504_0002
Create Date: 2026-05-05
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "20260505_0003"
down_revision: Union[str, None] = "20260504_0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "app_user",
        sa.Column("user_id", sa.Text(), primary_key=True),
        sa.Column("username", sa.Text(), nullable=False),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False, server_default=sa.text("'viewer'")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("username", name="uq_app_user_username"),
        schema="ops",
    )

    # Seed default admin user (password stored as plain text for demo purposes)
    op.execute(
        """
        INSERT INTO ops.app_user (user_id, username, password_hash, role)
        VALUES ('00000000-0000-0000-0000-000000000001', 'admin', 'admin', 'admin')
        ON CONFLICT (username) DO NOTHING
        """
    )


def downgrade() -> None:
    op.drop_table("app_user", schema="ops")
