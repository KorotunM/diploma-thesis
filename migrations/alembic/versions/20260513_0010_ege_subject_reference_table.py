"""ege_subject reference table

Adds core.ege_subject — a canonical lookup table of Unified State Exam (ЕГЭ)
subjects. Other tables (core.admission_exam) reference it via a foreign key on
the subject code column so the set of valid subjects is enforced at the DB level.

Revision ID: 20260513_0010
Revises: 20260512_0009
Create Date: 2026-05-13
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260513_0010"
down_revision: Union[str, None] = "20260512_0009"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Canonical list — order determines sort_order.
_EGE_SUBJECTS = [
    ("russian",     "Русский язык"),
    ("math",        "Математика"),
    ("physics",     "Физика"),
    ("chemistry",   "Химия"),
    ("biology",     "Биология"),
    ("informatics", "Информатика"),
    ("social",      "Обществознание"),
    ("history",     "История"),
    ("literature",  "Литература"),
    ("geography",   "География"),
    ("foreign",     "Иностранные языки"),
]


def upgrade() -> None:
    op.create_table(
        "ege_subject",
        sa.Column("subject_id", postgresql.UUID(), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("code",       sa.Text(), nullable=False),
        sa.Column("label",      sa.Text(), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.UniqueConstraint("code", name="uq_ege_subject_code"),
        schema="core",
    )
    op.create_index("idx_ege_subject_code", "ege_subject", ["code"], schema="core")

    # Seed the canonical subjects.
    ege_subject = sa.table(
        "ege_subject",
        sa.column("code",       sa.Text()),
        sa.column("label",      sa.Text()),
        sa.column("sort_order", sa.Integer()),
        schema="core",
    )
    op.bulk_insert(
        ege_subject,
        [
            {"code": code, "label": label, "sort_order": idx}
            for idx, (code, label) in enumerate(_EGE_SUBJECTS)
        ],
    )

    # Add FK on core.admission_exam.subject → core.ege_subject.code so that
    # only known subject codes can be stored in admission records.
    op.create_foreign_key(
        "fk_admission_exam_ege_subject",
        "admission_exam",
        "ege_subject",
        ["subject"],
        ["code"],
        source_schema="core",
        referent_schema="core",
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_admission_exam_ege_subject",
        "admission_exam",
        schema="core",
        type_="foreignkey",
    )
    op.drop_index("idx_ege_subject_code", table_name="ege_subject", schema="core")
    op.drop_table("ege_subject", schema="core")
