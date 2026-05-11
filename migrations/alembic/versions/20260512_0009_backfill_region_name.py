"""backfill region_name in university_search_doc from resolved_fact

Revision ID: 20260512_0009
Revises: 20260512_0008
Create Date: 2026-05-12
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20260512_0009"
down_revision: str | None = "20260512_0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Populate region_name for rows that were created before the column was added.
    # core.resolved_fact stores value_json as JSONB; text values are JSON-quoted strings,
    # so we strip the surrounding double-quotes with trim().
    op.execute(
        """
        UPDATE delivery.university_search_doc ssd
        SET region_name = TRIM(BOTH '"' FROM rf.value_json::text)
        FROM (
            SELECT DISTINCT ON (university_id)
                university_id,
                value_json
            FROM core.resolved_fact
            WHERE field_name = 'location.region'
              AND value_json IS NOT NULL
              AND value_json::text NOT IN ('null', '""', '')
            ORDER BY university_id, resolved_at DESC
        ) AS rf
        WHERE rf.university_id = ssd.university_id
          AND (ssd.region_name IS NULL OR ssd.region_name = '')
        """
    )


def downgrade() -> None:
    pass
