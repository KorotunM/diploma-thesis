from __future__ import annotations

import os

from alembic import context
from sqlalchemy import create_engine, pool


def _db_url() -> str:
    user = os.environ.get("POSTGRES_USER", "aggregator")
    password = os.environ.get("POSTGRES_PASSWORD", "aggregator")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "aggregator")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"


if not context.is_offline_mode():
    connectable = create_engine(
        _db_url(),
        poolclass=pool.NullPool,
        execution_options={"isolation_level": "AUTOCOMMIT"},
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, version_table_schema="public")
        context.run_migrations()
