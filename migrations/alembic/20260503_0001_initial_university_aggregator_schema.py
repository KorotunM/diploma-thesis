"""initial university aggregator schema

Revision ID: 20260503_0001
Revises: 
Create Date: 2026-05-03
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "20260503_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


SCHEMAS = ("ops", "ingestion", "parsing", "normalize", "core", "delivery")
EXTENSIONS = ("citext", "pg_trgm", "unaccent", "vector")


def upgrade() -> None:
    # PostgreSQL extensions used by citext columns, trigram indexes and future vector search.
    for extension in EXTENSIONS:
        op.execute(f"CREATE EXTENSION IF NOT EXISTS {extension}")

    for schema in SCHEMAS:
        op.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    op.create_table(
        "pipeline_run",
        sa.Column("run_id", postgresql.UUID(), primary_key=True),
        sa.Column("run_type", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("trigger_type", sa.Text(), nullable=False),
        sa.Column("source_key", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        schema="ops",
    )

    op.create_table(
        "source",
        sa.Column("source_id", postgresql.UUID(), primary_key=True),
        sa.Column("source_key", postgresql.CITEXT(), nullable=False),
        sa.Column("source_type", sa.Text(), nullable=False),
        sa.Column("trust_tier", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.UniqueConstraint("source_key", name="uq_source_source_key"),
        schema="ingestion",
    )

    op.create_table(
        "source_endpoint",
        sa.Column("endpoint_id", postgresql.UUID(), primary_key=True),
        sa.Column("source_id", postgresql.UUID(), nullable=False),
        sa.Column("endpoint_url", sa.Text(), nullable=False),
        sa.Column("parser_profile", sa.Text(), nullable=False, server_default=sa.text("'default'")),
        sa.Column("crawl_policy", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.ForeignKeyConstraint(["source_id"], ["ingestion.source.source_id"]),
        sa.UniqueConstraint("source_id", "endpoint_url", name="uq_source_endpoint_source_url"),
        schema="ingestion",
    )

    op.create_table(
        "raw_artifact",
        sa.Column("raw_artifact_id", postgresql.UUID(), primary_key=True),
        sa.Column("crawl_run_id", postgresql.UUID(), nullable=False),
        sa.Column("source_key", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("final_url", sa.Text(), nullable=True),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("content_type", sa.Text(), nullable=False),
        sa.Column("content_length", sa.BigInteger(), nullable=True),
        sa.Column("sha256", sa.Text(), nullable=False),
        sa.Column("storage_bucket", sa.Text(), nullable=False),
        sa.Column("storage_object_key", sa.Text(), nullable=False),
        sa.Column("etag", sa.Text(), nullable=True),
        sa.Column("last_modified", sa.Text(), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.UniqueConstraint("source_key", "sha256", name="uq_raw_artifact_source_sha256"),
        schema="ingestion",
    )

    op.create_table(
        "parsed_document",
        sa.Column("parsed_document_id", postgresql.UUID(), primary_key=True),
        sa.Column("crawl_run_id", postgresql.UUID(), nullable=False),
        sa.Column("raw_artifact_id", postgresql.UUID(), nullable=False),
        sa.Column("source_key", sa.Text(), nullable=False),
        sa.Column("parser_profile", sa.Text(), nullable=False),
        sa.Column("parser_version", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("entity_hint", sa.Text(), nullable=True),
        sa.Column("extracted_fragment_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("parsed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.ForeignKeyConstraint(["raw_artifact_id"], ["ingestion.raw_artifact.raw_artifact_id"]),
        sa.UniqueConstraint("raw_artifact_id", "parser_version", name="uq_parsed_document_raw_parser_version"),
        schema="parsing",
    )

    op.create_table(
        "extracted_fragment",
        sa.Column("fragment_id", postgresql.UUID(), primary_key=True),
        sa.Column("parsed_document_id", postgresql.UUID(), nullable=False),
        sa.Column("raw_artifact_id", postgresql.UUID(), nullable=False),
        sa.Column("source_key", sa.Text(), nullable=False),
        sa.Column("field_name", sa.Text(), nullable=False),
        sa.Column("value", postgresql.JSONB(), nullable=False),
        sa.Column("value_type", sa.Text(), nullable=False),
        sa.Column("locator", sa.Text(), nullable=True),
        sa.Column("confidence", sa.Double(), nullable=False, server_default=sa.text("1.0")),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.ForeignKeyConstraint(["parsed_document_id"], ["parsing.parsed_document.parsed_document_id"]),
        sa.ForeignKeyConstraint(["raw_artifact_id"], ["ingestion.raw_artifact.raw_artifact_id"]),
        schema="parsing",
    )

    op.create_table(
        "claim",
        sa.Column("claim_id", postgresql.UUID(), primary_key=True),
        sa.Column("parsed_document_id", postgresql.UUID(), nullable=False),
        sa.Column("source_key", sa.Text(), nullable=False),
        sa.Column("field_name", sa.Text(), nullable=False),
        sa.Column("value_json", postgresql.JSONB(), nullable=False),
        sa.Column("entity_hint", sa.Text(), nullable=True),
        sa.Column("parser_version", sa.Text(), nullable=False),
        sa.Column("normalizer_version", sa.Text(), nullable=True),
        sa.Column("parser_confidence", sa.Double(), nullable=False, server_default=sa.text("1.0")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.ForeignKeyConstraint(["parsed_document_id"], ["parsing.parsed_document.parsed_document_id"]),
        schema="normalize",
    )

    op.create_table(
        "claim_evidence",
        sa.Column("evidence_id", postgresql.UUID(), primary_key=True),
        sa.Column("claim_id", postgresql.UUID(), nullable=False),
        sa.Column("raw_artifact_id", postgresql.UUID(), nullable=False),
        sa.Column("fragment_id", postgresql.UUID(), nullable=True),
        sa.Column("source_key", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.ForeignKeyConstraint(["claim_id"], ["normalize.claim.claim_id"]),
        sa.ForeignKeyConstraint(["raw_artifact_id"], ["ingestion.raw_artifact.raw_artifact_id"]),
        schema="normalize",
    )

    op.create_table(
        "university",
        sa.Column("university_id", postgresql.UUID(), primary_key=True),
        sa.Column("canonical_name", postgresql.CITEXT(), nullable=False),
        sa.Column("canonical_domain", postgresql.CITEXT(), nullable=True),
        sa.Column("country_code", sa.Text(), nullable=True),
        sa.Column("city_name", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        schema="core",
    )

    op.create_table(
        "university_alias",
        sa.Column("alias_id", postgresql.UUID(), primary_key=True),
        sa.Column("university_id", postgresql.UUID(), nullable=False),
        sa.Column("alias_name", postgresql.CITEXT(), nullable=False),
        sa.Column("alias_kind", sa.Text(), nullable=False, server_default=sa.text("'display'")),
        sa.ForeignKeyConstraint(["university_id"], ["core.university.university_id"]),
        schema="core",
    )

    op.create_table(
        "resolved_fact",
        sa.Column("resolved_fact_id", postgresql.UUID(), primary_key=True),
        sa.Column("university_id", postgresql.UUID(), nullable=False),
        sa.Column("field_name", sa.Text(), nullable=False),
        sa.Column("value_json", postgresql.JSONB(), nullable=False),
        sa.Column("fact_score", sa.Double(), nullable=False),
        sa.Column("resolution_policy", sa.Text(), nullable=False),
        sa.Column("card_version", sa.Integer(), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.ForeignKeyConstraint(["university_id"], ["core.university.university_id"]),
        schema="core",
    )

    op.create_table(
        "card_version",
        sa.Column("university_id", postgresql.UUID(), nullable=False),
        sa.Column("card_version", sa.Integer(), nullable=False),
        sa.Column("normalizer_version", sa.Text(), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["university_id"], ["core.university.university_id"]),
        sa.PrimaryKeyConstraint("university_id", "card_version", name="pk_card_version"),
        schema="core",
    )

    op.create_table(
        "university_card",
        sa.Column("university_id", postgresql.UUID(), nullable=False),
        sa.Column("card_version", sa.Integer(), nullable=False),
        sa.Column("card_json", postgresql.JSONB(), nullable=False),
        sa.Column("search_text", postgresql.TSVECTOR(), nullable=True),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("university_id", "card_version", name="pk_university_card"),
        schema="delivery",
    )

    op.create_table(
        "university_search_doc",
        sa.Column("university_id", postgresql.UUID(), nullable=False),
        sa.Column("card_version", sa.Integer(), nullable=False),
        sa.Column("canonical_name", sa.Text(), nullable=False),
        sa.Column("canonical_name_normalized", sa.Text(), nullable=False),
        sa.Column("website_url", sa.Text(), nullable=True),
        sa.Column("website_domain", postgresql.CITEXT(), nullable=True),
        sa.Column("country_code", sa.Text(), nullable=True),
        sa.Column("city_name", sa.Text(), nullable=True),
        sa.Column("aliases", postgresql.ARRAY(sa.Text()), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("search_document", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("search_text", postgresql.TSVECTOR(), nullable=False),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(
            ["university_id", "card_version"],
            ["core.card_version.university_id", "core.card_version.card_version"],
        ),
        sa.PrimaryKeyConstraint("university_id", "card_version", name="pk_university_search_doc"),
        schema="delivery",
    )

    op.create_index("idx_raw_artifact_sha256", "raw_artifact", ["sha256"], schema="ingestion")
    op.create_index("idx_parsed_document_raw_artifact", "parsed_document", ["raw_artifact_id"], schema="parsing")
    op.create_index("idx_extracted_fragment_document", "extracted_fragment", ["parsed_document_id"], schema="parsing")
    op.create_index("idx_extracted_fragment_raw_artifact", "extracted_fragment", ["raw_artifact_id"], schema="parsing")
    op.create_index("idx_claim_parsed_document", "claim", ["parsed_document_id"], schema="normalize")
    op.create_index("idx_claim_field_name", "claim", ["field_name"], schema="normalize")
    op.create_index("idx_claim_evidence_claim", "claim_evidence", ["claim_id"], schema="normalize")
    op.create_index("idx_claim_evidence_raw_artifact", "claim_evidence", ["raw_artifact_id"], schema="normalize")
    op.create_index("idx_claim_evidence_fragment", "claim_evidence", ["fragment_id"], schema="normalize")
    op.create_index("idx_university_canonical_domain", "university", ["canonical_domain"], schema="core")
    op.create_index("idx_university_canonical_name", "university", ["canonical_name"], schema="core")
    op.create_index(
        "idx_university_canonical_name_trgm",
        "university",
        ["canonical_name"],
        schema="core",
        postgresql_using="gin",
        postgresql_ops={"canonical_name": "gin_trgm_ops"},
    )
    op.create_index("idx_resolved_fact_university_card", "resolved_fact", ["university_id", "card_version"], schema="core")
    op.create_index(
        "idx_delivery_search_text",
        "university_card",
        ["search_text"],
        schema="delivery",
        postgresql_using="gin",
    )
    op.create_index(
        "idx_university_search_doc_search_text",
        "university_search_doc",
        ["search_text"],
        schema="delivery",
        postgresql_using="gin",
    )
    op.create_index(
        "idx_university_search_doc_canonical_name_trgm",
        "university_search_doc",
        ["canonical_name"],
        schema="delivery",
        postgresql_using="gin",
        postgresql_ops={"canonical_name": "gin_trgm_ops"},
    )
    op.create_index("idx_university_search_doc_filters", "university_search_doc", ["country_code", "city_name"], schema="delivery")
    op.create_index("idx_university_search_doc_website_domain", "university_search_doc", ["website_domain"], schema="delivery")


def downgrade() -> None:
    op.drop_index("idx_university_search_doc_website_domain", table_name="university_search_doc", schema="delivery")
    op.drop_index("idx_university_search_doc_filters", table_name="university_search_doc", schema="delivery")
    op.drop_index("idx_university_search_doc_canonical_name_trgm", table_name="university_search_doc", schema="delivery")
    op.drop_index("idx_university_search_doc_search_text", table_name="university_search_doc", schema="delivery")
    op.drop_index("idx_delivery_search_text", table_name="university_card", schema="delivery")
    op.drop_index("idx_resolved_fact_university_card", table_name="resolved_fact", schema="core")
    op.drop_index("idx_university_canonical_name_trgm", table_name="university", schema="core")
    op.drop_index("idx_university_canonical_name", table_name="university", schema="core")
    op.drop_index("idx_university_canonical_domain", table_name="university", schema="core")
    op.drop_index("idx_claim_evidence_fragment", table_name="claim_evidence", schema="normalize")
    op.drop_index("idx_claim_evidence_raw_artifact", table_name="claim_evidence", schema="normalize")
    op.drop_index("idx_claim_evidence_claim", table_name="claim_evidence", schema="normalize")
    op.drop_index("idx_claim_field_name", table_name="claim", schema="normalize")
    op.drop_index("idx_claim_parsed_document", table_name="claim", schema="normalize")
    op.drop_index("idx_extracted_fragment_raw_artifact", table_name="extracted_fragment", schema="parsing")
    op.drop_index("idx_extracted_fragment_document", table_name="extracted_fragment", schema="parsing")
    op.drop_index("idx_parsed_document_raw_artifact", table_name="parsed_document", schema="parsing")
    op.drop_index("idx_raw_artifact_sha256", table_name="raw_artifact", schema="ingestion")

    op.drop_table("university_search_doc", schema="delivery")
    op.drop_table("university_card", schema="delivery")
    op.drop_table("card_version", schema="core")
    op.drop_table("resolved_fact", schema="core")
    op.drop_table("university_alias", schema="core")
    op.drop_table("university", schema="core")
    op.drop_table("claim_evidence", schema="normalize")
    op.drop_table("claim", schema="normalize")
    op.drop_table("extracted_fragment", schema="parsing")
    op.drop_table("parsed_document", schema="parsing")
    op.drop_table("raw_artifact", schema="ingestion")
    op.drop_table("source_endpoint", schema="ingestion")
    op.drop_table("source", schema="ingestion")
    op.drop_table("pipeline_run", schema="ops")

    for schema in reversed(SCHEMAS):
        op.execute(f"DROP SCHEMA IF EXISTS {schema}")

    # Drop extensions last. Remove these lines if extensions are managed globally outside Alembic.
    for extension in reversed(EXTENSIONS):
        op.execute(f"DROP EXTENSION IF EXISTS {extension}")
