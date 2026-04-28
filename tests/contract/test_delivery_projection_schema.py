from pathlib import Path

DDL_PATH = Path("schemas/sql/ddl/020_tables.sql")


def test_delivery_search_projection_schema_contract_is_declared() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS delivery.university_search_doc" in ddl
    assert "canonical_name_normalized text NOT NULL" in ddl
    assert "search_document jsonb NOT NULL DEFAULT '{}'::jsonb" in ddl
    assert "search_text tsvector NOT NULL" in ddl
    assert "REFERENCES core.card_version(university_id, card_version)" in ddl
    assert "idx_university_search_doc_search_text" in ddl
    assert "idx_university_search_doc_canonical_name_trgm" in ddl
    assert "idx_university_search_doc_filters" in ddl
    assert "idx_university_search_doc_website_domain" in ddl
