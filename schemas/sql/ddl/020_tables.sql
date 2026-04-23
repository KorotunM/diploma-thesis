CREATE TABLE IF NOT EXISTS ops.pipeline_run (
    run_id uuid PRIMARY KEY,
    run_type text NOT NULL,
    status text NOT NULL,
    trigger_type text NOT NULL,
    source_key text,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS ingestion.source (
    source_id uuid PRIMARY KEY,
    source_key citext NOT NULL UNIQUE,
    source_type text NOT NULL,
    trust_tier text NOT NULL,
    is_active boolean NOT NULL DEFAULT true,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS ingestion.source_endpoint (
    endpoint_id uuid PRIMARY KEY,
    source_id uuid NOT NULL REFERENCES ingestion.source(source_id),
    endpoint_url text NOT NULL,
    parser_profile text NOT NULL DEFAULT 'default',
    crawl_policy jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (source_id, endpoint_url)
);

CREATE TABLE IF NOT EXISTS ingestion.raw_artifact (
    raw_artifact_id uuid PRIMARY KEY,
    crawl_run_id uuid NOT NULL,
    source_key text NOT NULL,
    source_url text NOT NULL,
    final_url text,
    http_status integer,
    content_type text NOT NULL,
    content_length bigint,
    sha256 text NOT NULL,
    storage_bucket text NOT NULL,
    storage_object_key text NOT NULL,
    etag text,
    last_modified text,
    fetched_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (source_key, sha256)
);

CREATE TABLE IF NOT EXISTS parsing.parsed_document (
    parsed_document_id uuid PRIMARY KEY,
    crawl_run_id uuid NOT NULL,
    raw_artifact_id uuid NOT NULL REFERENCES ingestion.raw_artifact(raw_artifact_id),
    source_key text NOT NULL,
    parser_profile text NOT NULL,
    parser_version text NOT NULL,
    entity_type text NOT NULL,
    entity_hint text,
    extracted_fragment_count integer NOT NULL DEFAULT 0,
    parsed_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (raw_artifact_id, parser_version)
);

CREATE TABLE IF NOT EXISTS parsing.extracted_fragment (
    fragment_id uuid PRIMARY KEY,
    parsed_document_id uuid NOT NULL REFERENCES parsing.parsed_document(parsed_document_id),
    raw_artifact_id uuid NOT NULL REFERENCES ingestion.raw_artifact(raw_artifact_id),
    source_key text NOT NULL,
    field_name text NOT NULL,
    value jsonb NOT NULL,
    value_type text NOT NULL,
    locator text,
    confidence double precision NOT NULL DEFAULT 1.0,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS normalize.claim (
    claim_id uuid PRIMARY KEY,
    parsed_document_id uuid NOT NULL REFERENCES parsing.parsed_document(parsed_document_id),
    source_key text NOT NULL,
    field_name text NOT NULL,
    value_json jsonb NOT NULL,
    entity_hint text,
    parser_version text NOT NULL,
    normalizer_version text,
    parser_confidence double precision NOT NULL DEFAULT 1.0,
    created_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS normalize.claim_evidence (
    evidence_id uuid PRIMARY KEY,
    claim_id uuid NOT NULL REFERENCES normalize.claim(claim_id),
    raw_artifact_id uuid NOT NULL REFERENCES ingestion.raw_artifact(raw_artifact_id),
    fragment_id uuid,
    source_key text NOT NULL,
    source_url text NOT NULL,
    captured_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS core.university (
    university_id uuid PRIMARY KEY,
    canonical_name citext NOT NULL,
    canonical_domain citext,
    country_code text,
    city_name text,
    created_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS core.university_alias (
    alias_id uuid PRIMARY KEY,
    university_id uuid NOT NULL REFERENCES core.university(university_id),
    alias_name citext NOT NULL,
    alias_kind text NOT NULL DEFAULT 'display'
);

CREATE TABLE IF NOT EXISTS core.resolved_fact (
    resolved_fact_id uuid PRIMARY KEY,
    university_id uuid NOT NULL REFERENCES core.university(university_id),
    field_name text NOT NULL,
    value_json jsonb NOT NULL,
    fact_score double precision NOT NULL,
    resolution_policy text NOT NULL,
    card_version integer NOT NULL,
    resolved_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS core.card_version (
    university_id uuid NOT NULL REFERENCES core.university(university_id),
    card_version integer NOT NULL,
    normalizer_version text NOT NULL,
    generated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (university_id, card_version)
);

CREATE TABLE IF NOT EXISTS delivery.university_card (
    university_id uuid NOT NULL,
    card_version integer NOT NULL,
    card_json jsonb NOT NULL,
    search_text tsvector,
    generated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (university_id, card_version)
);

CREATE INDEX IF NOT EXISTS idx_raw_artifact_sha256 ON ingestion.raw_artifact (sha256);
CREATE INDEX IF NOT EXISTS idx_parsed_document_raw_artifact ON parsing.parsed_document (raw_artifact_id);
CREATE INDEX IF NOT EXISTS idx_extracted_fragment_document ON parsing.extracted_fragment (parsed_document_id);
CREATE INDEX IF NOT EXISTS idx_extracted_fragment_raw_artifact ON parsing.extracted_fragment (raw_artifact_id);
CREATE INDEX IF NOT EXISTS idx_claim_parsed_document ON normalize.claim (parsed_document_id);
CREATE INDEX IF NOT EXISTS idx_claim_field_name ON normalize.claim (field_name);
CREATE INDEX IF NOT EXISTS idx_claim_evidence_claim ON normalize.claim_evidence (claim_id);
CREATE INDEX IF NOT EXISTS idx_claim_evidence_raw_artifact ON normalize.claim_evidence (raw_artifact_id);
CREATE INDEX IF NOT EXISTS idx_claim_evidence_fragment ON normalize.claim_evidence (fragment_id);
CREATE INDEX IF NOT EXISTS idx_university_canonical_name_trgm ON core.university USING gin (canonical_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_delivery_search_text ON delivery.university_card USING gin (search_text);
