export interface BackendSearchItem {
  university_id: string;
  card_version: number;
  canonical_name: string;
  city: string | null;
  country_code: string | null;
  website: string | null;
  aliases: string[];
  score: number;
  match_signals: string[];
}

export interface BackendSearchFiltersDto {
  city: string | null;
  country: string | null;
  source_type: string | null;
}

export interface BackendSearchResponse {
  query: string;
  total: number;
  page: number;
  page_size: number;
  has_more: boolean;
  filters: BackendSearchFiltersDto;
  items: BackendSearchItem[];
}

export interface FieldAttributionDto {
  source_key: string;
  source_url: string;
  evidence_ids: string[];
}

export interface ConfidenceValueDto {
  value: string | number | null;
  confidence: number;
  sources: FieldAttributionDto[];
}

export interface UniversityCardDto {
  university_id: string;
  canonical_name: ConfidenceValueDto;
  aliases: string[];
  location: {
    country: string | null;
    city: string | null;
    address?: string | null;
    geo?: Record<string, number> | null;
  };
  contacts: {
    website: string | null;
    emails: string[];
    phones: string[];
  };
  institutional: {
    type: string | null;
    founded_year: number | null;
  };
  programs: Array<Record<string, unknown>>;
  tuition: Array<Record<string, unknown>>;
  ratings: Array<Record<string, unknown>>;
  dormitory: Record<string, unknown>;
  reviews: {
    summary: string | null;
    items: Array<Record<string, unknown>>;
  };
  sources: FieldAttributionDto[];
  version: {
    card_version: number;
    generated_at: string;
  };
  admission: {
    contacts: {
      website: string | null;
      emails: string[];
      phones: string[];
      field_attribution: Record<
        string,
        {
          field_name: string;
          source_key: string | null;
          source_trust_tier: string | null;
          source_urls: string[];
          selected_claim_ids: string[];
          selected_evidence_ids: string[];
          resolution_policy: string;
          resolution_strategy: string | null;
          rationale: string;
        }
      >;
    };
    programs: Array<{
      field_name: string;
      faculty: string | null;
      code: string | null;
      name: string | null;
      budget_places: number | null;
      passing_score: number | null;
      year: number | null;
      confidence: number | null;
      sources: Array<Record<string, unknown>>;
      field_attribution: {
        field_name: string;
        source_key: string | null;
        source_trust_tier: string | null;
        source_urls: string[];
        selected_claim_ids: string[];
        selected_evidence_ids: string[];
        resolution_policy: string;
        resolution_strategy: string | null;
        rationale: string;
      } | null;
    }>;
  };
}

export interface DeliveryProjectionTraceDto {
  university_id: string;
  card_version: number;
  card: UniversityCardDto;
  projection_generated_at: string;
  card_generated_at: string | null;
  normalizer_version: string | null;
}

export interface ResolvedFactTraceDto {
  resolved_fact_id: string;
  university_id: string;
  field_name: string;
  value: unknown;
  value_type: string;
  fact_score: number;
  resolution_policy: string;
  selected_claim_ids: string[];
  selected_evidence_ids: string[];
  card_version: number;
  resolved_at: string;
  metadata: Record<string, unknown>;
}

export interface ClaimTraceDto {
  claim_id: string;
  parsed_document_id: string;
  source_key: string;
  field_name: string;
  value: unknown;
  value_type: string;
  entity_hint: string | null;
  parser_version: string;
  normalizer_version: string | null;
  parser_confidence: number;
  created_at: string;
  metadata: Record<string, unknown>;
}

export interface ClaimEvidenceTraceDto {
  evidence_id: string;
  claim_id: string;
  raw_artifact_id: string;
  fragment_id: string | null;
  source_key: string;
  source_url: string;
  captured_at: string;
  metadata: Record<string, unknown>;
}

export interface ParsedDocumentTraceDto {
  parsed_document_id: string;
  crawl_run_id: string;
  raw_artifact_id: string;
  source_key: string;
  parser_profile: string;
  parser_version: string;
  entity_type: string;
  entity_hint: string | null;
  extracted_fragment_count: number;
  parsed_at: string;
  metadata: Record<string, unknown>;
}

export interface RawArtifactTraceDto {
  raw_artifact_id: string;
  crawl_run_id: string;
  source_key: string;
  source_url: string;
  final_url: string | null;
  http_status: number | null;
  content_type: string;
  content_length: number | null;
  sha256: string;
  storage_bucket: string;
  storage_object_key: string;
  etag: string | null;
  last_modified: string | null;
  fetched_at: string;
  metadata: Record<string, unknown>;
}

export interface UniversityProvenanceDto {
  university_id: string;
  chain: string[];
  delivery_projection: DeliveryProjectionTraceDto;
  resolved_facts: ResolvedFactTraceDto[];
  claims: ClaimTraceDto[];
  claim_evidence: ClaimEvidenceTraceDto[];
  parsed_documents: ParsedDocumentTraceDto[];
  raw_artifacts: RawArtifactTraceDto[];
}
