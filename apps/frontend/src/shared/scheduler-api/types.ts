export interface CrawlPolicyDto {
  schedule_enabled: boolean;
  interval_seconds: number | null;
  timeout_seconds: number;
  max_retries: number;
  retry_backoff_seconds: number;
  render_mode: "http" | "browser" | "auto";
  respect_robots_txt: boolean;
  allowed_content_types: string[];
  request_headers: Record<string, string>;
}

export interface SourceResponseDto {
  source_id: string;
  source_key: string;
  source_type: string;
  trust_tier: string;
  is_active: boolean;
  metadata: Record<string, unknown>;
}

export interface SourceEndpointResponseDto {
  endpoint_id: string;
  source_id: string;
  source_key: string;
  endpoint_url: string;
  parser_profile: string;
  crawl_policy: CrawlPolicyDto;
}

export interface SourceListResponseDto {
  total: number;
  limit: number;
  offset: number;
  items: SourceResponseDto[];
}

export interface SourceEndpointListResponseDto {
  total: number;
  limit: number;
  offset: number;
  items: SourceEndpointResponseDto[];
}
