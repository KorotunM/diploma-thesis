import { JsonHttpClient } from "../http";
import type { JsonHttpClientOptions, JsonHttpRequestOptions } from "../http";
import { PlatformServiceClient } from "../platform-api";
import type { HealthResponseDto } from "../platform-api";

import type {
  BackendSearchResponse,
  UniversityCardDto,
  UniversityProvenanceDto,
} from "./types";

export interface BackendApiClientOptions extends JsonHttpClientOptions {}

export class BackendApiClient {
  private readonly http: JsonHttpClient;
  private readonly platformClient: PlatformServiceClient;

  constructor(options: BackendApiClientOptions) {
    this.http = new JsonHttpClient(options);
    this.platformClient = new PlatformServiceClient(options);
  }

  getHealth(options?: JsonHttpRequestOptions): Promise<HealthResponseDto> {
    return this.platformClient.getHealth(options);
  }

  async searchUniversities(
    params: {
      query?: string;
    } = {},
    options?: JsonHttpRequestOptions,
  ): Promise<BackendSearchResponse> {
    const search = new URLSearchParams();
    if (params.query) {
      search.set("query", params.query);
    }
    const suffix = search.size > 0 ? `?${search.toString()}` : "";
    return this.http.get<BackendSearchResponse>(`/api/v1/search${suffix}`, options);
  }

  async getUniversityCard(
    universityId: string,
    options?: JsonHttpRequestOptions,
  ): Promise<UniversityCardDto> {
    return this.http.get<UniversityCardDto>(
      `/api/v1/universities/${encodeURIComponent(universityId)}`,
      options,
    );
  }

  async getUniversityProvenance(
    universityId: string,
    options?: JsonHttpRequestOptions,
  ): Promise<UniversityProvenanceDto> {
    return this.http.get<UniversityProvenanceDto>(
      `/api/v1/universities/${encodeURIComponent(universityId)}/provenance`,
      options,
    );
  }
}
