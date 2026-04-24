import { JsonHttpClient } from "../http";
import type { JsonHttpClientOptions, JsonHttpRequestOptions } from "../http";
import { PlatformServiceClient } from "../platform-api";
import type { HealthResponseDto } from "../platform-api";

import type {
  SourceEndpointListResponseDto,
  SourceListResponseDto,
} from "./types";

export interface SchedulerApiClientOptions extends JsonHttpClientOptions {}

export class SchedulerApiClient {
  private readonly http: JsonHttpClient;
  private readonly platformClient: PlatformServiceClient;

  constructor(options: SchedulerApiClientOptions) {
    this.http = new JsonHttpClient(options);
    this.platformClient = new PlatformServiceClient(options);
  }

  getHealth(options?: JsonHttpRequestOptions): Promise<HealthResponseDto> {
    return this.platformClient.getHealth(options);
  }

  listSources(
    params: {
      limit?: number;
      offset?: number;
      includeInactive?: boolean;
    } = {},
    options?: JsonHttpRequestOptions,
  ): Promise<SourceListResponseDto> {
    const search = new URLSearchParams();
    if (params.limit !== undefined) {
      search.set("limit", String(params.limit));
    }
    if (params.offset !== undefined) {
      search.set("offset", String(params.offset));
    }
    if (params.includeInactive !== undefined) {
      search.set("include_inactive", String(params.includeInactive));
    }
    const suffix = search.size > 0 ? `?${search.toString()}` : "";
    return this.http.get<SourceListResponseDto>(`/admin/v1/sources${suffix}`, options);
  }

  listSourceEndpoints(
    sourceKey: string,
    params: {
      limit?: number;
      offset?: number;
    } = {},
    options?: JsonHttpRequestOptions,
  ): Promise<SourceEndpointListResponseDto> {
    const search = new URLSearchParams();
    if (params.limit !== undefined) {
      search.set("limit", String(params.limit));
    }
    if (params.offset !== undefined) {
      search.set("offset", String(params.offset));
    }
    const suffix = search.size > 0 ? `?${search.toString()}` : "";
    return this.http.get<SourceEndpointListResponseDto>(
      `/admin/v1/sources/${encodeURIComponent(sourceKey)}/endpoints${suffix}`,
      options,
    );
  }
}
