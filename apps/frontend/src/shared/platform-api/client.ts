import { JsonHttpClient } from "../http";
import type { JsonHttpClientOptions, JsonHttpRequestOptions } from "../http";

import type { HealthResponseDto } from "./types";

export interface PlatformServiceClientOptions extends JsonHttpClientOptions {}

export class PlatformServiceClient {
  private readonly http: JsonHttpClient;

  constructor(options: PlatformServiceClientOptions) {
    this.http = new JsonHttpClient(options);
  }

  getHealth(options?: JsonHttpRequestOptions): Promise<HealthResponseDto> {
    return this.http.get<HealthResponseDto>("/healthz", options);
  }
}
