import type {
  BackendSearchResponse,
  UniversityCardDto,
  UniversityProvenanceDto,
} from "./types";

export interface BackendApiClientOptions {
  baseUrl: string;
  requestTimeoutMs: number;
}

interface RequestOptions {
  signal?: AbortSignal;
}

export class BackendApiError extends Error {
  readonly status: number;
  readonly url: string;
  readonly detail: string | null;

  constructor(params: {
    status: number;
    url: string;
    detail: string | null;
  }) {
    super(params.detail ?? `Backend API request failed with status ${params.status}.`);
    this.name = "BackendApiError";
    this.status = params.status;
    this.url = params.url;
    this.detail = params.detail;
  }
}

export class BackendApiClient {
  private readonly baseUrl: string;
  private readonly requestTimeoutMs: number;

  constructor(options: BackendApiClientOptions) {
    this.baseUrl = normalizeBaseUrl(options.baseUrl);
    this.requestTimeoutMs = options.requestTimeoutMs;
  }

  async searchUniversities(
    params: {
      query?: string;
    } = {},
    options?: RequestOptions,
  ): Promise<BackendSearchResponse> {
    const search = new URLSearchParams();
    if (params.query) {
      search.set("query", params.query);
    }
    const suffix = search.size > 0 ? `?${search.toString()}` : "";
    return this.requestJson<BackendSearchResponse>(`/api/v1/search${suffix}`, options);
  }

  async getUniversityCard(
    universityId: string,
    options?: RequestOptions,
  ): Promise<UniversityCardDto> {
    return this.requestJson<UniversityCardDto>(
      `/api/v1/universities/${encodeURIComponent(universityId)}`,
      options,
    );
  }

  async getUniversityProvenance(
    universityId: string,
    options?: RequestOptions,
  ): Promise<UniversityProvenanceDto> {
    return this.requestJson<UniversityProvenanceDto>(
      `/api/v1/universities/${encodeURIComponent(universityId)}/provenance`,
      options,
    );
  }

  private async requestJson<T>(
    path: string,
    options?: RequestOptions,
  ): Promise<T> {
    const url = joinUrl(this.baseUrl, path);
    const timeoutController = new AbortController();
    const timeoutId = window.setTimeout(
      () => timeoutController.abort(`Request timed out after ${this.requestTimeoutMs}ms.`),
      this.requestTimeoutMs,
    );
    const signal = mergeSignals(options?.signal, timeoutController.signal);

    try {
      const response = await fetch(url, {
        method: "GET",
        headers: {
          Accept: "application/json",
        },
        signal,
      });
      if (!response.ok) {
        throw new BackendApiError({
          status: response.status,
          url,
          detail: await readErrorDetail(response),
        });
      }
      return (await response.json()) as T;
    } finally {
      window.clearTimeout(timeoutId);
    }
  }
}

async function readErrorDetail(response: Response): Promise<string | null> {
  const cloned = response.clone();
  try {
    const payload: unknown = await response.json();
    if (
      typeof payload === "object" &&
      payload !== null &&
      "detail" in payload &&
      typeof payload.detail === "string"
    ) {
      return payload.detail;
    }
    return null;
  } catch {
    const text = await cloned.text().catch(() => "");
    return text || null;
  }
}

function mergeSignals(
  primary?: AbortSignal,
  secondary?: AbortSignal,
): AbortSignal | undefined {
  if (!primary) {
    return secondary;
  }
  if (!secondary) {
    return primary;
  }

  const controller = new AbortController();
  const abort = (reason?: unknown) => controller.abort(reason);

  if (primary.aborted || secondary.aborted) {
    abort(primary.reason ?? secondary.reason);
    return controller.signal;
  }

  primary.addEventListener("abort", () => abort(primary.reason), { once: true });
  secondary.addEventListener("abort", () => abort(secondary.reason), { once: true });
  return controller.signal;
}

function normalizeBaseUrl(baseUrl: string): string {
  if (!baseUrl || baseUrl === "/") {
    return "";
  }
  return baseUrl.endsWith("/") ? baseUrl.slice(0, -1) : baseUrl;
}

function joinUrl(baseUrl: string, path: string): string {
  if (/^https?:\/\//.test(path)) {
    return path;
  }
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  if (!baseUrl) {
    return normalizedPath;
  }
  return `${baseUrl}${normalizedPath}`;
}
