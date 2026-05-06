export interface JsonHttpClientOptions {
  baseUrl: string;
  requestTimeoutMs: number;
  getToken?: () => string | null;
}

export interface JsonHttpRequestOptions {
  signal?: AbortSignal;
}

export class HttpRequestError extends Error {
  readonly status: number;
  readonly url: string;
  readonly detail: string | null;

  constructor(params: { status: number; url: string; detail: string | null }) {
    super(params.detail ?? `HTTP request failed with status ${params.status}.`);
    this.name = "HttpRequestError";
    this.status = params.status;
    this.url = params.url;
    this.detail = params.detail;
  }
}

export class JsonHttpClient {
  private readonly baseUrl: string;
  private readonly requestTimeoutMs: number;
  private readonly getToken: (() => string | null) | undefined;

  constructor(options: JsonHttpClientOptions) {
    this.baseUrl = normalizeBaseUrl(options.baseUrl);
    this.requestTimeoutMs = options.requestTimeoutMs;
    this.getToken = options.getToken;
  }

  private buildHeaders(extra?: Record<string, string>): Record<string, string> {
    const headers: Record<string, string> = { Accept: "application/json", ...extra };
    const token = this.getToken?.();
    if (token) headers["Authorization"] = `Bearer ${token}`;
    return headers;
  }

  async get<T>(path: string, options?: JsonHttpRequestOptions): Promise<T> {
    return this._request<T>("GET", path, undefined, options);
  }

  async post<T>(path: string, body?: unknown, options?: JsonHttpRequestOptions): Promise<T> {
    return this._request<T>("POST", path, body, options);
  }

  async delete<T = void>(path: string, options?: JsonHttpRequestOptions): Promise<T> {
    return this._request<T>("DELETE", path, undefined, options);
  }

  private async _request<T>(
    method: string,
    path: string,
    body: unknown,
    options?: JsonHttpRequestOptions,
  ): Promise<T> {
    const url = joinUrl(this.baseUrl, path);
    const timeoutController = new AbortController();
    const timeoutId = window.setTimeout(
      () => timeoutController.abort(`Request timed out after ${this.requestTimeoutMs}ms.`),
      this.requestTimeoutMs,
    );
    const signal = mergeSignals(options?.signal, timeoutController.signal);

    try {
      const headers = this.buildHeaders(
        body !== undefined ? { "Content-Type": "application/json" } : undefined,
      );
      const response = await fetch(url, {
        method,
        headers,
        body: body !== undefined ? JSON.stringify(body) : undefined,
        signal,
      });

      if (!response.ok) {
        throw new HttpRequestError({
          status: response.status,
          url,
          detail: await readErrorDetail(response),
        });
      }

      if (response.status === 204) return undefined as T;
      const text = await response.text();
      return text ? (JSON.parse(text) as T) : (undefined as T);
    } finally {
      window.clearTimeout(timeoutId);
    }
  }
}

export function describeRequestError(error: unknown): string {
  if (isAbortError(error)) return "Request was aborted.";
  if (error instanceof HttpRequestError) {
    return error.detail ?? `Request failed with status ${error.status}.`;
  }
  if (error instanceof Error) return error.message;
  return "Unknown request error.";
}

export function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
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

function mergeSignals(primary?: AbortSignal, secondary?: AbortSignal): AbortSignal | undefined {
  if (!primary) return secondary;
  if (!secondary) return primary;
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
  if (!baseUrl || baseUrl === "/") return "";
  return baseUrl.endsWith("/") ? baseUrl.slice(0, -1) : baseUrl;
}

function joinUrl(baseUrl: string, path: string): string {
  if (/^https?:\/\//.test(path)) return path;
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  if (!baseUrl) return normalizedPath;
  return `${baseUrl}${normalizedPath}`;
}
