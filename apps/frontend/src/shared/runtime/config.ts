export interface FrontendRuntimeConfig {
  appEnvironment: string;
  backendBaseUrl: string;
  schedulerBaseUrl: string;
  parserBaseUrl: string;
  normalizerBaseUrl: string;
  apiRequestTimeoutMs: number;
  overviewRefreshIntervalMs: number;
}

export function loadFrontendRuntimeConfig(): FrontendRuntimeConfig {
  const runtimeConfig = window.__APP_RUNTIME_CONFIG__ ?? {};
  return {
    appEnvironment: runtimeConfig.appEnvironment ?? import.meta.env.MODE ?? "development",
    backendBaseUrl:
      runtimeConfig.backendBaseUrl ??
      import.meta.env.VITE_BACKEND_BASE_URL ??
      "http://localhost:8004",
    schedulerBaseUrl:
      runtimeConfig.schedulerBaseUrl ??
      import.meta.env.VITE_SCHEDULER_BASE_URL ??
      "http://localhost:8001",
    parserBaseUrl:
      runtimeConfig.parserBaseUrl ??
      import.meta.env.VITE_PARSER_BASE_URL ??
      "http://localhost:8002",
    normalizerBaseUrl:
      runtimeConfig.normalizerBaseUrl ??
      import.meta.env.VITE_NORMALIZER_BASE_URL ??
      "http://localhost:8003",
    apiRequestTimeoutMs: readTimeoutValue(
      runtimeConfig.apiRequestTimeoutMs,
      import.meta.env.VITE_API_REQUEST_TIMEOUT_MS,
    ),
    overviewRefreshIntervalMs: readTimeoutValue(
      runtimeConfig.overviewRefreshIntervalMs,
      import.meta.env.VITE_OVERVIEW_REFRESH_INTERVAL_MS,
    ),
  };
}

function readTimeoutValue(
  runtimeValue: number | string | undefined,
  envValue: string | undefined,
): number {
  const candidate = runtimeValue ?? envValue;
  if (typeof candidate === "number" && Number.isFinite(candidate) && candidate > 0) {
    return candidate;
  }
  if (typeof candidate === "string") {
    const parsed = Number.parseInt(candidate, 10);
    if (Number.isFinite(parsed) && parsed > 0) {
      return parsed;
    }
  }
  return 10_000;
}
