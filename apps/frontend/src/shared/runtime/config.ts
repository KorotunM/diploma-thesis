export interface FrontendRuntimeConfig {
  appEnvironment: string;
  backendBaseUrl: string;
  backendRequestTimeoutMs: number;
}

export function loadFrontendRuntimeConfig(): FrontendRuntimeConfig {
  const runtimeConfig = window.__APP_RUNTIME_CONFIG__ ?? {};
  return {
    appEnvironment: runtimeConfig.appEnvironment ?? import.meta.env.MODE ?? "development",
    backendBaseUrl:
      runtimeConfig.backendBaseUrl ??
      import.meta.env.VITE_BACKEND_BASE_URL ??
      "http://localhost:8000",
    backendRequestTimeoutMs: readTimeoutValue(
      runtimeConfig.backendRequestTimeoutMs,
      import.meta.env.VITE_BACKEND_REQUEST_TIMEOUT_MS,
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
