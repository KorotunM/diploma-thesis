/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_BACKEND_BASE_URL?: string;
  readonly VITE_SCHEDULER_BASE_URL?: string;
  readonly VITE_PARSER_BASE_URL?: string;
  readonly VITE_NORMALIZER_BASE_URL?: string;
  readonly VITE_API_REQUEST_TIMEOUT_MS?: string;
  readonly VITE_OVERVIEW_REFRESH_INTERVAL_MS?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

interface Window {
  __APP_RUNTIME_CONFIG__?: {
    appEnvironment?: string;
    backendBaseUrl?: string;
    schedulerBaseUrl?: string;
    parserBaseUrl?: string;
    normalizerBaseUrl?: string;
    apiRequestTimeoutMs?: number | string;
    overviewRefreshIntervalMs?: number | string;
  };
}
