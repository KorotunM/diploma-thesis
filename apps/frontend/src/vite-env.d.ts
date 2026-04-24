/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_BACKEND_BASE_URL?: string;
  readonly VITE_BACKEND_REQUEST_TIMEOUT_MS?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

interface Window {
  __APP_RUNTIME_CONFIG__?: {
    appEnvironment?: string;
    backendBaseUrl?: string;
    backendRequestTimeoutMs?: number | string;
  };
}
