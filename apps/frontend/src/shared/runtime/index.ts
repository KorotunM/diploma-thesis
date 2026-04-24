import { BackendApiClient } from "../backend-api";
import type { FrontendRuntime } from "./context";
import { FrontendRuntimeProvider, useFrontendRuntime } from "./context";
import {
  type FrontendRuntimeConfig,
  loadFrontendRuntimeConfig,
} from "./config";

export function createFrontendRuntime(
  config: FrontendRuntimeConfig = loadFrontendRuntimeConfig(),
): FrontendRuntime {
  return {
    config,
    backendApi: new BackendApiClient({
      baseUrl: config.backendBaseUrl,
      requestTimeoutMs: config.backendRequestTimeoutMs,
    }),
  };
}

export { FrontendRuntimeProvider, loadFrontendRuntimeConfig, useFrontendRuntime };
export type { FrontendRuntime, FrontendRuntimeConfig };
