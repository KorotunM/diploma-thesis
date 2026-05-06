import { BackendApiClient } from "../backend-api";
import { PlatformServiceClient } from "../platform-api";
import { SchedulerApiClient } from "../scheduler-api";
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
      requestTimeoutMs: config.apiRequestTimeoutMs,
      getToken: () => localStorage.getItem("auth_token"),
    }),
    schedulerApi: new SchedulerApiClient({
      baseUrl: config.schedulerBaseUrl,
      requestTimeoutMs: config.apiRequestTimeoutMs,
    }),
    parserApi: new PlatformServiceClient({
      baseUrl: config.parserBaseUrl,
      requestTimeoutMs: config.apiRequestTimeoutMs,
    }),
    normalizerApi: new PlatformServiceClient({
      baseUrl: config.normalizerBaseUrl,
      requestTimeoutMs: config.apiRequestTimeoutMs,
    }),
  };
}

export { FrontendRuntimeProvider, loadFrontendRuntimeConfig, useFrontendRuntime };
export type { FrontendRuntime, FrontendRuntimeConfig };
