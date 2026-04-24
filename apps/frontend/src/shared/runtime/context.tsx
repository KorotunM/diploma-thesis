import { createContext, useContext } from "react";
import type { ReactNode } from "react";

import type { BackendApiClient } from "../backend-api";
import type { FrontendRuntimeConfig } from "./config";

export interface FrontendRuntime {
  config: FrontendRuntimeConfig;
  backendApi: BackendApiClient;
}

const FrontendRuntimeContext = createContext<FrontendRuntime | null>(null);

export function FrontendRuntimeProvider(props: {
  value: FrontendRuntime;
  children: ReactNode;
}) {
  return (
    <FrontendRuntimeContext.Provider value={props.value}>
      {props.children}
    </FrontendRuntimeContext.Provider>
  );
}

export function useFrontendRuntime(): FrontendRuntime {
  const runtime = useContext(FrontendRuntimeContext);
  if (runtime === null) {
    throw new Error("useFrontendRuntime must be used within FrontendRuntimeProvider.");
  }
  return runtime;
}
