import type { FrontendRuntime } from "../../shared/runtime";

import type { UniversitySearchSnapshot } from "./models";

export async function searchUniversities(options: {
  runtime: FrontendRuntime;
  query: string;
  signal?: AbortSignal;
}): Promise<UniversitySearchSnapshot> {
  const response = await options.runtime.backendApi.searchUniversities(
    { query: options.query },
    { signal: options.signal },
  );

  return {
    requestedQuery: response.query,
    total: response.total,
    items: response.items,
    receivedAt: new Date().toISOString(),
  };
}
