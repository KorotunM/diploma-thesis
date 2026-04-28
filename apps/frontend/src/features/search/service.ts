import type { FrontendRuntime } from "../../shared/runtime";

import type { SearchQueryState, UniversitySearchSnapshot } from "./models";

export async function searchUniversities(options: {
  runtime: FrontendRuntime;
  state: SearchQueryState;
  signal?: AbortSignal;
}): Promise<UniversitySearchSnapshot> {
  const response = await options.runtime.backendApi.searchUniversities(
    {
      query: options.state.query,
      city: options.state.city,
      country: options.state.country,
      sourceType: options.state.sourceType,
      page: options.state.page,
      pageSize: options.state.pageSize,
    },
    { signal: options.signal },
  );

  return {
    requestedQuery: response.query,
    total: response.total,
    page: response.page,
    pageSize: response.page_size,
    hasMore: response.has_more,
    filters: response.filters,
    items: response.items,
    receivedAt: new Date().toISOString(),
  };
}
