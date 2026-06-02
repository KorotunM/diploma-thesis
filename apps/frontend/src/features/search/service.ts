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
      region: options.state.region,
      country: options.state.country,
      sourceType: options.state.sourceType,
      egeSubjects: options.state.egeSubjects.length > 0 ? options.state.egeSubjects : undefined,
      programCodes: options.state.programCodes.length > 0 ? options.state.programCodes : undefined,
      dormitory: options.state.dormitory,
      militaryDepartment: options.state.militaryDepartment,
      sortBy: options.state.sortBy,
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
