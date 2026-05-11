import type { FrontendRuntime } from "../../shared/runtime";
import type { RankingsSnapshot } from "./models";

export async function fetchRankings(options: {
  runtime: FrontendRuntime;
  page: number;
  pageSize: number;
  signal?: AbortSignal;
}): Promise<RankingsSnapshot> {
  const response = await options.runtime.backendApi.getRankings(
    { page: options.page, pageSize: options.pageSize },
    { signal: options.signal },
  );
  return {
    total: response.total,
    page: response.page,
    pageSize: response.page_size,
    hasMore: response.has_more,
    items: response.items,
  };
}
