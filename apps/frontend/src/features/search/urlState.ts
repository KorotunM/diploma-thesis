import type { SearchQueryState } from "./models";

const DEFAULT_PAGE = 1;
const DEFAULT_PAGE_SIZE = 20;

export function readSearchQueryStateFromLocation(): SearchQueryState {
  const params = new URLSearchParams(window.location.search);
  return {
    query: params.get("query")?.trim() ?? "",
    city: params.get("city")?.trim() ?? "",
    country: params.get("country")?.trim() ?? "",
    sourceType: params.get("source_type")?.trim() ?? "",
    page: positiveInt(params.get("page")) ?? DEFAULT_PAGE,
    pageSize: positiveInt(params.get("page_size")) ?? DEFAULT_PAGE_SIZE,
  };
}

export function writeSearchQueryStateToLocation(state: SearchQueryState): void {
  const url = new URL(window.location.href);
  writeParam(url.searchParams, "query", state.query);
  writeParam(url.searchParams, "city", state.city);
  writeParam(url.searchParams, "country", state.country);
  writeParam(url.searchParams, "source_type", state.sourceType);
  writePositiveInt(url.searchParams, "page", state.page, DEFAULT_PAGE);
  writePositiveInt(url.searchParams, "page_size", state.pageSize, DEFAULT_PAGE_SIZE);
  window.history.replaceState({}, "", url);
}

function writeParam(searchParams: URLSearchParams, key: string, value: string): void {
  const normalized = value.trim();
  if (normalized) {
    searchParams.set(key, normalized);
    return;
  }
  searchParams.delete(key);
}

function writePositiveInt(
  searchParams: URLSearchParams,
  key: string,
  value: number,
  defaultValue: number,
): void {
  if (value > 0 && value !== defaultValue) {
    searchParams.set(key, String(value));
    return;
  }
  searchParams.delete(key);
}

function positiveInt(value: string | null): number | null {
  if (value === null) {
    return null;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return null;
  }
  return parsed;
}
