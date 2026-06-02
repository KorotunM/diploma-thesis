import type { SearchQueryState, SearchSortBy } from "./models";

const DEFAULT_PAGE = 1;
const DEFAULT_PAGE_SIZE = 20;
const DEFAULT_SORT: SearchSortBy = "rating";

export function readSearchQueryStateFromLocation(): SearchQueryState {
  const params = new URLSearchParams(window.location.search);
  return {
    query: params.get("query")?.trim() ?? "",
    city: params.get("city")?.trim() ?? "",
    region: params.get("region")?.trim() ?? "",
    country: params.get("country")?.trim() ?? "",
    sourceType: params.get("source_type")?.trim() ?? "",
    egeSubjects: params.getAll("ege_subjects").filter(Boolean),
    programCodes: params.getAll("program_codes").filter(Boolean),
    dormitory: readBoolean(params.get("dormitory")),
    militaryDepartment: readBoolean(params.get("military_department")),
    sortBy: readSortBy(params.get("sort_by")),
    page: positiveInt(params.get("page")) ?? DEFAULT_PAGE,
    pageSize: positiveInt(params.get("page_size")) ?? DEFAULT_PAGE_SIZE,
  };
}

export function writeSearchQueryStateToLocation(state: SearchQueryState): void {
  const url = new URL(window.location.href);
  writeParam(url.searchParams, "query", state.query);
  writeParam(url.searchParams, "city", state.city);
  writeParam(url.searchParams, "region", state.region);
  writeParam(url.searchParams, "country", state.country);
  writeParam(url.searchParams, "source_type", state.sourceType);
  url.searchParams.delete("ege_subjects");
  for (const s of state.egeSubjects) url.searchParams.append("ege_subjects", s);
  url.searchParams.delete("program_codes");
  for (const code of state.programCodes) url.searchParams.append("program_codes", code);
  writeBoolean(url.searchParams, "dormitory", state.dormitory);
  writeBoolean(url.searchParams, "military_department", state.militaryDepartment);
  writeSortBy(url.searchParams, state.sortBy);
  writePositiveInt(url.searchParams, "page", state.page, DEFAULT_PAGE);
  writePositiveInt(url.searchParams, "page_size", state.pageSize, DEFAULT_PAGE_SIZE);
  window.history.replaceState({}, "", url);
}

function readSortBy(value: string | null): SearchSortBy {
  if (value === "budget_places" || value === "avg_passing_score" || value === "rating") {
    return value;
  }
  return DEFAULT_SORT;
}

function writeSortBy(searchParams: URLSearchParams, value: SearchSortBy): void {
  if (value === DEFAULT_SORT) {
    searchParams.delete("sort_by");
    return;
  }
  searchParams.set("sort_by", value);
}

function writeParam(searchParams: URLSearchParams, key: string, value: string): void {
  const normalized = value.trim();
  if (normalized) {
    searchParams.set(key, normalized);
    return;
  }
  searchParams.delete(key);
}

function readBoolean(value: string | null): boolean {
  return value === "1" || value === "true";
}

function writeBoolean(searchParams: URLSearchParams, key: string, value: boolean): void {
  if (value) {
    searchParams.set(key, "1");
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
