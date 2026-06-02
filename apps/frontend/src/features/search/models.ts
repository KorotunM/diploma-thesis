import type {
  BackendSearchFiltersDto,
  BackendSearchItem,
} from "../../shared/backend-api";

export type SearchSortBy = "rating" | "budget_places" | "avg_passing_score";

export interface SearchQueryState {
  query: string;
  city: string;
  region: string;
  country: string;
  sourceType: string;
  egeSubjects: string[];
  programCodes: string[];
  dormitory: boolean;
  militaryDepartment: boolean;
  sortBy: SearchSortBy;
  page: number;
  pageSize: number;
}

export interface UniversitySearchSnapshot {
  requestedQuery: string;
  total: number;
  page: number;
  pageSize: number;
  hasMore: boolean;
  filters: BackendSearchFiltersDto;
  items: BackendSearchItem[];
  receivedAt: string;
}
