import type {
  BackendSearchFiltersDto,
  BackendSearchItem,
} from "../../shared/backend-api";

export interface SearchQueryState {
  query: string;
  city: string;
  country: string;
  sourceType: string;
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
