import type { BackendSearchItem } from "../../shared/backend-api";

export interface UniversitySearchSnapshot {
  requestedQuery: string;
  total: number;
  items: BackendSearchItem[];
  receivedAt: string;
}
