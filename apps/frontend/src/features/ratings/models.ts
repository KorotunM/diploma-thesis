import type { RankingItemDto } from "../../shared/backend-api";

export type { RankingItemDto as RankingItem };

export interface RankingsSnapshot {
  total: number;
  page: number;
  pageSize: number;
  hasMore: boolean;
  items: RankingItemDto[];
}
