import { JsonHttpClient } from "../http";
import type { JsonHttpClientOptions, JsonHttpRequestOptions } from "../http";
import { PlatformServiceClient } from "../platform-api";
import type { HealthResponseDto } from "../platform-api";

import type {
  AiChatRequestDto,
  AiChatResponseDto,
  AuthResponseDto,
  BackendSearchResponse,
  ComparisonResponseDto,
  CurrentUserDto,
  EgeSubjectsResponseDto,
  FavoritesResponseDto,
  LocationSuggestResponseDto,
  RankingsResponseDto,
  SavedSearchCreateDto,
  SavedSearchItemDto,
  SavedSearchesResponseDto,
  UniversityCardDto,
  UniversityProvenanceDto,
} from "./types";

export interface BackendApiClientOptions extends JsonHttpClientOptions {}

export class BackendApiClient {
  private readonly http: JsonHttpClient;
  private readonly platformClient: PlatformServiceClient;

  constructor(options: BackendApiClientOptions) {
    this.http = new JsonHttpClient(options);
    this.platformClient = new PlatformServiceClient(options);
  }

  getHealth(options?: JsonHttpRequestOptions): Promise<HealthResponseDto> {
    return this.platformClient.getHealth(options);
  }

  async getEgeSubjects(options?: JsonHttpRequestOptions): Promise<EgeSubjectsResponseDto> {
    return this.http.get<EgeSubjectsResponseDto>("/api/v1/subjects", options);
  }

  async searchUniversities(
    params: {
      query?: string;
      city?: string;
      region?: string;
      country?: string;
      sourceType?: string;
      egeSubjects?: string[];
      programCodes?: string[];
      dormitory?: boolean;
      militaryDepartment?: boolean;
      sortBy?: string;
      page?: number;
      pageSize?: number;
    } = {},
    options?: JsonHttpRequestOptions,
  ): Promise<BackendSearchResponse> {
    const search = new URLSearchParams();
    if (params.query) search.set("query", params.query);
    if (params.city) search.set("city", params.city);
    if (params.region) search.set("region", params.region);
    if (params.country) search.set("country", params.country);
    if (params.sourceType) search.set("source_type", params.sourceType);
    if (params.egeSubjects && params.egeSubjects.length > 0) {
      for (const s of params.egeSubjects) search.append("ege_subjects", s);
    }
    if (params.programCodes && params.programCodes.length > 0) {
      for (const code of params.programCodes) search.append("program_codes", code);
    }
    if (params.dormitory) search.set("dormitory", "true");
    if (params.militaryDepartment) search.set("military_department", "true");
    if (params.sortBy) search.set("sort_by", params.sortBy);
    if (params.page && params.page > 1) search.set("page", String(params.page));
    if (params.pageSize) search.set("page_size", String(params.pageSize));
    const suffix = search.size > 0 ? `?${search.toString()}` : "";
    return this.http.get<BackendSearchResponse>(`/api/v1/search${suffix}`, options);
  }

  async suggestRegions(
    q: string,
    options?: JsonHttpRequestOptions,
  ): Promise<LocationSuggestResponseDto> {
    const search = new URLSearchParams({ q });
    return this.http.get<LocationSuggestResponseDto>(`/api/v1/regions?${search}`, options);
  }

  async suggestCities(
    q: string,
    options?: JsonHttpRequestOptions,
  ): Promise<LocationSuggestResponseDto> {
    const search = new URLSearchParams({ q });
    return this.http.get<LocationSuggestResponseDto>(`/api/v1/cities?${search}`, options);
  }

  async getCitiesByRegion(
    region: string,
    options?: JsonHttpRequestOptions,
  ): Promise<LocationSuggestResponseDto> {
    const search = new URLSearchParams({ region });
    return this.http.get<LocationSuggestResponseDto>(`/api/v1/cities?${search}`, options);
  }

  async getAllRegions(options?: JsonHttpRequestOptions): Promise<LocationSuggestResponseDto> {
    return this.http.get<LocationSuggestResponseDto>("/api/v1/regions", options);
  }

  async getUniversityCard(
    universityId: string,
    options?: JsonHttpRequestOptions,
  ): Promise<UniversityCardDto> {
    return this.http.get<UniversityCardDto>(
      `/api/v1/universities/${encodeURIComponent(universityId)}`,
      options,
    );
  }

  async getUniversityProvenance(
    universityId: string,
    options?: JsonHttpRequestOptions,
  ): Promise<UniversityProvenanceDto> {
    return this.http.get<UniversityProvenanceDto>(
      `/api/v1/universities/${encodeURIComponent(universityId)}/provenance`,
      options,
    );
  }

  // ── Auth ───────────────────────────────────────────────────────────

  async authRegister(
    body: { email: string; password: string; display_name?: string | null },
    options?: JsonHttpRequestOptions,
  ): Promise<AuthResponseDto> {
    return this.http.post<AuthResponseDto>("/api/v1/auth/register", body, options);
  }

  async authLogin(
    body: { email: string; password: string },
    options?: JsonHttpRequestOptions,
  ): Promise<AuthResponseDto> {
    return this.http.post<AuthResponseDto>("/api/v1/auth/login", body, options);
  }

  async authLogout(options?: JsonHttpRequestOptions): Promise<void> {
    return this.http.post<void>("/api/v1/auth/logout", undefined, options);
  }

  async getMe(options?: JsonHttpRequestOptions): Promise<CurrentUserDto> {
    return this.http.get<CurrentUserDto>("/api/v1/auth/me", options);
  }

  // ── Favorites ──────────────────────────────────────────────────────

  async getFavorites(options?: JsonHttpRequestOptions): Promise<FavoritesResponseDto> {
    return this.http.get<FavoritesResponseDto>("/api/v1/me/favorites", options);
  }

  async addFavorite(universityId: string, options?: JsonHttpRequestOptions): Promise<void> {
    await this.http.post<unknown>(
      `/api/v1/me/favorites/${encodeURIComponent(universityId)}`,
      undefined,
      options,
    );
  }

  async removeFavorite(universityId: string, options?: JsonHttpRequestOptions): Promise<void> {
    await this.http.delete<void>(
      `/api/v1/me/favorites/${encodeURIComponent(universityId)}`,
      options,
    );
  }

  // ── Comparisons ────────────────────────────────────────────────────

  async getComparisons(options?: JsonHttpRequestOptions): Promise<ComparisonResponseDto> {
    return this.http.get<ComparisonResponseDto>("/api/v1/me/comparisons", options);
  }

  async addComparison(universityId: string, options?: JsonHttpRequestOptions): Promise<void> {
    await this.http.post<unknown>(
      `/api/v1/me/comparisons/${encodeURIComponent(universityId)}`,
      undefined,
      options,
    );
  }

  async removeComparison(universityId: string, options?: JsonHttpRequestOptions): Promise<void> {
    await this.http.delete<void>(
      `/api/v1/me/comparisons/${encodeURIComponent(universityId)}`,
      options,
    );
  }

  // --- Saved searches -------------------------------------------------

  async getSavedSearches(options?: JsonHttpRequestOptions): Promise<SavedSearchesResponseDto> {
    return this.http.get<SavedSearchesResponseDto>("/api/v1/me/saved-searches", options);
  }

  async createSavedSearch(
    body: SavedSearchCreateDto,
    options?: JsonHttpRequestOptions,
  ): Promise<SavedSearchItemDto> {
    return this.http.post<SavedSearchItemDto>("/api/v1/me/saved-searches", body, options);
  }

  async deleteSavedSearch(savedSearchId: string, options?: JsonHttpRequestOptions): Promise<void> {
    await this.http.delete<void>(
      `/api/v1/me/saved-searches/${encodeURIComponent(savedSearchId)}`,
      options,
    );
  }

  // --- Rankings --------------------------------------------------------

  async getRankings(
    params: { page?: number; pageSize?: number } = {},
    options?: JsonHttpRequestOptions,
  ): Promise<RankingsResponseDto> {
    const search = new URLSearchParams();
    if (params.page && params.page > 1) search.set("page", String(params.page));
    if (params.pageSize) search.set("page_size", String(params.pageSize));
    const suffix = search.size > 0 ? `?${search.toString()}` : "";
    return this.http.get<RankingsResponseDto>(`/api/v1/rankings${suffix}`, options);
  }

  // --- AI chat ---------------------------------------------------------

  async aiChat(
    body: AiChatRequestDto,
    options?: JsonHttpRequestOptions,
  ): Promise<AiChatResponseDto> {
    return this.http.post<AiChatResponseDto>("/api/v1/ai/chat", body, options);
  }
}
