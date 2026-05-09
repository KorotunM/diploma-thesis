import { useEffect, useMemo, useState } from "react";

import { useAuth } from "../shared/auth";
import type { FavoriteItemDto, SavedSearchItemDto } from "../shared/backend-api/types";
import { describeRequestError } from "../shared/http";
import { useFrontendRuntime } from "../shared/runtime";
import { ViewState } from "../shared/ui/view-state";

interface ProfilePageProps {
  onShowLogin: () => void;
}

export function ProfilePage({ onShowLogin }: ProfilePageProps) {
  const { backendApi } = useFrontendRuntime();
  const { user } = useAuth();
  const [favorites, setFavorites] = useState<FavoriteItemDto[]>([]);
  const [savedSearches, setSavedSearches] = useState<SavedSearchItemDto[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busyId, setBusyId] = useState<string | null>(null);

  const registeredAt = useMemo(() => {
    if (!user) return null;
    if (!user.created_at) return "Не указана";
    return new Intl.DateTimeFormat("ru-RU", { dateStyle: "medium" }).format(
      new Date(user.created_at),
    );
  }, [user]);

  useEffect(() => {
    if (!user) return;
    setLoading(true);
    setError(null);
    Promise.all([backendApi.getFavorites(), backendApi.getSavedSearches()])
      .then(([favoritesResponse, savedSearchesResponse]) => {
        setFavorites(favoritesResponse.items);
        setSavedSearches(savedSearchesResponse.items);
      })
      .catch((e: unknown) => setError(describeRequestError(e)))
      .finally(() => setLoading(false));
  }, [backendApi, user]);

  if (!user) {
    return (
      <div className="profile-page">
        <ViewState
          kind="empty"
          title="Нужен вход"
          message="Личный кабинет доступен после авторизации."
          actions={<button className="button button--primary" type="button" onClick={onShowLogin}>Войти</button>}
        />
      </div>
    );
  }

  const removeFavorite = async (universityId: string) => {
    setBusyId(universityId);
    try {
      await backendApi.removeFavorite(universityId);
      setFavorites((items) => items.filter((item) => item.university_id !== universityId));
    } finally {
      setBusyId(null);
    }
  };

  const removeSavedSearch = async (savedSearchId: string) => {
    setBusyId(savedSearchId);
    try {
      await backendApi.deleteSavedSearch(savedSearchId);
      setSavedSearches((items) => items.filter((item) => item.saved_search_id !== savedSearchId));
    } finally {
      setBusyId(null);
    }
  };

  return (
    <div className="profile-page">
      <header className="profile-page__header">
        <div>
          <h1 className="profile-page__title">Личный кабинет</h1>
          <p className="profile-page__subtitle">
            Избранные вузы, сравнение и сохранённые поиски в одном месте.
          </p>
        </div>
        <div className="profile-page__identity">
          <span className="profile-page__avatar">{(user.display_name ?? user.email)[0].toUpperCase()}</span>
          <div>
            <strong>{user.display_name ?? user.email}</strong>
            <span>{user.email}</span>
          </div>
        </div>
      </header>

      <section className="profile-grid">
        <div className="profile-panel profile-panel--compact">
          <h2 className="profile-panel__title">Профиль</h2>
          <div className="profile-facts">
            <span>Email</span>
            <strong>{user.email}</strong>
            <span>Имя</span>
            <strong>{user.display_name ?? "Не указано"}</strong>
            <span>Регистрация</span>
            <strong>{registeredAt}</strong>
          </div>
        </div>

        <div className="profile-panel profile-panel--compact">
          <h2 className="profile-panel__title">Сводка</h2>
          <div className="profile-stats">
            <div>
              <strong>{favorites.length}</strong>
              <span>избранных</span>
            </div>
            <div>
              <strong>{savedSearches.length}</strong>
              <span>поисков</span>
            </div>
          </div>
        </div>
      </section>

      {loading && (
        <ViewState kind="loading" title="Загружаем кабинет" message="Получаем избранное и сохранённые поиски." />
      )}
      {error && !loading && (
        <ViewState kind="error" title="Не удалось загрузить профиль" message={error} />
      )}

      {!loading && !error && (
        <section className="profile-layout">
          <div className="profile-panel">
            <div className="profile-panel__header">
              <h2 className="profile-panel__title">Избранные вузы</h2>
              <span>{favorites.length}</span>
            </div>
            {favorites.length === 0 ? (
              <p className="profile-empty">Добавьте вуз в избранное из карточки вуза.</p>
            ) : (
              <div className="profile-list">
                {favorites.map((favorite) => (
                  <article className="profile-uni" key={favorite.university_id}>
                    <div className="profile-uni__logo">
                      {favorite.logo_url ? (
                        <img src={favorite.logo_url} alt="" />
                      ) : (
                        (favorite.canonical_name ?? "В")[0]
                      )}
                    </div>
                    <div className="profile-uni__body">
                      <button
                        className="profile-uni__name"
                        type="button"
                        onClick={() => openUniversityCard(favorite.university_id)}
                      >
                        {favorite.canonical_name ?? favorite.university_id}
                      </button>
                      <span>
                        {[favorite.city, favorite.country_code].filter(Boolean).join(", ") || "Город не указан"}
                      </span>
                      {favorite.website && (
                        <a href={favorite.website} target="_blank" rel="noopener noreferrer">
                          {favorite.website}
                        </a>
                      )}
                    </div>
                    <button
                      className="profile-action"
                      type="button"
                      disabled={busyId === favorite.university_id}
                      onClick={() => removeFavorite(favorite.university_id)}
                    >
                      Удалить
                    </button>
                  </article>
                ))}
              </div>
            )}
          </div>

          <div className="profile-panel">
            <div className="profile-panel__header">
              <h2 className="profile-panel__title">Сохранённые поиски</h2>
              <span>{savedSearches.length}</span>
            </div>
            {savedSearches.length === 0 ? (
              <p className="profile-empty">Сохраните параметры поиска на странице подбора вузов.</p>
            ) : (
              <div className="profile-list">
                {savedSearches.map((search) => (
                  <article className="profile-search" key={search.saved_search_id}>
                    <div>
                      <button
                        className="profile-search__name"
                        type="button"
                        onClick={() => openSavedSearch(search)}
                      >
                        {search.name}
                      </button>
                      <span>{describeSavedSearch(search)}</span>
                    </div>
                    <button
                      className="profile-action"
                      type="button"
                      disabled={busyId === search.saved_search_id}
                      onClick={() => removeSavedSearch(search.saved_search_id)}
                    >
                      Удалить
                    </button>
                  </article>
                ))}
              </div>
            )}
          </div>
        </section>
      )}
    </div>
  );
}

function openUniversityCard(universityId: string): void {
  const url = new URL(window.location.href);
  url.searchParams.set("university_id", universityId);
  window.history.replaceState({}, "", url);
  window.location.hash = "university";
}

function openSavedSearch(search: SavedSearchItemDto): void {
  const url = new URL(window.location.href);
  writeParam(url.searchParams, "query", search.query);
  writeParam(url.searchParams, "city", search.city ?? "");
  writeParam(url.searchParams, "country", search.country ?? "");
  writeParam(url.searchParams, "source_type", search.source_type ?? "");
  if (search.page_size !== 20) url.searchParams.set("page_size", String(search.page_size));
  else url.searchParams.delete("page_size");
  url.searchParams.delete("page");
  window.history.replaceState({}, "", url);
  window.location.hash = "search";
}

function writeParam(params: URLSearchParams, key: string, value: string): void {
  const normalized = value.trim();
  if (normalized) params.set(key, normalized);
  else params.delete(key);
}

function describeSavedSearch(search: SavedSearchItemDto): string {
  const parts = [
    search.query ? `запрос: ${search.query}` : null,
    search.city ? `город: ${search.city}` : null,
    search.country ? `страна: ${search.country}` : null,
    search.source_type ? `источник: ${search.source_type}` : null,
  ].filter(Boolean);
  return parts.length > 0 ? parts.join(" · ") : "Без фильтров";
}
