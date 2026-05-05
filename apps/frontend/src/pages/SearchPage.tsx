import { useUniversitySearch } from "../features/search";
import { ViewState } from "../shared/ui/view-state";

const PAGE_SIZE_OPTIONS = [10, 20, 50];
const SOURCE_TYPE_OPTIONS = [
  { value: "", label: "Все источники" },
  { value: "official_site", label: "Официальные сайты" },
  { value: "aggregator", label: "Агрегаторы" },
  { value: "ranking", label: "Рейтинги" },
];

export function SearchPage() {
  const {
    query,
    setQuery,
    city,
    setCity,
    country,
    setCountry,
    sourceType,
    setSourceType,
    page,
    setPage,
    pageSize,
    setPageSize,
    resetFilters,
    snapshot,
    error,
    loading,
    refreshing,
  } = useUniversitySearch();
  const hasQueryState =
    query.trim().length > 0 ||
    city.trim().length > 0 ||
    country.trim().length > 0 ||
    sourceType.trim().length > 0;
  const hasResults = (snapshot?.items.length ?? 0) > 0;

  return (
    <section className="panel panel--search search-panel">
      <div className="search-panel__layout">
        <aside className="search-panel__filters">
          <div className="search-panel__filters-header">
            <span className="panel__kicker">Фильтры</span>
            <button className="button button--ghost" type="button" onClick={resetFilters}>
              Сбросить
            </button>
          </div>

          <label className="field">
            <span className="field__label">Название, алиас или домен</span>
            <input
              className="field__control"
              type="search"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="kubsu.ru или КубГУ"
            />
          </label>

          <div className="search-panel__filter-grid">
            <label className="field">
              <span className="field__label">Город</span>
              <input
                className="field__control"
                type="search"
                value={city}
                onChange={(event) => setCity(event.target.value)}
                placeholder="Краснодар"
              />
            </label>

            <label className="field">
              <span className="field__label">Страна</span>
              <input
                className="field__control"
                type="search"
                value={country}
                onChange={(event) => setCountry(event.target.value)}
                placeholder="RU"
              />
            </label>
          </div>

          <div className="search-panel__filter-grid">
            <label className="field">
              <span className="field__label">Тип источника</span>
              <select
                className="field__control field__control--select"
                value={sourceType}
                onChange={(event) => setSourceType(event.target.value)}
              >
                {SOURCE_TYPE_OPTIONS.map((option) => (
                  <option key={option.value || "all"} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>

            <label className="field">
              <span className="field__label">Размер страницы</span>
              <select
                className="field__control field__control--select"
                value={String(pageSize)}
                onChange={(event) => setPageSize(Number(event.target.value))}
              >
                {PAGE_SIZE_OPTIONS.map((value) => (
                  <option key={value} value={value}>
                    {value} на странице
                  </option>
                ))}
              </select>
            </label>
          </div>

          <div className="search-panel__state">
            <small>Запрос: {query.trim() || "режим просмотра"}</small>
            <small>Город: {city.trim() || "любой"}</small>
            <small>Страна: {country.trim().toUpperCase() || "любая"}</small>
            <small>Источник: {sourceType || "все источники"}</small>
          </div>
        </aside>

        <div className="search-panel__results">
          {error && snapshot ? <p className="panel-alert">{error}</p> : null}

          <div className="search-panel__toolbar">
            <small>
              Всего: <strong>{snapshot?.total ?? 0}</strong>
            </small>
            <small>{snapshot ? formatTimestamp(snapshot.receivedAt) : ""}</small>
          </div>

          <div className="search-panel__cards">
            {loading && !snapshot ? (
              <ViewState
                kind="loading"
                title="Загружаем результаты поиска"
                message="Интерфейс ожидает первый ответ от backend search service."
                detail="Запрос и фильтры уже синхронизированы с текущим URL."
              />
            ) : null}
            {!loading && !snapshot && error ? (
              <ViewState
                kind="error"
                title="Результаты поиска недоступны"
                message={error}
                detail="Повтори запрос после того, как backend search и delivery снова станут доступны."
              />
            ) : null}
            {hasResults
              ? snapshot?.items.map((item) => (
                  <article
                    key={item.university_id}
                    className="search-panel__card"
                  >
                    <div className="search-panel__card-header">
                      <div>
                        <strong>{item.canonical_name}</strong>
                        <p>{item.university_id}</p>
                      </div>
                      <div className="search-panel__chips">
                        <span className="chip">{item.city ?? "город неизвестен"}</span>
                        <span className="chip">{item.country_code ?? "страна неизвестна"}</span>
                      </div>
                    </div>
                    <dl className="search-panel__meta">
                      <div>
                        <dt>Сайт</dt>
                        <dd>{item.website ?? "не указан"}</dd>
                      </div>
                      <div>
                        <dt>Алиасы</dt>
                        <dd>{item.aliases.length > 0 ? item.aliases.join(", ") : "нет"}</dd>
                      </div>
                      <div>
                        <dt>Оценка</dt>
                        <dd>{item.score.toFixed(3)}</dd>
                      </div>
                      <div>
                        <dt>Сигналы совпадения</dt>
                        <dd>{item.match_signals.join(", ") || "нет"}</dd>
                      </div>
                    </dl>
                    <div className="search-panel__actions">
                      <button
                        className="button button--primary"
                        type="button"
                        onClick={() => openUniversityCard(item.university_id)}
                      >
                        Открыть карточку
                      </button>
                    </div>
                  </article>
                ))
              : null}
            {!loading && snapshot && !hasResults && !hasQueryState ? (
              <ViewState
                kind="empty"
                title="Поиск готов"
                message="Пока нет активного запроса или фильтров."
                detail="Начни с названия вуза, домена, города, страны или типа источника."
              />
            ) : null}
            {!loading && snapshot && !hasResults && hasQueryState ? (
              <ViewState
                kind="empty"
                title="Совпадения не найдены"
                message="Текущий запрос и фильтры не вернули ни одного документа."
                detail="Расшири условия поиска через поля и фильтры выше."
              />
            ) : null}
          </div>

          {snapshot ? (
            <div className="search-panel__pagination">
              <button
                className="button button--secondary"
                type="button"
                disabled={loading || page <= 1}
                onClick={() => setPage(page - 1)}
              >
                Назад
              </button>
              <div className="search-panel__pagination-status">
                <strong>Страница {snapshot.page}</strong>
                <small>{snapshot.total} совпадений всего</small>
              </div>
              <button
                className="button button--secondary"
                type="button"
                disabled={loading || !snapshot.hasMore}
                onClick={() => setPage(page + 1)}
              >
                Далее
              </button>
            </div>
          ) : null}
        </div>
      </div>
    </section>
  );
}

function openUniversityCard(universityId: string): void {
  const url = new URL(window.location.href);
  url.searchParams.set("university_id", universityId);
  window.history.replaceState({}, "", url);
  window.location.hash = "university";
}

function formatTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("ru-RU", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}
