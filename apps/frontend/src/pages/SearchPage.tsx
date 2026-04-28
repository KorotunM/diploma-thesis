import { formatSearchFilters } from "../features/search/formatSearchFilters";
import { useUniversitySearch } from "../features/search";

const PAGE_SIZE_OPTIONS = [10, 20, 50];
const SOURCE_TYPE_OPTIONS = [
  { value: "", label: "All sources" },
  { value: "official_site", label: "Official sites" },
  { value: "aggregator", label: "Aggregators" },
  { value: "ranking", label: "Rankings" },
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

  return (
    <section className="panel search-panel">
      <div className="search-heading">
        <div>
          <p className="section-kicker">Discovery</p>
          <h2>Search universities via backend query endpoint</h2>
          <p className="section-copy">
            Search page now talks to backend `GET /api/v1/search` and renders live results
            instead of static capability notes.
          </p>
        </div>
        <span className={`live-pill ${refreshing ? "live-pill-refreshing" : ""}`}>
          {loading ? "Searching" : `${snapshot?.total ?? 0} indexed hits`}
        </span>
      </div>

      <div className="search-shell">
        <aside className="search-filter-panel">
          <div className="search-filter-panel-header">
            <div>
              <p className="section-kicker">Filters</p>
              <h3>URL-synced browse state</h3>
            </div>
            <button className="filter-reset-button" type="button" onClick={resetFilters}>
              Reset filters
            </button>
          </div>

          <label className="search-control">
            <span>Name, alias or domain</span>
            <input
              className="search-input"
              type="search"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="example.edu or Example State University"
            />
          </label>

          <div className="search-filter-grid">
            <label className="search-control">
              <span>City</span>
              <input
                className="search-input"
                type="search"
                value={city}
                onChange={(event) => setCity(event.target.value)}
                placeholder="Moscow"
              />
            </label>

            <label className="search-control">
              <span>Country</span>
              <input
                className="search-input"
                type="search"
                value={country}
                onChange={(event) => setCountry(event.target.value)}
                placeholder="RU"
              />
            </label>
          </div>

          <div className="search-filter-grid">
            <label className="search-control">
              <span>Source Type</span>
              <select
                className="search-input search-select"
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

            <label className="search-control">
              <span>Page Size</span>
              <select
                className="search-input search-select"
                value={String(pageSize)}
                onChange={(event) => setPageSize(Number(event.target.value))}
              >
                {PAGE_SIZE_OPTIONS.map((value) => (
                  <option key={value} value={value}>
                    {value} per page
                  </option>
                ))}
              </select>
            </label>
          </div>

          <div className="search-url-state">
            <small>query: {query.trim() || "empty"}</small>
            <small>city: {city.trim() || "any"}</small>
            <small>country: {country.trim().toUpperCase() || "any"}</small>
            <small>source: {sourceType || "any"}</small>
          </div>
        </aside>

        <div className="search-results-panel">
          {error ? <p className="panel-alert">{error}</p> : null}

          <div className="search-toolbar">
            <small>
              backend query: <strong>{snapshot?.requestedQuery || query.trim() || "empty"}</strong>
            </small>
                <small>
                  filters:{" "}
                  <strong>
                    {formatSearchFilters({
                      city: snapshot?.filters.city ?? city,
                      country: snapshot?.filters.country ?? country,
                      sourceType: snapshot?.filters.source_type ?? sourceType,
                    })}
                  </strong>
                </small>
            <small>{snapshot ? formatTimestamp(snapshot.receivedAt) : "waiting for response"}</small>
          </div>

          <div className="search-toolbar search-toolbar-secondary">
            <small>
              page <strong>{snapshot?.page ?? page}</strong> / size{" "}
              <strong>{snapshot?.pageSize ?? pageSize}</strong>
            </small>
            <small>
              total <strong>{snapshot?.total ?? 0}</strong>
            </small>
            <small>
              next page: <strong>{snapshot?.hasMore ? "available" : "end reached"}</strong>
            </small>
          </div>

          <div className="search-results">
            {snapshot?.items.map((item) => (
              <article key={item.university_id} className="search-result-card">
                <div className="search-result-header">
                  <div>
                    <strong>{item.canonical_name}</strong>
                    <p>{item.university_id}</p>
                  </div>
                  <div className="search-result-badges">
                    <span className="chip">{item.city ?? "city unknown"}</span>
                    <span className="chip">{item.country_code ?? "country unknown"}</span>
                  </div>
                </div>
                <dl className="search-result-meta">
                  <div>
                    <dt>Website</dt>
                    <dd>{item.website ?? "not provided"}</dd>
                  </div>
                  <div>
                    <dt>Aliases</dt>
                    <dd>{item.aliases.length > 0 ? item.aliases.join(", ") : "none"}</dd>
                  </div>
                  <div>
                    <dt>Score</dt>
                    <dd>{item.score.toFixed(3)}</dd>
                  </div>
                  <div>
                    <dt>Match signals</dt>
                    <dd>{item.match_signals.join(", ") || "none"}</dd>
                  </div>
                </dl>
              </article>
            ))}

            {!loading && (snapshot?.items.length ?? 0) === 0 ? (
              <p className="empty-state">
                No universities matched this query/filter set. URL state is already synced, so
                you can refine and share this exact search view.
              </p>
            ) : null}
          </div>

          <div className="search-pagination">
            <button
              className="card-action-secondary"
              type="button"
              disabled={loading || page <= 1}
              onClick={() => setPage(page - 1)}
            >
              Previous page
            </button>
            <div className="search-pagination-status">
              <strong>Page {snapshot?.page ?? page}</strong>
              <small>{snapshot?.total ?? 0} total matches</small>
            </div>
            <button
              className="card-action-secondary"
              type="button"
              disabled={loading || !snapshot?.hasMore}
              onClick={() => setPage(page + 1)}
            >
              Next page
            </button>
          </div>
        </div>
      </div>
    </section>
  );
}

function formatTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function formatFilters(options: {
  city: string | null | undefined;
  country: string | null | undefined;
  sourceType: string | null | undefined;
}): string {
  const parts = [
    options.city?.trim() ? `city=${options.city.trim()}` : null,
    options.country?.trim() ? `country=${options.country.trim()}` : null,
    options.sourceType?.trim() ? `source=${options.sourceType.trim()}` : null,
  ].filter((value): value is string => value !== null);

  if (parts.length === 0) {
    return "none";
  }
  return parts.join(" · ");
}
