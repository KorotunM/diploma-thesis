import { formatSearchFilters } from "../features/search/formatSearchFilters";
import { useUniversitySearch } from "../features/search";
import { ViewState } from "../shared/ui/view-state";

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
  const hasQueryState =
    query.trim().length > 0 ||
    city.trim().length > 0 ||
    country.trim().length > 0 ||
    sourceType.trim().length > 0;
  const hasResults = (snapshot?.items.length ?? 0) > 0;

  return (
    <section className="panel search-panel">
      <div className="search-heading">
        <div>
          <p className="section-kicker">Discovery</p>
          <h2>Search universities via backend query endpoint</h2>
          <p className="section-copy">
            Search page talks to backend `GET /api/v1/search`, keeps query state in the URL and
            renders live matches from the delivery search projection.
          </p>
        </div>
        <span className={`live-pill ${refreshing ? "live-pill-refreshing" : ""}`}>
          {loading && !snapshot
            ? "Searching"
            : snapshot
              ? `${snapshot.total} indexed hits`
              : error
                ? "Search unavailable"
                : "Ready to browse"}
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
            <small>query: {query.trim() || "browse mode"}</small>
            <small>city: {city.trim() || "any city"}</small>
            <small>country: {country.trim().toUpperCase() || "any country"}</small>
            <small>source: {sourceType || "all sources"}</small>
          </div>
        </aside>

        <div className="search-results-panel">
          {error && snapshot ? <p className="panel-alert">{error}</p> : null}

          <div className="search-toolbar">
            <small>
              backend query: <strong>{snapshot?.requestedQuery || query.trim() || "browse mode"}</strong>
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
            {loading && !snapshot ? (
              <ViewState
                kind="loading"
                title="Loading search results"
                message="The frontend is waiting for the first response from the backend search service."
                detail="Query and filter state are already synced to the current URL."
              />
            ) : null}
            {!loading && !snapshot && error ? (
              <ViewState
                kind="error"
                title="Search results are unavailable"
                message={error}
                detail="Retry after backend search and delivery services become reachable."
              />
            ) : null}
            {hasResults
              ? snapshot?.items.map((item) => (
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
                ))
              : null}
            {!loading && snapshot && !hasResults && !hasQueryState ? (
              <ViewState
                kind="empty"
                title="Search is ready"
                message="No query or filters are active yet, so the page is waiting for a browse request."
                detail="Start with a university name, domain, city, country or source type."
              />
            ) : null}
            {!loading && snapshot && !hasResults && hasQueryState ? (
              <ViewState
                kind="empty"
                title="No universities matched"
                message="The current query and filter set returned zero search documents."
                detail="Adjust the URL-synced inputs above to broaden the match window."
              />
            ) : null}
          </div>

          {snapshot ? (
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
                <strong>Page {snapshot.page}</strong>
                <small>{snapshot.total} total matches</small>
              </div>
              <button
                className="card-action-secondary"
                type="button"
                disabled={loading || !snapshot.hasMore}
                onClick={() => setPage(page + 1)}
              >
                Next page
              </button>
            </div>
          ) : null}
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
