import { useUniversitySearch } from "../features/search";

export function SearchPage() {
  const { query, setQuery, snapshot, error, loading, refreshing } = useUniversitySearch();

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
          {loading ? "Searching" : `${snapshot?.total ?? 0} hits`}
        </span>
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

      {error ? <p className="panel-alert">{error}</p> : null}

      <div className="search-toolbar">
        <small>
          backend query: <strong>{(snapshot?.requestedQuery ?? query.trim()) || "empty"}</strong>
        </small>
        <small>{snapshot ? formatTimestamp(snapshot.receivedAt) : "waiting for response"}</small>
      </div>

      <div className="search-results">
        {snapshot?.items.map((item) => (
          <article key={item.university_id} className="search-result-card">
            <div className="search-result-header">
              <div>
                <strong>{item.canonical_name}</strong>
                <p>{item.university_id}</p>
              </div>
              <span className="chip">{item.city ?? "city unknown"}</span>
            </div>
            <dl className="search-result-meta">
              <div>
                <dt>Website</dt>
                <dd>{item.website ?? "not provided"}</dd>
              </div>
              <div>
                <dt>Search mode</dt>
                <dd>backend projection lookup</dd>
              </div>
            </dl>
          </article>
        ))}

        {!loading && (snapshot?.items.length ?? 0) === 0 ? (
          <p className="empty-state">
            No universities matched this query yet. Narrow results, ranking facets and city
            filters will build on top of this connected backend search layer.
          </p>
        ) : null}
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
