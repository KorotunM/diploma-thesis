import { useUniversityCardLookup } from "../features/university-card";

export function UniversityCardPage() {
  const {
    draftUniversityId,
    snapshot,
    error,
    validationError,
    loading,
    refreshing,
    canSubmit,
    setDraftUniversityId,
    submit,
    clear,
  } = useUniversityCardLookup();
  const card = snapshot?.card ?? null;

  return (
    <section className="panel feature-panel card-panel">
      <div className="card-heading">
        <div>
          <p className="section-kicker">Delivery Card</p>
          <h2>University card page on live delivery payload</h2>
          <p className="section-copy">
            Card page now resolves a real `delivery.university_card` projection through backend
            instead of rendering static placeholder fields.
          </p>
        </div>
        <span className={`live-pill ${refreshing ? "live-pill-refreshing" : ""}`}>
          {loading ? "Loading card" : card ? `v${card.version.card_version}` : "No card"}
        </span>
      </div>

      <form
        className="card-lookup-form"
        onSubmit={(event) => {
          event.preventDefault();
          submit();
        }}
      >
        <label className="search-control card-lookup-control">
          <span>University id</span>
          <input
            className="search-input"
            type="text"
            value={draftUniversityId}
            onChange={(event) => setDraftUniversityId(event.target.value)}
            placeholder="00000000-0000-0000-0000-000000000000"
          />
        </label>
        <div className="card-actions">
          <button className="card-action-primary" type="submit" disabled={!canSubmit}>
            Load live card
          </button>
          <button className="card-action-secondary" type="button" onClick={clear}>
            Clear
          </button>
        </div>
      </form>

      {validationError ? <p className="panel-alert">{validationError}</p> : null}
      {error ? <p className="panel-alert">{error}</p> : null}

      {card ? (
        <div className="card-layout">
          <article className="card-hero">
            <div>
              <p className="card-hero-label">Canonical name</p>
              <h3>{stringValue(card.canonical_name.value) ?? "Unnamed university"}</h3>
            </div>
            <div className="card-confidence-block">
              <span>confidence</span>
              <strong>{card.canonical_name.confidence.toFixed(2)}</strong>
            </div>
          </article>

          <div className="card-fact-grid">
            <article className="card-fact">
              <span>website</span>
              <strong>{card.contacts.website ?? "not provided"}</strong>
            </article>
            <article className="card-fact">
              <span>location</span>
              <strong>{formatLocation(card.location.city, card.location.country)}</strong>
            </article>
            <article className="card-fact">
              <span>institution type</span>
              <strong>{card.institutional.type ?? "not resolved"}</strong>
            </article>
            <article className="card-fact">
              <span>founded</span>
              <strong>
                {card.institutional.founded_year !== null
                  ? String(card.institutional.founded_year)
                  : "not resolved"}
              </strong>
            </article>
          </div>

          <div className="card-metadata-grid">
            <article className="card-metadata-block">
              <h3>Projection metadata</h3>
              <div className="stat-row">
                <span>university_id</span>
                <strong>{card.university_id}</strong>
              </div>
              <div className="stat-row">
                <span>card_version</span>
                <strong>{card.version.card_version}</strong>
              </div>
              <div className="stat-row">
                <span>generated_at</span>
                <strong>{formatTimestamp(card.version.generated_at)}</strong>
              </div>
              <div className="stat-row">
                <span>received_at</span>
                <strong>{snapshot ? formatTimestamp(snapshot.receivedAt) : "not loaded"}</strong>
              </div>
            </article>

            <article className="card-metadata-block">
              <h3>Attribution sources</h3>
              <div className="source-list">
                {card.sources.map((source) => (
                  <article key={`${source.source_key}:${source.source_url}`} className="source-item">
                    <span className="chip">{source.source_key}</span>
                    <strong>{source.source_url}</strong>
                    <small>{source.evidence_ids.length} evidence ids</small>
                  </article>
                ))}
                {card.sources.length === 0 ? (
                  <p className="empty-state">No source attribution was attached to this projection.</p>
                ) : null}
              </div>
            </article>
          </div>
        </div>
      ) : (
        <p className="empty-state card-empty-state">
          Enter a live `university_id` from backend delivery projection to render the card payload.
        </p>
      )}
    </section>
  );
}

function stringValue(value: string | number | null): string | null {
  if (typeof value === "number") {
    return String(value);
  }
  if (typeof value === "string") {
    return value;
  }
  return null;
}

function formatLocation(city: string | null, country: string | null): string {
  const parts = [city, country].filter((value): value is string => Boolean(value));
  if (parts.length === 0) {
    return "not resolved";
  }
  return parts.join(", ");
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
