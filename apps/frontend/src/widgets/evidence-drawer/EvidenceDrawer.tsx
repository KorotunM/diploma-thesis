import { useEvidenceDrawer } from "../../features/evidence-drawer";
import { ViewState } from "../../shared/ui/view-state";

export function EvidenceDrawer() {
  const { activeUniversityId, snapshot, error, loading, refreshing } = useEvidenceDrawer();
  const hasFieldAttributions = (snapshot?.fieldAttributions.length ?? 0) > 0;

  return (
    <section className="panel accent-panel evidence-panel">
      <div className="evidence-heading">
        <div>
          <p className="section-kicker">Evidence Drawer</p>
          <h2>Provenance-backed evidence and field attribution</h2>
          <p className="section-copy">
            Drawer now resolves live backend provenance and shows how delivery fields map back to
            resolved facts, evidence ids and raw artifacts.
          </p>
        </div>
        <span className={`live-pill ${refreshing ? "live-pill-refreshing" : ""}`}>
          {loading
            ? "Loading trace"
            : snapshot
              ? `${snapshot.provenance.chain.length} stages`
              : activeUniversityId
                ? "Trace unavailable"
                : "Awaiting selection"}
        </span>
      </div>

      {error && snapshot ? <p className="panel-alert">{error}</p> : null}

      {!activeUniversityId ? (
        <ViewState
          kind="empty"
          title="No university selected yet"
          message="Select a university card first. The drawer follows the same university_id and expands it into provenance details."
        />
      ) : null}

      {activeUniversityId && loading && !snapshot ? (
        <ViewState
          kind="loading"
          title="Loading provenance trace"
          message="Resolving raw artifacts, parsed documents, claims, evidence and resolved facts."
          detail={`university_id: ${activeUniversityId}`}
        />
      ) : null}

      {activeUniversityId && !loading && !snapshot && error ? (
        <ViewState
          kind="error"
          title="Provenance trace unavailable"
          message={error}
          detail="The card selection is preserved, so the trace will appear on the next successful lookup."
        />
      ) : null}

      {snapshot ? (
        <div className="evidence-layout">
          <div className="summary-grid evidence-summary-grid">
            <article className="summary-tile">
              <span>university_id</span>
              <strong>{snapshot.universityId}</strong>
              <small>{formatTimestamp(snapshot.receivedAt)}</small>
            </article>
            <article className="summary-tile">
              <span>evidence ids</span>
              <strong>{snapshot.provenance.claim_evidence.length}</strong>
              <small>{snapshot.provenance.raw_artifacts.length} raw artifacts</small>
            </article>
            <article className="summary-tile">
              <span>claims</span>
              <strong>{snapshot.provenance.claims.length}</strong>
              <small>{snapshot.provenance.parsed_documents.length} parsed documents</small>
            </article>
            <article className="summary-tile">
              <span>resolved facts</span>
              <strong>{snapshot.provenance.resolved_facts.length}</strong>
              <small>card v{snapshot.provenance.delivery_projection.card_version}</small>
            </article>
          </div>

          <div className="evidence-columns">
            <section className="evidence-subpanel">
              <div className="subpanel-heading">
                <h3>Field attribution</h3>
                <small>
                  {snapshot.fieldAttributions.length} resolved fields with source pointers
                </small>
              </div>
              <div className="attribution-list">
                {hasFieldAttributions
                  ? snapshot.fieldAttributions.map((item) => (
                      <article key={item.fieldName} className="attribution-card">
                        <div className="attribution-header">
                          <strong>{item.fieldName}</strong>
                          <span className="chip">{item.confidence.toFixed(2)}</span>
                        </div>
                        <p>{item.sourceKey ?? "source key unavailable"}</p>
                        <div className="attribution-links">
                          {item.sourceUrls.map((sourceUrl) => (
                            <code key={sourceUrl}>{sourceUrl}</code>
                          ))}
                        </div>
                        <small>{item.evidenceIds.length} evidence ids linked</small>
                      </article>
                    ))
                  : null}
                {!hasFieldAttributions ? (
                  <ViewState
                    kind="empty"
                    title="No field attribution rows"
                    message="The provenance response did not include per-field source pointers for this card."
                    compact
                  />
                ) : null}
              </div>
            </section>

            <section className="evidence-subpanel">
              <div className="subpanel-heading">
                <h3>Evidence chain</h3>
                <small>{snapshot.provenance.chain.join(" -> ")}</small>
              </div>
              <div className="evidence-chain-list">
                {snapshot.evidenceChain.map((entry) => (
                  <article key={entry.evidenceId} className="evidence-card">
                    <div className="evidence-card-header">
                      <span className="chip">{entry.sourceKey}</span>
                      <small>{formatTimestamp(entry.capturedAt)}</small>
                    </div>
                    <code>{entry.sourceUrl}</code>
                    <div className="evidence-chip-list">
                      {entry.fieldNames.map((fieldName) => (
                        <span key={fieldName} className="dependency-chip">
                          {fieldName}
                        </span>
                      ))}
                    </div>
                    <div className="evidence-meta-grid">
                      <span>http {entry.httpStatus ?? "?"}</span>
                      <span>{entry.parserVersions.join(", ") || "parser unknown"}</span>
                    </div>
                    <small>{entry.storageObjectKey ?? "storage object not linked"}</small>
                  </article>
                ))}
                {snapshot.evidenceChain.length === 0 ? (
                  <ViewState
                    kind="empty"
                    title="No evidence rows attached"
                    message="The provenance response exists, but this card has no linked claim_evidence rows yet."
                    compact
                  />
                ) : null}
              </div>
            </section>
          </div>
        </div>
      ) : null}
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
