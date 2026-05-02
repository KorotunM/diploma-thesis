import { useEvidenceDrawer } from "../../features/evidence-drawer";
import { ViewState } from "../../shared/ui/view-state";

export function EvidenceDrawer() {
  const { activeUniversityId, snapshot, error, loading, refreshing } = useEvidenceDrawer();
  const hasFieldAttributions = (snapshot?.fieldAttributions.length ?? 0) > 0;

  return (
    <section className="panel panel--evidence evidence-panel">
      <div className="panel__header">
        <div>
          <p className="panel__kicker">Доказательства</p>
          <h2 className="panel__title">Provenance, атрибуция полей и цепочка доказательств</h2>
          <p className="panel__copy">
            Отдельный экран для разбора происхождения данных. Здесь видно, из каких raw artifacts,
            claims и evidence ID собирается текущая карточка.
          </p>
        </div>
        <span className={`panel__badge ${refreshing ? "panel__badge--refreshing" : ""}`}>
          {loading
            ? "Загружаем трассировку"
            : snapshot
              ? `${snapshot.provenance.chain.length} этапов`
              : activeUniversityId
                ? "Трассировка недоступна"
                : "Ожидание выбора"}
        </span>
      </div>

      {error && snapshot ? <p className="panel-alert">{error}</p> : null}

      {!activeUniversityId ? (
        <ViewState
          kind="empty"
          title="Вуз еще не выбран"
          message="Сначала выбери карточку вуза. Этот экран использует тот же university_id и раскрывает provenance-детали."
        />
      ) : null}

      {activeUniversityId && loading && !snapshot ? (
        <ViewState
          kind="loading"
          title="Загружаем provenance-трассировку"
          message="Разрешаем raw artifacts, parsed documents, claims, evidence и resolved facts."
          detail={`university_id: ${activeUniversityId}`}
        />
      ) : null}

      {activeUniversityId && !loading && !snapshot && error ? (
        <ViewState
          kind="error"
          title="Provenance-трассировка недоступна"
          message={error}
          detail="Выбранная карточка сохранена, поэтому трассировка появится после следующего успешного запроса."
        />
      ) : null}

      {snapshot ? (
        <div className="evidence-panel__layout">
          <div className="evidence-panel__summary">
            <article className="summary-card">
              <span>university_id</span>
              <strong>{snapshot.universityId}</strong>
              <small>{formatTimestamp(snapshot.receivedAt)}</small>
            </article>
            <article className="summary-card">
              <span>evidence ID</span>
              <strong>{snapshot.provenance.claim_evidence.length}</strong>
              <small>{snapshot.provenance.raw_artifacts.length} raw artifacts</small>
            </article>
            <article className="summary-card">
              <span>claims</span>
              <strong>{snapshot.provenance.claims.length}</strong>
              <small>{snapshot.provenance.parsed_documents.length} parsed documents</small>
            </article>
            <article className="summary-card">
              <span>resolved facts</span>
              <strong>{snapshot.provenance.resolved_facts.length}</strong>
              <small>карточка v{snapshot.provenance.delivery_projection.card_version}</small>
            </article>
          </div>

          <div className="evidence-panel__columns">
            <section className="evidence-panel__section">
              <div className="evidence-panel__section-header">
                <h3>Атрибуция полей</h3>
                <small>
                  {snapshot.fieldAttributions.length} resolved fields с указателями на источник
                </small>
              </div>
              <div className="evidence-panel__attributions">
                {hasFieldAttributions
                  ? snapshot.fieldAttributions.map((item) => (
                      <article key={item.fieldName} className="evidence-panel__attribution-card">
                        <div className="evidence-panel__attribution-header">
                          <strong>{item.fieldName}</strong>
                          <span className="chip">{item.confidence.toFixed(2)}</span>
                        </div>
                        <p>{item.sourceKey ?? "source key недоступен"}</p>
                        <div className="evidence-panel__links">
                          {item.sourceUrls.map((sourceUrl) => (
                            <code key={sourceUrl}>{sourceUrl}</code>
                          ))}
                        </div>
                        <small>{item.evidenceIds.length} evidence ID привязано</small>
                      </article>
                    ))
                  : null}
                {!hasFieldAttributions ? (
                  <ViewState
                    kind="empty"
                    title="Нет строк атрибуции полей"
                    message="Ответ provenance не содержит указателей на источники по полям для этой карточки."
                    compact
                  />
                ) : null}
              </div>
            </section>

            <section className="evidence-panel__section">
              <div className="evidence-panel__section-header">
                <h3>Цепочка доказательств</h3>
                <small>{snapshot.provenance.chain.join(" -> ")}</small>
              </div>
              <div className="evidence-panel__chain">
                {snapshot.evidenceChain.map((entry) => (
                  <article key={entry.evidenceId} className="evidence-panel__chain-card">
                    <div className="evidence-panel__chain-header">
                      <span className="chip">{entry.sourceKey}</span>
                      <small>{formatTimestamp(entry.capturedAt)}</small>
                    </div>
                    <code>{entry.sourceUrl}</code>
                    <div className="evidence-panel__chip-list">
                      {entry.fieldNames.map((fieldName) => (
                        <span key={fieldName} className="dependency-chip">
                          {fieldName}
                        </span>
                      ))}
                    </div>
                    <div className="evidence-panel__meta">
                      <span>http {entry.httpStatus ?? "?"}</span>
                      <span>{entry.parserVersions.join(", ") || "parser неизвестен"}</span>
                    </div>
                    <small>{entry.storageObjectKey ?? "storage object не привязан"}</small>
                  </article>
                ))}
                {snapshot.evidenceChain.length === 0 ? (
                  <ViewState
                    kind="empty"
                    title="Нет связанных доказательств"
                    message="Ответ provenance существует, но для этой карточки пока нет связанных claim_evidence строк."
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
  return new Intl.DateTimeFormat("ru-RU", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}
