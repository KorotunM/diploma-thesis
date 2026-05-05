import { useUniversityCardLookup } from "../features/university-card";
import { ViewState } from "../shared/ui/view-state";

export function UniversityCardPage() {
  const { activeUniversityId, snapshot, error, loading, refreshing, clear } =
    useUniversityCardLookup();
  const card = snapshot?.card ?? null;

  return (
    <section className="panel panel--card card-panel">
      <div className="card-panel__topbar">
        <button
          className="button button--ghost"
          type="button"
          onClick={goBack}
        >
          ← Назад
        </button>
        <span className={`panel__badge ${refreshing ? "panel__badge--refreshing" : ""}`}>
          {loading
            ? "Загружаем"
            : card
              ? `v${card.version.card_version}`
              : error
                ? "Ошибка"
                : "Не найдена"}
        </span>
      </div>

      {card ? (
        <div className="card-panel__layout">
          <article className="card-panel__hero">
            <div>
              <p className="card-panel__hero-label">Каноническое название</p>
              <h2>{stringValue(card.canonical_name.value) ?? "Вуз без названия"}</h2>
            </div>
            <div className="card-panel__confidence">
              <span>Уверенность</span>
              <strong>{card.canonical_name.confidence.toFixed(2)}</strong>
            </div>
          </article>

          <div className="card-panel__facts">
            <article className="card-panel__fact">
              <span>Сайт</span>
              <strong>{card.contacts.website ?? "не указан"}</strong>
            </article>
            <article className="card-panel__fact">
              <span>Локация</span>
              <strong>{formatLocation(card.location.city, card.location.country)}</strong>
            </article>
            <article className="card-panel__fact">
              <span>Тип учреждения</span>
              <strong>{card.institutional.type ?? "не определён"}</strong>
            </article>
            <article className="card-panel__fact">
              <span>Год основания</span>
              <strong>
                {card.institutional.founded_year !== null
                  ? String(card.institutional.founded_year)
                  : "не определён"}
              </strong>
            </article>
          </div>

          <div className="card-panel__meta-grid">
            <article className="card-panel__meta">
              <h3>Метаданные</h3>
              <div className="key-value">
                <span>university_id</span>
                <strong>{card.university_id}</strong>
              </div>
              <div className="key-value">
                <span>Версия карточки</span>
                <strong>{card.version.card_version}</strong>
              </div>
              <div className="key-value">
                <span>Сгенерировано</span>
                <strong>{formatTimestamp(card.version.generated_at)}</strong>
              </div>
              <div className="key-value">
                <span>Получено</span>
                <strong>{snapshot ? formatTimestamp(snapshot.receivedAt) : "—"}</strong>
              </div>
            </article>

            <article className="card-panel__meta">
              <h3>Источники атрибуции</h3>
              <div className="card-panel__source-list">
                {card.sources.map((source) => (
                  <article
                    key={`${source.source_key}:${source.source_url}`}
                    className="card-panel__source"
                  >
                    <span className="chip">{source.source_key}</span>
                    <strong>{source.source_url}</strong>
                    <small>{source.evidence_ids.length} evidence ID</small>
                  </article>
                ))}
                {card.sources.length === 0 ? (
                  <ViewState
                    kind="empty"
                    title="Атрибуция источников отсутствует"
                    message="Эта карточка пока не ссылается на provenance-источники."
                    compact
                  />
                ) : null}
              </div>
            </article>
          </div>
        </div>
      ) : (
        <div className="card-panel__empty">
          {loading ? (
            <ViewState
              kind="loading"
              title="Загружаем карточку вуза"
              message="Получаем последнюю delivery projection для выбранного вуза."
              detail={activeUniversityId ? `university_id: ${activeUniversityId}` : undefined}
            />
          ) : null}
          {!loading && error ? (
            <ViewState
              kind="error"
              title="Карточка вуза недоступна"
              message={error}
              detail="Вернись в поиск и выбери вуз из списка."
            />
          ) : null}
          {!loading && !error && !activeUniversityId ? (
            <ViewState
              kind="empty"
              title="Вуз не выбран"
              message="Вернись в поиск и нажми «Открыть карточку» у нужного вуза."
            />
          ) : null}
        </div>
      )}
    </section>
  );
}

function goBack(): void {
  window.location.hash = "search";
}

function stringValue(value: string | number | null): string | null {
  if (typeof value === "number") return String(value);
  if (typeof value === "string") return value;
  return null;
}

function formatLocation(city: string | null, country: string | null): string {
  const parts = [city, country].filter((v): v is string => Boolean(v));
  return parts.length > 0 ? parts.join(", ") : "не определено";
}

function formatTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("ru-RU", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}
