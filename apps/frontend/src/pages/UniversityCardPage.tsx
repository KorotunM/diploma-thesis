import { useUniversityCardLookup } from "../features/university-card";
import { ViewState } from "../shared/ui/view-state";

export function UniversityCardPage() {
  const {
    activeUniversityId,
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
    <section className="panel panel--card card-panel" id="university-card">
      <div className="panel__header">
        <div>
          <p className="panel__kicker">Карточка вуза</p>
          <h2 className="panel__title">Живая delivery projection по выбранному вузу</h2>
          <p className="panel__copy">
            Карточка читает `delivery.university_card`, показывает ключевые поля и не разъезжается
            по ширине, когда рядом появляется длинный список результатов.
          </p>
        </div>
        <span className={`panel__badge ${refreshing ? "panel__badge--refreshing" : ""}`}>
          {loading
            ? "Загружаем карточку"
            : card
              ? `v${card.version.card_version}`
              : error
                ? "Карточка недоступна"
                : activeUniversityId
                  ? "Не найдена"
                  : "Ожидание выбора"}
        </span>
      </div>

      {activeUniversityId ? (
        <div className="card-panel__selection">
          <strong>Выбранный вуз</strong>
          <code>{activeUniversityId}</code>
        </div>
      ) : null}

      <form
        className="card-panel__form"
        onSubmit={(event) => {
          event.preventDefault();
          submit();
        }}
      >
        <label className="field">
          <span className="field__label">University ID</span>
          <input
            className="field__control"
            type="text"
            value={draftUniversityId}
            onChange={(event) => setDraftUniversityId(event.target.value)}
            placeholder="ID подставляется из поиска автоматически или вводится вручную"
          />
        </label>
        <div className="card-panel__actions">
          <button className="button button--primary" type="submit" disabled={!canSubmit}>
            Загрузить карточку
          </button>
          <button className="button button--secondary" type="button" onClick={clear}>
            Очистить
          </button>
        </div>
      </form>

      {validationError ? <p className="panel-alert">{validationError}</p> : null}
      {error ? <p className="panel-alert">{error}</p> : null}

      {card ? (
        <div className="card-panel__layout">
          <article className="card-panel__hero">
            <div>
              <p className="card-panel__hero-label">Каноническое название</p>
              <h3>{stringValue(card.canonical_name.value) ?? "Вуз без названия"}</h3>
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
              <strong>{card.institutional.type ?? "не определен"}</strong>
            </article>
            <article className="card-panel__fact">
              <span>Год основания</span>
              <strong>
                {card.institutional.founded_year !== null
                  ? String(card.institutional.founded_year)
                  : "не определен"}
              </strong>
            </article>
          </div>

          <div className="card-panel__meta-grid">
            <article className="card-panel__meta">
              <h3>Метаданные проекции</h3>
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
                <strong>{snapshot ? formatTimestamp(snapshot.receivedAt) : "не загружено"}</strong>
              </div>
            </article>

            <article className="card-panel__meta">
              <h3>Источники атрибуции</h3>
              <div className="card-panel__source-list">
                {card.sources.map((source) => (
                  <article key={`${source.source_key}:${source.source_url}`} className="card-panel__source">
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
              detail="Выбери вуз из результатов поиска или загрузи другой UUID вручную."
            />
          ) : null}
          {!loading && !error ? (
            <ViewState
              kind="empty"
              title="Вуз еще не выбран"
              message="Открой карточку из результатов поиска или вставь UUID вручную."
              detail="Этот же выбор будет использован и в панели доказательств."
            />
          ) : null}
        </div>
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
    return "не определено";
  }
  return parts.join(", ");
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
