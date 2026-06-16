import { useRatings } from "../features/ratings";
import type { RankingItem } from "../features/ratings";
import { ViewState } from "../shared/ui/view-state";

function categoryColor(cat: string): string {
  if (cat === "А+") return "rank-row__category--aplus";
  if (cat === "А")  return "rank-row__category--a";
  if (cat === "А-") return "rank-row__category--aminus";
  if (cat === "B+") return "rank-row__category--bplus";
  if (cat === "B")  return "rank-row__category--b";
  return "rank-row__category--c";
}

function LogoCell({ name, logoUrl }: { name: string; logoUrl: string | null }) {
  if (logoUrl) {
    return (
      <img
        className="rank-row__logo"
        src={logoUrl}
        alt={name}
        onError={(e) => {
          const img = e.currentTarget as HTMLImageElement;
          img.style.display = "none";
          const fallback = img.nextElementSibling as HTMLElement | null;
          if (fallback) fallback.style.display = "flex";
        }}
      />
    );
  }
  return (
    <div className="rank-row__logo rank-row__logo--fallback">
      {name.charAt(0).toUpperCase()}
    </div>
  );
}

function TrendBadge({ trend, delta }: { trend: string; delta: number }) {
  if (trend === "neutral" || delta === 0) return null;
  const up = trend === "up";
  return (
    <span className={`rank-row__trend rank-row__trend--${up ? "up" : "down"}`}>
      {up ? "▲" : "▼"}{delta}
    </span>
  );
}

function RankRow({ item, onClick }: { item: RankingItem; onClick: () => void }) {
  return (
    <div
      className="rank-row"
      role="button"
      tabIndex={0}
      onClick={onClick}
      onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") onClick(); }}
    >
      <div className="rank-row__pos-wrap">
        <span className="rank-row__pos">#{item.rank}</span>
        <TrendBadge trend={item.trend} delta={item.trend_delta} />
      </div>
      <LogoCell name={item.canonical_name} logoUrl={item.logo_url} />
      {item.logo_url && (
        <div className="rank-row__logo rank-row__logo--fallback" style={{ display: "none" }}>
          {item.canonical_name.charAt(0).toUpperCase()}
        </div>
      )}
      <div className="rank-row__info">
        <p className="rank-row__name">{item.canonical_name}</p>
        {(item.city || item.region) && (
          <p className="rank-row__location">{item.city ?? item.region}</p>
        )}
      </div>
      <div className="rank-row__right">
        <span className="rank-row__score">{item.composite_score.toFixed(2)}</span>
        <span className={`rank-row__category ${categoryColor(item.category)}`}>
          {item.category}
        </span>
      </div>
    </div>
  );
}

function openUniversityCard(universityId: string): void {
  const url = new URL(window.location.href);
  url.searchParams.set("university_id", universityId);
  window.history.replaceState({}, "", url);
  window.location.hash = "university";
}

export function RatingsPage() {
  const { snapshot, loading, error, page, setPage } = useRatings();
  const totalPages = snapshot ? Math.ceil(snapshot.total / snapshot.pageSize) : 0;
  const updatedDate = formatRatingDate(snapshot?.updatedAt);
  const sourceLabel = snapshot?.sourceLabel ?? "Рейтинги из карточек вузов";

  return (
    <>
      {/* Hero */}
      <section className="hero ratings-hero">
        <div className="hero__inner ratings-hero__inner">
          <p className="ratings-hero__eyebrow">Глобальный сводный</p>
          <h1 className="ratings-hero__title">РЕЙТИНГ ВУЗОВ РОССИИ 2026</h1>
          <div className="ratings-hero__meta">
            <span className="ratings-hero__meta-item">
              <span className="ratings-hero__meta-icon">📅</span>
              {updatedDate}
            </span>
            <span className="ratings-hero__meta-item">
              <span className="ratings-hero__meta-icon">◎</span>
              {sourceLabel}
            </span>
          </div>
        </div>
      </section>

      {/* List */}
      <div className="ratings-page">
        <div className="section-header">
          <h2 className="section-header__title">Все вузы</h2>
          {snapshot && (
            <span className="section-header__count">{snapshot.total} вузов</span>
          )}
        </div>

        {loading && (
          <ViewState kind="loading" title="Загружаем рейтинг..." message="Пожалуйста, подождите" />
        )}

        {error && !loading && (
          <ViewState kind="error" title="Рейтинг недоступен" message={error} />
        )}

        {!loading && !error && snapshot && snapshot.items.length === 0 && (
          <ViewState
            kind="empty"
            title="Нет данных"
            message="Рейтинговые данные ещё не загружены. Запустите пайплайн."
          />
        )}

        {!loading && snapshot && snapshot.items.length > 0 && (
          <>
            <div className="rank-list">
              {snapshot.items.map((item) => (
                <RankRow
                  key={item.university_id}
                  item={item}
                  onClick={() => openUniversityCard(item.university_id)}
                />
              ))}
            </div>

            <div className="search-pagination">
              <button
                className="button button--secondary"
                type="button"
                disabled={loading || page <= 1}
                onClick={() => setPage((p) => p - 1)}
              >
                Назад
              </button>
              <div className="search-pagination__status">
                <strong>Страница {page} из {totalPages}</strong>
                <span>{snapshot.total} вузов</span>
              </div>
              <button
                className="button button--secondary"
                type="button"
                disabled={loading || !snapshot.hasMore}
                onClick={() => setPage((p) => p + 1)}
              >
                Далее
              </button>
            </div>
          </>
        )}
      </div>
    </>
  );
}

function formatRatingDate(value: string | null | undefined): string {
  if (!value) return "16 июня 2026";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "16 июня 2026";
  return new Intl.DateTimeFormat("ru-RU", {
    day: "numeric",
    month: "long",
    year: "numeric",
  }).format(date);
}
