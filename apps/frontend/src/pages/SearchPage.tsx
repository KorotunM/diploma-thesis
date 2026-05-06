import { useEffect, useState } from "react";

import { useUniversitySearch } from "../features/search";
import { ViewState } from "../shared/ui/view-state";

const DIRECTIONS = [
  "IT и цифровые технологии",
  "Инженерия",
  "Экономика",
  "Медицина",
  "Управление",
  "Гуманитарные науки",
];

const EGE_SUBJECTS = [
  { id: "math",        label: "Математика" },
  { id: "russian",     label: "Русский" },
  { id: "physics",     label: "Физика" },
  { id: "social",      label: "Обществознание" },
  { id: "history",     label: "История" },
  { id: "biology",     label: "Биология" },
  { id: "informatics", label: "Информатика" },
  { id: "chemistry",   label: "Химия" },
  { id: "literature",  label: "Литература" },
  { id: "geography",   label: "География" },
  { id: "foreign",     label: "Иностранные языки" },
];

type EgeScores = Record<string, string>;

// ── EGE panel ─────────────────────────────────────────────────────────────────

function EgePanel({
  scores,
  checked,
  onToggle,
  onScore,
}: {
  scores: EgeScores;
  checked: Set<string>;
  onToggle: (id: string) => void;
  onScore: (id: string, val: string) => void;
}) {
  return (
    <div className="ege-panel__grid">
        {EGE_SUBJECTS.map((s) => {
          const active = checked.has(s.id);
          return (
            <div
              key={s.id}
              className={`ege-card${active ? " ege-card--active" : ""}`}
              role="button"
              tabIndex={0}
              onClick={() => onToggle(s.id)}
              onKeyDown={(e) => { if (e.key === " " || e.key === "Enter") { e.preventDefault(); onToggle(s.id); } }}
            >
              <div className="ege-card__top">
                <span className={`ege-card__checkbox${active ? " ege-card__checkbox--checked" : ""}`} aria-hidden>
                  {active ? "✓" : ""}
                </span>
                <span className="ege-card__label">{s.label}</span>
              </div>
              <input
                className="ege-card__score"
                type="number"
                min={0}
                max={100}
                placeholder="Баллы"
                value={scores[s.id] ?? ""}
                onClick={(e) => e.stopPropagation()}
                onChange={(e) => { onScore(s.id, e.target.value); if (!checked.has(s.id)) onToggle(s.id); }}
              />
            </div>
          );
        })}
      </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

export function SearchPage() {
  const {
    query,
    setQuery,
    city,
    setCity,
    page,
    setPage,
    resetFilters,
    snapshot,
    error,
    loading,
  } = useUniversitySearch();

  const [localQuery, setLocalQuery] = useState(query);
  const [showEge, setShowEge] = useState(false);
  const [egeChecked, setEgeChecked] = useState<Set<string>>(new Set());
  const [egeScores, setEgeScores] = useState<EgeScores>({});

  useEffect(() => {
    if (!showEge) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") setShowEge(false); };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [showEge]);

  const hasResults = (snapshot?.items.length ?? 0) > 0;
  const hasQueryState = query.trim().length > 0 || city.trim().length > 0;

  const handleSearch = () => setQuery(localQuery);
  const handleKeyDown = (e: React.KeyboardEvent) => { if (e.key === "Enter") handleSearch(); };
  const handleDirectionClick = (direction: string) => { setLocalQuery(direction); setQuery(direction); };

  const handleEgeToggle = (id: string) => {
    setEgeChecked((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  };

  const handleEgeScore = (id: string, val: string) => {
    setEgeScores((prev) => ({ ...prev, [id]: val }));
  };

  return (
    <>
      {/* Hero */}
      <section className="hero">
        <div className="hero__inner">
          <h1 className="hero__heading">Поступи в вуз мечты</h1>
          <p className="hero__sub">
            Найдите лучшие вузы, программы и возможности для вашего будущего
          </p>

          <div className="hero__search">
            <input
              className="hero__search-input"
              type="search"
              value={localQuery}
              onChange={(e) => setLocalQuery(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Введите название вуза, города или направления..."
            />
            <button className="hero__search-btn" type="button" onClick={handleSearch}>
              Подобрать
            </button>
          </div>

          <div className="hero__filters">
            <div className="hero__filter-wrap">
              <span className="hero__filter-label">Направление подготовки</span>
              <select className="hero__filter" defaultValue="">
                <option value="">Любое направление</option>
                {DIRECTIONS.map((d) => (
                  <option key={d} value={d}>{d}</option>
                ))}
              </select>
            </div>
            <div className="hero__filter-wrap">
              <span className="hero__filter-label">Регион</span>
              <input
                className="hero__filter"
                type="text"
                value={city}
                onChange={(e) => setCity(e.target.value)}
                placeholder="Вся Россия"
              />
            </div>
            <div className="hero__filter-wrap">
              <span className="hero__filter-label">Форма обучения</span>
              <select className="hero__filter" defaultValue="full">
                <option value="full">Очная</option>
                <option value="part">Заочная</option>
                <option value="mixed">Очно-заочная</option>
              </select>
            </div>
            <div className="hero__filter-wrap">
              <span className="hero__filter-label">Бюджет/платно</span>
              <select className="hero__filter" defaultValue="">
                <option value="">Любой вариант</option>
                <option value="budget">Бюджет</option>
                <option value="paid">Платно</option>
              </select>
            </div>
            <div className="hero__filter-wrap">
              <span className="hero__filter-label">Баллы ЕГЭ</span>
              <button
                className={`hero__filter hero__filter--ege-btn${showEge ? " hero__filter--ege-btn--active" : ""}`}
                type="button"
                onClick={() => setShowEge((v) => !v)}
              >
                {egeChecked.size > 0 ? `${egeChecked.size} предмет${egeChecked.size === 1 ? "" : egeChecked.size < 5 ? "а" : "ов"}` : "Указать баллы"}
                <span className="hero__filter-ege-arrow">{showEge ? "▲" : "▼"}</span>
              </button>
            </div>
          </div>

          <div className="hero__directions">
            <span className="hero__directions-label">Популярные направления</span>
            {DIRECTIONS.map((d) => (
              <button
                key={d}
                className="hero__direction-chip"
                type="button"
                onClick={() => handleDirectionClick(d)}
              >
                {d}
              </button>
            ))}
          </div>
        </div>
      </section>

      {/* EGE modal */}
      {showEge && (
        <div className="modal-overlay" onClick={() => setShowEge(false)}>
          <div className="modal ege-modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal__header">
              <h2 className="modal__title">
                Баллы ЕГЭ
                {egeChecked.size > 0 && (
                  <span className="ege-panel__badge" style={{ marginLeft: 10 }}>{egeChecked.size} выбрано</span>
                )}
              </h2>
              <button className="modal__close" type="button" onClick={() => setShowEge(false)}>✕</button>
            </div>
            <div className="modal__body ege-modal__body">
              <p className="ege-panel__hint">
                Отметьте предметы и укажите баллы — подберём подходящие программы.
              </p>
              <EgePanel
                scores={egeScores}
                checked={egeChecked}
                onToggle={handleEgeToggle}
                onScore={handleEgeScore}
              />
              <button
                className="modal__submit"
                type="button"
                onClick={() => setShowEge(false)}
              >
                Применить
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Results */}
      <div className="search-page">
        <div className="section-header">
          <h2 className="section-header__title">
            {hasQueryState ? "Результаты поиска" : "Популярные вузы"}
          </h2>
          {snapshot && (
            <span className="section-header__count">{snapshot.total} вузов</span>
          )}
          {hasQueryState && (
            <button
              className="section-header__link"
              type="button"
              onClick={() => { resetFilters(); setLocalQuery(""); }}
            >
              Сбросить фильтры ✕
            </button>
          )}
        </div>

        {error && !loading && (
          <ViewState
            kind="error"
            title="Поиск недоступен"
            message={error}
            detail="Проверьте, что backend сервис запущен."
          />
        )}

        {loading && !snapshot && (
          <ViewState kind="loading" title="Загружаем вузы" message="Ждём ответ от сервера..." />
        )}

        {!loading && snapshot && !hasResults && (
          <ViewState
            kind="empty"
            title="Вузы не найдены"
            message="Попробуйте изменить запрос или сбросить фильтры."
          />
        )}

        {hasResults && (
          <div className="uni-list">
            {snapshot!.items.map((item) => (
              <div
                key={item.university_id}
                className="uni-card"
                onClick={() => openUniversityCard(item.university_id)}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => { if (e.key === "Enter") openUniversityCard(item.university_id); }}
              >
                <div className="uni-card__logo">{item.canonical_name.charAt(0)}</div>

                <div className="uni-card__info">
                  <p className="uni-card__name">{item.canonical_name}</p>
                  <p className="uni-card__fullname">
                    {item.aliases.length > 0 ? item.aliases[0] : item.website ?? "сайт не указан"}
                  </p>
                  <div className="uni-card__tags">
                    <span className="uni-card__tag uni-card__tag--state">Государственный</span>
                    {item.city && (
                      <span className="uni-card__tag uni-card__tag--city">📍 {item.city}</span>
                    )}
                    {item.country_code && item.country_code !== "RU" && (
                      <span className="uni-card__tag uni-card__tag--city">{item.country_code}</span>
                    )}
                  </div>
                </div>

                <div className="uni-card__stats">
                  <div className="uni-card__stat">
                    <span className="uni-card__stat-value uni-card__stat-value--rating">
                      ★ {item.score.toFixed(1)}
                    </span>
                    <span className="uni-card__stat-label">Рейтинг</span>
                  </div>
                  <div className="uni-card__stat">
                    <span className="uni-card__stat-value">—</span>
                    <span className="uni-card__stat-label">Бюджетных мест</span>
                  </div>
                  <div className="uni-card__stat">
                    <span className="uni-card__stat-value">—</span>
                    <span className="uni-card__stat-label">Проходной балл</span>
                  </div>
                </div>

                <div className="uni-card__arrow">›</div>
              </div>
            ))}
          </div>
        )}

        {snapshot && (
          <div className="search-pagination">
            <button
              className="button button--secondary"
              type="button"
              disabled={loading || page <= 1}
              onClick={() => setPage(page - 1)}
            >
              Назад
            </button>
            <div className="search-pagination__status">
              <strong>Страница {snapshot.page}</strong>
              <span>{snapshot.total} совпадений</span>
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
        )}
      </div>
    </>
  );
}

function openUniversityCard(universityId: string): void {
  const url = new URL(window.location.href);
  url.searchParams.set("university_id", universityId);
  window.history.replaceState({}, "", url);
  window.location.hash = "university";
}
