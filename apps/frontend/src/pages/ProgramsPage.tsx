import { useEffect, useState } from "react";

import type { BackendSearchResponse } from "../shared/backend-api";
import { useFrontendRuntime } from "../shared/runtime";
import { DIRECTIONS, EGE_FILTER_SUBJECTS, type Direction } from "./programs-data";

// ── Direction list card ────────────────────────────────────────────────────────

function DirectionCard({
  direction,
  onClick,
}: {
  direction: Direction;
  onClick: () => void;
}) {
  return (
    <article
      className="prog-card"
      role="button"
      tabIndex={0}
      onClick={onClick}
      onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onClick(); } }}
    >
      <div className="prog-card__top">
        <span className="prog-card__code">{direction.code}</span>
        <span className="prog-card__level">{direction.level}</span>
      </div>
      <h3 className="prog-card__name">{direction.name}</h3>
      <p className="prog-card__group">{direction.ugnsGroup}</p>
      <div className="prog-card__subjects">
        {direction.egeSubjects[0]?.map((subj) => (
          <span key={subj} className="prog-card__subject-chip">{subj}</span>
        ))}
        {(direction.egeSubjects.length > 1) && (
          <span className="prog-card__subject-or">или другое сочетание</span>
        )}
      </div>
      <div className="prog-card__professions">
        {direction.professions.slice(0, 3).join(" · ")}
      </div>
      <div className="prog-card__arrow">→</div>
    </article>
  );
}

// ── Direction detail ───────────────────────────────────────────────────────────

function DirectionDetail({
  direction,
  unis,
  loadingUnis,
  onBack,
}: {
  direction: Direction;
  unis: BackendSearchResponse | null;
  loadingUnis: boolean;
  onBack: () => void;
}) {
  return (
    <div className="prog-detail">
      <button className="prog-detail__back" type="button" onClick={onBack}>
        ← Назад к программам
      </button>

      <div
        className="prog-detail__hero"
        style={{ background: `linear-gradient(135deg, ${direction.ugnsGroupColor}cc 0%, ${direction.ugnsGroupColor}88 100%)` }}
      >
        <div className="prog-detail__hero-inner">
          <span className="prog-detail__code">{direction.code}</span>
          <h1 className="prog-detail__name">{direction.name}</h1>
          <div className="prog-detail__meta">
            <span className="prog-detail__level">{direction.level}</span>
            <span className="prog-detail__group">{direction.ugnsGroup}</span>
          </div>
        </div>
      </div>

      <div className="prog-detail__body">
        <div className="prog-detail__main">
          <section className="prog-detail__section">
            <h2 className="prog-detail__section-title">О направлении</h2>
            <p className="prog-detail__description">{direction.description}</p>
          </section>

          <section className="prog-detail__section">
            <h2 className="prog-detail__section-title">Требуемые предметы ЕГЭ</h2>
            <div className="prog-detail__ege-variants">
              {direction.egeSubjects.map((variant, i) => (
                <div key={i} className="prog-detail__ege-variant">
                  {i > 0 && <span className="prog-detail__ege-or">или</span>}
                  <div className="prog-detail__ege-chips">
                    {variant.map((subj) => (
                      <span key={subj} className="prog-detail__ege-chip">{subj}</span>
                    ))}
                    <span className="prog-detail__ege-chip prog-detail__ege-chip--always">+ Русский язык</span>
                    <span className="prog-detail__ege-chip prog-detail__ege-chip--always">+ Математика (профиль)</span>
                  </div>
                </div>
              ))}
            </div>
          </section>

          <section className="prog-detail__section">
            <h2 className="prog-detail__section-title">Кем можно работать</h2>
            <ul className="prog-detail__professions">
              {direction.professions.map((prof) => (
                <li key={prof} className="prog-detail__profession">
                  <span className="prog-detail__profession-dot" />
                  {prof}
                </li>
              ))}
            </ul>
          </section>
        </div>

        <section className="prog-detail__unis">
          <h2 className="prog-detail__section-title">
            Вузы с этим направлением
            {unis && <span className="prog-detail__unis-count">{unis.total}</span>}
          </h2>

          {loadingUnis && (
            <div className="prog-detail__unis-loading">Ищем вузы…</div>
          )}

          {!loadingUnis && unis && unis.items.length === 0 && (
            <div className="prog-detail__unis-empty">
              Данные о вузах по этому направлению появятся после обновления базы.
            </div>
          )}

          {!loadingUnis && unis && unis.items.length > 0 && (
            <div className="prog-detail__unis-list">
              {unis.items.map((item, idx) => (
                <div key={item.university_id} className="prog-detail__uni-row">
                  <span className="prog-detail__uni-rank">#{idx + 1}</span>
                  <div className="prog-detail__uni-logo">
                    {item.logo_url ? (
                      <img
                        src={item.logo_url}
                        alt={item.canonical_name}
                        onError={(e) => {
                          (e.currentTarget as HTMLImageElement).style.display = "none";
                          const parent = e.currentTarget.parentElement as HTMLElement;
                          parent.textContent = item.canonical_name.charAt(0);
                        }}
                      />
                    ) : (
                      item.canonical_name.charAt(0)
                    )}
                  </div>
                  <div className="prog-detail__uni-info">
                    <p className="prog-detail__uni-name">{item.canonical_name}</p>
                    {item.city && (
                      <p className="prog-detail__uni-city">📍 {item.city}</p>
                    )}
                  </div>
                  <div className="prog-detail__uni-score">
                    ★ {item.score.toFixed(1)}
                  </div>
                </div>
              ))}
            </div>
          )}
        </section>
      </div>
    </div>
  );
}

// ── Main page ──────────────────────────────────────────────────────────────────

const UGNS_GROUPS = Array.from(new Set(DIRECTIONS.map((d) => d.ugnsGroup)));

export function ProgramsPage() {
  const { backendApi } = useFrontendRuntime();
  const [filterSubject, setFilterSubject] = useState<string | null>(null);
  const [filterGroup, setFilterGroup] = useState<string | null>(null);
  const [selected, setSelected] = useState<Direction | null>(null);
  const [unis, setUnis] = useState<BackendSearchResponse | null>(null);
  const [loadingUnis, setLoadingUnis] = useState(false);

  useEffect(() => {
    if (!selected) {
      setUnis(null);
      return;
    }
    setLoadingUnis(true);
    setUnis(null);
    backendApi
      .searchUniversities({ query: selected.searchQuery, pageSize: 10 })
      .then((r) => setUnis(r))
      .catch(() => setUnis(null))
      .finally(() => setLoadingUnis(false));
  }, [selected, backendApi]);

  const filtered = DIRECTIONS.filter((d) => {
    if (filterGroup && d.ugnsGroup !== filterGroup) return false;
    if (filterSubject && !d.egeSubjects.some((variant) => variant.includes(filterSubject))) return false;
    return true;
  });

  if (selected) {
    return (
      <div className="programs-page">
        <DirectionDetail
          direction={selected}
          unis={unis}
          loadingUnis={loadingUnis}
          onBack={() => setSelected(null)}
        />
      </div>
    );
  }

  return (
    <div className="programs-page">
      <div className="programs-page__header">
        <h1 className="programs-page__title">Программы высшего образования</h1>
        <p className="programs-page__sub">
          Изучите направления подготовки, узнайте о профессиях и найдите вузы, где они есть.
        </p>
      </div>

      <div className="programs-page__filters">
        <div className="programs-page__filter-row">
          <span className="programs-page__filter-label">Область:</span>
          <button
            className={`programs-page__filter-chip${filterGroup === null ? " programs-page__filter-chip--active" : ""}`}
            type="button"
            onClick={() => setFilterGroup(null)}
          >
            Все области
          </button>
          {UGNS_GROUPS.map((g) => (
            <button
              key={g}
              className={`programs-page__filter-chip${filterGroup === g ? " programs-page__filter-chip--active" : ""}`}
              type="button"
              onClick={() => setFilterGroup(filterGroup === g ? null : g)}
            >
              {g}
            </button>
          ))}
        </div>

        <div className="programs-page__filter-row">
          <span className="programs-page__filter-label">Предмет ЕГЭ:</span>
          <button
            className={`programs-page__filter-chip programs-page__filter-chip--ege${filterSubject === null ? " programs-page__filter-chip--active" : ""}`}
            type="button"
            onClick={() => setFilterSubject(null)}
          >
            Все предметы
          </button>
          {EGE_FILTER_SUBJECTS.map((subj) => (
            <button
              key={subj}
              className={`programs-page__filter-chip programs-page__filter-chip--ege${filterSubject === subj ? " programs-page__filter-chip--active" : ""}`}
              type="button"
              onClick={() => setFilterSubject(filterSubject === subj ? null : subj)}
            >
              {subj}
            </button>
          ))}
        </div>
      </div>

      {filtered.length === 0 && (
        <div className="programs-page__empty">
          По выбранным фильтрам направлений не найдено. Попробуйте изменить фильтры.
        </div>
      )}

      <div className="programs-page__grid">
        {filtered.map((direction) => (
          <DirectionCard
            key={direction.code}
            direction={direction}
            onClick={() => setSelected(direction)}
          />
        ))}
      </div>
    </div>
  );
}
