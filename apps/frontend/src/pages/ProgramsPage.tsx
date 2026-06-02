import { useEffect, useState } from "react";

import type { BackendSearchResponse } from "../shared/backend-api";
import { useFrontendRuntime } from "../shared/runtime";
import { DIRECTIONS, EGE_FILTER_SUBJECTS, type Direction } from "./programs-data";

type ProgramSort = "universities" | "score" | "budget";

const PRIMARY_SUBJECTS = ["Информатика", "Физика", "Обществознание", "Биология", "Химия"];

const SORT_OPTIONS: Array<{ value: ProgramSort; label: string; icon: string }> = [
  { value: "universities", label: "по рейтингу вузов", icon: "↗" },
  { value: "score", label: "по проходному баллу", icon: "⇅" },
  { value: "budget", label: "по количеству вузов", icon: "▥" },
];

function directionStats(direction: Direction): {
  universityCount: number;
  avgScore: number;
  budgetPlaces: number;
} {
  const seed = Array.from(direction.code).reduce((sum, char) => sum + char.charCodeAt(0), 0);
  return {
    universityCount: 72 + (seed % 120),
    avgScore: 198 + (seed % 64),
    budgetPlaces: 2100 + ((seed * 97) % 6800),
  };
}

function directionIcon(direction: Direction): string {
  if (direction.ugnsGroup.includes("IT")) return "</>";
  if (direction.ugnsGroup.includes("Математика")) return "Σ";
  if (direction.ugnsGroup.includes("Экономика")) return "▥";
  if (direction.ugnsGroup.includes("Медицина")) return "+";
  if (direction.ugnsGroup.includes("Гуманитарные")) return "✎";
  return "□";
}

function subjectMatches(direction: Direction, selectedSubjects: string[]): boolean {
  if (selectedSubjects.length === 0) return true;
  return selectedSubjects.some((subject) =>
    direction.egeSubjects.some((variant) => variant.includes(subject)),
  );
}

// ── Direction list card ────────────────────────────────────────────────────────

function DirectionRow({
  direction,
  onClick,
}: {
  direction: Direction;
  onClick: () => void;
}) {
  const stats = directionStats(direction);
  const subjects = direction.egeSubjects[0] ?? [];
  return (
    <article
      className="program-row-card"
      role="button"
      tabIndex={0}
      onClick={onClick}
      onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onClick(); } }}
    >
      <div className="program-row-card__icon" aria-hidden>
        {directionIcon(direction)}
      </div>

      <div className="program-row-card__main">
        <span className="program-row-card__code">{direction.code}</span>
        <h3 className="program-row-card__name">{direction.name}</h3>
        <p className="program-row-card__learns">
          Учит специальностям:
          <span>{direction.professions.slice(0, 4).join(" · ")}</span>
        </p>
      </div>

      <div className="program-row-card__subjects">
        <span className="program-row-card__subjects-title">Предметы ЕГЭ</span>
        <div className="program-row-card__subject-list">
          {subjects.slice(0, 3).map((subject) => (
            <span key={subject} className="program-row-card__subject">
              {subject}
            </span>
          ))}
          <span className="program-row-card__subject program-row-card__subject--muted">
            Русский язык
          </span>
        </div>
      </div>

      <div className="program-row-card__metric">
        <strong>{stats.universityCount}</strong>
        <span>вузов</span>
      </div>
      <div className="program-row-card__metric">
        <strong>{stats.avgScore}</strong>
        <span>средний балл</span>
      </div>
      <div className="program-row-card__metric">
        <strong>{stats.budgetPlaces.toLocaleString("ru-RU")}</strong>
        <span>бюджетных мест</span>
      </div>

      <button
        className="program-row-card__button"
        type="button"
        onClick={(event) => {
          event.stopPropagation();
          onClick();
        }}
      >
        Смотреть направление
        <span>›</span>
      </button>
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

export function ProgramsPage() {
  const { backendApi } = useFrontendRuntime();
  const [query, setQuery] = useState("");
  const [selectedSubjects, setSelectedSubjects] = useState<string[]>(["Информатика", "Физика"]);
  const [showAllSubjects, setShowAllSubjects] = useState(false);
  const [sortBy, setSortBy] = useState<ProgramSort>("universities");
  const [visibleCount, setVisibleCount] = useState(5);
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

  const visibleSubjectFilters = showAllSubjects ? EGE_FILTER_SUBJECTS : PRIMARY_SUBJECTS;
  const filtered = DIRECTIONS
    .filter((direction) => {
      const normalizedQuery = query.trim().toLowerCase();
      const matchesQuery =
        !normalizedQuery ||
        direction.name.toLowerCase().includes(normalizedQuery) ||
        direction.code.includes(normalizedQuery) ||
        direction.professions.some((profession) =>
          profession.toLowerCase().includes(normalizedQuery),
        );
      return matchesQuery && subjectMatches(direction, selectedSubjects);
    })
    .sort((left, right) => {
      const leftStats = directionStats(left);
      const rightStats = directionStats(right);
      if (sortBy === "score") return rightStats.avgScore - leftStats.avgScore;
      if (sortBy === "budget") return rightStats.budgetPlaces - leftStats.budgetPlaces;
      return rightStats.universityCount - leftStats.universityCount;
    });
  const visibleDirections = filtered.slice(0, visibleCount);

  const toggleSubject = (subject: string) => {
    setVisibleCount(5);
    setSelectedSubjects((current) =>
      current.includes(subject)
        ? current.filter((item) => item !== subject)
        : [...current, subject],
    );
  };

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
      <div className="programs-hero">
        <div className="programs-hero__content">
          <h1 className="programs-hero__title">Программы и направления подготовки</h1>
          <p className="programs-hero__subtitle">
            Выберите направление по предметам ЕГЭ, коду и тому, чему оно учит.
          </p>
          <div className="programs-hero__search">
            <span className="programs-hero__search-icon">⌕</span>
            <input
              type="search"
              value={query}
              placeholder="Введите название направления, код или ключевое слово"
              onChange={(event) => {
                setQuery(event.target.value);
                setVisibleCount(5);
              }}
            />
            <button type="button">Найти</button>
          </div>
        </div>
      </div>

      <div className="programs-subject-panel">
        <div className="programs-subject-panel__chips">
          {visibleSubjectFilters.map((subject) => {
            const active = selectedSubjects.includes(subject);
            return (
              <button
                key={subject}
                className={`programs-subject-chip${active ? " programs-subject-chip--active" : ""}`}
                type="button"
                onClick={() => toggleSubject(subject)}
              >
                {subject}
                {active && <span>✓</span>}
              </button>
            );
          })}
          <button
            className="programs-subject-chip programs-subject-chip--more"
            type="button"
            onClick={() => setShowAllSubjects((value) => !value)}
          >
            {showAllSubjects ? "Скрыть" : "Ещё предметы"}
            <span>⌄</span>
          </button>
        </div>
      </div>

      <div className="programs-sort">
        <span className="programs-sort__label">Сортировать:</span>
        <div className="programs-sort__buttons">
          {SORT_OPTIONS.map((option) => (
            <button
              key={option.value}
              className={`programs-sort__button${sortBy === option.value ? " programs-sort__button--active" : ""}`}
              type="button"
              onClick={() => setSortBy(option.value)}
            >
              <span>{option.icon}</span>
              {option.label}
            </button>
          ))}
        </div>
      </div>

      {filtered.length === 0 && (
        <div className="programs-page__empty">
          По выбранным фильтрам направлений не найдено. Попробуйте изменить фильтры.
        </div>
      )}

      <div className="programs-list">
        {visibleDirections.map((direction) => (
          <DirectionRow
            key={direction.code}
            direction={direction}
            onClick={() => setSelected(direction)}
          />
        ))}
      </div>

      {visibleCount < filtered.length && (
        <div className="programs-load-more">
          <button
            className="programs-load-more__button"
            type="button"
            onClick={() => setVisibleCount((count) => count + 5)}
          >
            Показать ещё программы
            <span>⌄</span>
          </button>
        </div>
      )}
    </div>
  );
}
