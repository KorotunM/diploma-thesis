import type { CSSProperties } from "react";
import { useEffect, useState } from "react";

import type { BackendSearchResponse, ProgramDirectoryItemDto } from "../shared/backend-api";
import { useFrontendRuntime } from "../shared/runtime";
import { ViewState } from "../shared/ui/view-state";

type ProgramSort = "universities" | "score" | "budget";

const PRIMARY_SUBJECTS = ["Информатика", "Физика", "Обществознание", "Биология", "Химия"];
const ADDITIONAL_SUBJECTS = [
  "Математика",
  "История",
  "Литература",
  "География",
  "Иностранный язык",
];

const SORT_OPTIONS: Array<{ value: ProgramSort; label: string; icon: string }> = [
  { value: "universities", label: "по рейтингу вузов", icon: "↗" },
  { value: "score", label: "по проходному баллу", icon: "⇅" },
  { value: "budget", label: "по количеству вузов", icon: "▥" },
];

function programGroup(code: string): string {
  const prefix = code.split(".")[0];
  if (prefix === "09" || prefix === "10") return "IT и цифровые технологии";
  if (prefix === "01" || prefix === "02") return "Математика и точные науки";
  if (["08", "13", "15", "19", "20", "21", "23", "27"].includes(prefix)) return "Инженерия и технологии";
  if (prefix === "38") return "Экономика и управление";
  if (prefix === "36" || prefix === "35") return "Аграрные и биологические науки";
  if (["37", "40", "41", "42", "44", "45", "46"].includes(prefix)) return "Гуманитарные науки";
  if (prefix === "43") return "Сервис и туризм";
  return "Другие направления";
}

function programColor(code: string): string {
  const group = programGroup(code);
  if (group.includes("IT")) return "#2563eb";
  if (group.includes("Математика")) return "#7c3aed";
  if (group.includes("Экономика")) return "#059669";
  if (group.includes("Аграрные")) return "#16a34a";
  if (group.includes("Гуманитарные")) return "#0891b2";
  if (group.includes("Сервис")) return "#ea580c";
  return "#d97706";
}

function directionIcon(program: ProgramDirectoryItemDto): string {
  const group = programGroup(program.code);
  if (group.includes("IT")) return "</>";
  if (group.includes("Математика")) return "Σ";
  if (group.includes("Экономика")) return "▥";
  if (group.includes("Аграрные")) return "✚";
  if (group.includes("Гуманитарные")) return "✎";
  return "□";
}

function subjectMatches(program: ProgramDirectoryItemDto, selectedSubjects: string[]): boolean {
  if (selectedSubjects.length === 0) return true;
  return selectedSubjects.some((subject) =>
    program.ege_subjects.some((item) => item.toLowerCase().includes(subject.toLowerCase())),
  );
}

// ── Direction list card ────────────────────────────────────────────────────────

function DirectionRow({
  program,
  onClick,
}: {
  program: ProgramDirectoryItemDto;
  onClick: () => void;
}) {
  return (
    <article
      className="program-row-card"
      role="button"
      tabIndex={0}
      onClick={onClick}
      onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onClick(); } }}
    >
      <div className="program-row-card__icon" aria-hidden>
        {directionIcon(program)}
      </div>

      <div className="program-row-card__main">
        <span className="program-row-card__code">{program.code}</span>
        <h3 className="program-row-card__name">{program.name}</h3>
        <p className="program-row-card__learns">
          {program.description ?? `Направление группы «${programGroup(program.code)}».`}
        </p>
      </div>

      <div className="program-row-card__subjects">
        <span className="program-row-card__subjects-title">Предметы ЕГЭ</span>
        <div className="program-row-card__subject-list">
          {program.ege_subjects.slice(0, 4).map((subject) => (
            <span key={subject} className="program-row-card__subject">
              {subject}
            </span>
          ))}
        </div>
      </div>

      <div className="program-row-card__metric">
        <strong>{program.university_count}</strong>
        <span>вузов</span>
      </div>
      <div className="program-row-card__metric">
        <strong>{program.avg_passing_score ?? "—"}</strong>
        <span>средний балл</span>
      </div>
      <div className="program-row-card__metric">
        <strong>{program.budget_places.toLocaleString("ru-RU")}</strong>
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
  program,
  unis,
  loadingUnis,
  onBack,
}: {
  program: ProgramDirectoryItemDto;
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
        style={{ background: `linear-gradient(135deg, ${programColor(program.code)}cc 0%, ${programColor(program.code)}88 100%)` }}
      >
        <div className="prog-detail__hero-inner">
          <span className="prog-detail__code">{program.code}</span>
          <h1 className="prog-detail__name">{program.name}</h1>
          <div className="prog-detail__meta">
            <span className="prog-detail__level">{program.level ?? "Бакалавриат"}</span>
            <span className="prog-detail__group">{programGroup(program.code)}</span>
          </div>
        </div>
      </div>

      <div className="prog-detail__body">
        <div className="prog-detail__main">
          <section className="prog-detail__section">
            <h2 className="prog-detail__section-title">О направлении</h2>
            <p className="prog-detail__description">
              {program.description ?? "Описание направления появится после загрузки официальных карточек вузов."}
            </p>
          </section>

          <section className="prog-detail__section">
            <h2 className="prog-detail__section-title">Требуемые предметы ЕГЭ</h2>
            <div className="prog-detail__ege-variants">
              <div className="prog-detail__ege-variant">
                <div className="prog-detail__ege-chips">
                  {program.ege_subjects.map((subj) => (
                    <span key={subj} className="prog-detail__ege-chip">{subj}</span>
                  ))}
                </div>
              </div>
            </div>
          </section>

          <section className="prog-detail__section">
            <h2 className="prog-detail__section-title">Сводные показатели</h2>
            <ul className="prog-detail__professions">
              <li className="prog-detail__profession"><span className="prog-detail__profession-dot" />Вузов: {program.university_count}</li>
              <li className="prog-detail__profession"><span className="prog-detail__profession-dot" />Бюджетных мест: {program.budget_places.toLocaleString("ru-RU")}</li>
              <li className="prog-detail__profession"><span className="prog-detail__profession-dot" />Платных мест: {program.paid_places.toLocaleString("ru-RU")}</li>
              <li className="prog-detail__profession"><span className="prog-detail__profession-dot" />Стоимость от: {program.min_tuition_per_year ? `${program.min_tuition_per_year.toLocaleString("ru-RU")} ₽` : "—"}</li>
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
  const [programs, setPrograms] = useState<ProgramDirectoryItemDto[]>([]);
  const [programsLoading, setProgramsLoading] = useState(true);
  const [programsError, setProgramsError] = useState<string | null>(null);
  const [selected, setSelected] = useState<ProgramDirectoryItemDto | null>(null);
  const [unis, setUnis] = useState<BackendSearchResponse | null>(null);
  const [loadingUnis, setLoadingUnis] = useState(false);

  useEffect(() => {
    setProgramsLoading(true);
    backendApi
      .getPrograms()
      .then((response) => {
        setPrograms(response.items);
        setProgramsError(null);
      })
      .catch((error: unknown) => {
        setProgramsError(error instanceof Error ? error.message : "Не удалось загрузить программы");
      })
      .finally(() => setProgramsLoading(false));
  }, [backendApi]);

  useEffect(() => {
    if (!selected) {
      setUnis(null);
      return;
    }
    setLoadingUnis(true);
    setUnis(null);
    backendApi
      .searchUniversities({ programCodes: [selected.code], pageSize: 10 })
      .then((r) => setUnis(r))
      .catch(() => setUnis(null))
      .finally(() => setLoadingUnis(false));
  }, [selected, backendApi]);

  const filtered = programs
    .filter((program) => {
      const normalizedQuery = query.trim().toLowerCase();
      const matchesQuery =
        !normalizedQuery ||
        program.name.toLowerCase().includes(normalizedQuery) ||
        program.code.includes(normalizedQuery) ||
        (program.description ?? "").toLowerCase().includes(normalizedQuery);
      return matchesQuery && subjectMatches(program, selectedSubjects);
    })
    .sort((left, right) => {
      if (sortBy === "score") return (right.avg_passing_score ?? 0) - (left.avg_passing_score ?? 0);
      if (sortBy === "budget") return right.budget_places - left.budget_places;
      return right.university_count - left.university_count;
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
          program={selected}
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
        <div className="programs-subject-panel__head">
          <span className="programs-subject-panel__title">Предметы ЕГЭ</span>
          <button
            className={`programs-subject-toggle${showAllSubjects ? " programs-subject-toggle--open" : ""}`}
            type="button"
            aria-expanded={showAllSubjects}
            onClick={() => setShowAllSubjects((value) => !value)}
          >
            {showAllSubjects ? "Скрыть дополнительные" : "Ещё предметы"}
            <span className="programs-subject-toggle__chevron" aria-hidden>⌄</span>
          </button>
        </div>

        <div className="programs-subject-panel__chips">
          {PRIMARY_SUBJECTS.map((subject) => {
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
        </div>

        <div
          className={`programs-subject-extra${showAllSubjects ? " programs-subject-extra--open" : ""}`}
          aria-hidden={!showAllSubjects}
        >
          <div className="programs-subject-extra__inner">
            {ADDITIONAL_SUBJECTS.map((subject, index) => {
              const active = selectedSubjects.includes(subject);
              return (
                <button
                  key={subject}
                  className={`programs-subject-chip programs-subject-chip--extra${active ? " programs-subject-chip--active" : ""}`}
                  type="button"
                  tabIndex={showAllSubjects ? 0 : -1}
                  style={{ "--chip-index": index } as CSSProperties}
                  onClick={() => toggleSubject(subject)}
                >
                  {subject}
                  {active && <span>✓</span>}
                </button>
              );
            })}
          </div>
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

      {programsLoading && (
        <ViewState
          kind="loading"
          title="Загружаем направления"
          message="Собираем список программ из актуальных карточек вузов."
        />
      )}

      {!programsLoading && programsError && (
        <ViewState kind="error" title="Не удалось загрузить программы" message={programsError} />
      )}

      {!programsLoading && !programsError && filtered.length === 0 && (
        <div className="programs-page__empty">
          По выбранным фильтрам направлений не найдено. Попробуйте изменить фильтры.
        </div>
      )}

      {!programsLoading && !programsError && (
        <div className="programs-list">
          {visibleDirections.map((direction) => (
          <DirectionRow
            key={direction.code}
            program={direction}
            onClick={() => setSelected(direction)}
          />
          ))}
        </div>
      )}

      {!programsLoading && !programsError && visibleCount < filtered.length && (
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
