import { useEffect, useMemo, useState } from "react";

import { HomePage } from "./pages/HomePage";
import { SearchWorkspacePage } from "./pages/SearchWorkspacePage";
import { useSelectedUniversity } from "./shared/selected-university";
import { EvidenceDrawer } from "./widgets/evidence-drawer/EvidenceDrawer";

type AppView = "search" | "monitoring" | "evidence";

const APP_VIEWS: Array<{
  description: string;
  id: AppView;
  label: string;
}> = [
  {
    id: "search",
    label: "Поиск",
    description: "Главная рабочая зона для поиска вузов и просмотра карточки.",
  },
  {
    id: "monitoring",
    label: "Мониторинг",
    description: "Состояние пайплайна, реестра источников и актуальности данных.",
  },
  {
    id: "evidence",
    label: "Доказательства",
    description: "Provenance, атрибуция полей и цепочка доказательств по выбранному вузу.",
  },
];

export default function App() {
  const { activeUniversityId } = useSelectedUniversity();
  const [activeView, setActiveView] = useState<AppView>(readViewFromLocation);

  useEffect(() => {
    const handleHashChange = () => {
      setActiveView(readViewFromLocation());
    };

    window.addEventListener("hashchange", handleHashChange);
    return () => {
      window.removeEventListener("hashchange", handleHashChange);
    };
  }, []);

  const activeViewMeta = useMemo(
    () => APP_VIEWS.find((entry) => entry.id === activeView) ?? APP_VIEWS[0],
    [activeView],
  );

  return (
    <div className="app">
      <header className="app__hero">
        <div className="app__hero-content">
          <p className="app__eyebrow">Агрегатор вузов с опорой на доказательства</p>
          <h1 className="app__title">Поиск, карточки вузов и provenance в одном интерфейсе.</h1>
          <p className="app__copy">
            Главная страница сфокусирована на поиске. Мониторинг пайплайна и панель доказательств
            вынесены в отдельные представления, чтобы рабочая зона не разваливалась по сетке.
          </p>
        </div>

        <div className="app__hero-meta">
          <div className="app__meta-card">
            <span className="app__meta-label">Текущий режим</span>
            <strong>{activeViewMeta.label}</strong>
            <small>{activeViewMeta.description}</small>
          </div>
          <div className="app__meta-card">
            <span className="app__meta-label">Выбранный вуз</span>
            <strong>{activeUniversityId ? "Есть активная карточка" : "Пока не выбран"}</strong>
            <small>{activeUniversityId ?? "Открой карточку из результатов поиска."}</small>
          </div>
        </div>
      </header>

      <nav className="app__nav" aria-label="Разделы интерфейса">
        {APP_VIEWS.map((view) => (
          <button
            key={view.id}
            className={`app__nav-button ${
              view.id === activeView ? "app__nav-button--active" : ""
            }`}
            type="button"
            onClick={() => navigateToView(view.id)}
          >
            <span>{view.label}</span>
            <small>{view.description}</small>
          </button>
        ))}
      </nav>

      <main className="app__main">
        {activeView === "search" ? <SearchWorkspacePage /> : null}
        {activeView === "monitoring" ? <HomePage /> : null}
        {activeView === "evidence" ? <EvidenceDrawer /> : null}
      </main>
    </div>
  );
}

function navigateToView(view: AppView): void {
  const nextHash = `#${view}`;
  if (window.location.hash === nextHash) {
    return;
  }
  window.location.hash = nextHash;
}

function readViewFromLocation(): AppView {
  const hash = window.location.hash.replace("#", "");
  return APP_VIEWS.some((entry) => entry.id === hash) ? (hash as AppView) : "search";
}
