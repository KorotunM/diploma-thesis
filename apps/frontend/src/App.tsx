import { useEffect, useState } from "react";

import { HomePage } from "./pages/HomePage";
import { SearchWorkspacePage } from "./pages/SearchWorkspacePage";
import { UniversityCardPage } from "./pages/UniversityCardPage";
import { EvidenceDrawer } from "./widgets/evidence-drawer/EvidenceDrawer";

type AppView = "search" | "monitoring" | "evidence" | "university";

const NAV_VIEWS: Array<{ id: AppView; label: string }> = [
  { id: "search", label: "Поиск" },
  { id: "monitoring", label: "Мониторинг" },
  { id: "evidence", label: "Доказательства" },
];

const ALL_VIEW_IDS: AppView[] = ["search", "monitoring", "evidence", "university"];

export default function App() {
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

  return (
    <div className="app">
      <header className="app__header">
        <span className="app__logo">Агрегатор вузов</span>
        <nav className="app__nav" aria-label="Разделы интерфейса">
          {NAV_VIEWS.map((view) => (
            <button
              key={view.id}
              className={`app__nav-button ${
                view.id === activeView ? "app__nav-button--active" : ""
              }`}
              type="button"
              onClick={() => navigateToView(view.id)}
            >
              {view.label}
            </button>
          ))}
        </nav>
      </header>

      <main className="app__main">
        {activeView === "search" ? <SearchWorkspacePage /> : null}
        {activeView === "monitoring" ? <HomePage /> : null}
        {activeView === "evidence" ? <EvidenceDrawer /> : null}
        {activeView === "university" ? <UniversityCardPage /> : null}
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
  return ALL_VIEW_IDS.includes(hash as AppView) ? (hash as AppView) : "search";
}
