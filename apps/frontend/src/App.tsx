import { useEffect, useState } from "react";

import { SearchWorkspacePage } from "./pages/SearchWorkspacePage";
import { UniversityCardPage } from "./pages/UniversityCardPage";
import { AdminDashboard } from "./pages/AdminDashboard";
import { LoginModal } from "./components/LoginModal";

type AppView = "search" | "university" | "admin";

const ALL_VIEW_IDS: AppView[] = ["search", "university", "admin"];

const NAV_LINKS = [
  { id: "search" as AppView, label: "Поиск вуза" },
  { id: null, label: "Специальности" },
  { id: null, label: "Калькулятор" },
  { id: null, label: "Отзывы" },
  { id: null, label: "Рейтинги" },
];

function readViewFromLocation(): AppView {
  const hash = window.location.hash.replace("#", "");
  return ALL_VIEW_IDS.includes(hash as AppView) ? (hash as AppView) : "search";
}

function navigateTo(view: AppView): void {
  const next = `#${view}`;
  if (window.location.hash !== next) {
    window.location.hash = next;
  }
}

export default function App() {
  const [activeView, setActiveView] = useState<AppView>(readViewFromLocation);
  const [isAdmin, setIsAdmin] = useState(
    () => localStorage.getItem("admin_auth") === "1",
  );
  const [showLogin, setShowLogin] = useState(false);

  useEffect(() => {
    const handleHashChange = () => setActiveView(readViewFromLocation());
    window.addEventListener("hashchange", handleHashChange);
    return () => window.removeEventListener("hashchange", handleHashChange);
  }, []);

  const handleLoginSuccess = () => {
    setIsAdmin(true);
    setShowLogin(false);
    navigateTo("admin");
  };

  const handleLogout = () => {
    localStorage.removeItem("admin_auth");
    setIsAdmin(false);
    navigateTo("search");
  };

  const handleLoginClick = () => {
    if (isAdmin) {
      navigateTo("admin");
    } else {
      setShowLogin(true);
    }
  };

  return (
    <div className="app">
      <header className="app__header">
        <div className="app__header-inner">
          <button
            className="app__logo"
            type="button"
            onClick={() => navigateTo("search")}
            style={{ border: "none", cursor: "pointer", background: "none" }}
          >
            <div className="app__logo-icon">А+</div>
            <div className="app__logo-text">
              <span className="app__logo-title">Агрегатор вузов</span>
              <span className="app__logo-sub">Навигатор в мир образования</span>
            </div>
          </button>

          <nav className="app__nav" aria-label="Навигация">
            {NAV_LINKS.map(({ id, label }) => (
              <button
                key={label}
                className={`app__nav-link ${id === activeView ? "app__nav-link--active" : ""}`}
                type="button"
                onClick={() => id && navigateTo(id)}
                disabled={id === null}
                style={id === null ? { opacity: 0.4, cursor: "default" } : undefined}
              >
                {label}
              </button>
            ))}
          </nav>

          <div className="app__header-actions">
            {isAdmin ? (
              <button className="app__user-btn" type="button" onClick={handleLoginClick}>
                <div className="app__user-avatar">A</div>
                Личный кабинет
              </button>
            ) : (
              <button className="app__login-btn" type="button" onClick={handleLoginClick}>
                <span>→</span> Войти
              </button>
            )}
          </div>
        </div>
      </header>

      <main className="app__main">
        {activeView === "search" && <SearchWorkspacePage />}
        {activeView === "university" && <UniversityCardPage />}
        {activeView === "admin" && isAdmin && <AdminDashboard onLogout={handleLogout} />}
        {activeView === "admin" && !isAdmin && <SearchWorkspacePage />}
      </main>

      {showLogin && (
        <LoginModal onClose={() => setShowLogin(false)} onSuccess={handleLoginSuccess} />
      )}
    </div>
  );
}
