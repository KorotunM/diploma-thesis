import { useEffect, useState } from "react";

import { AdminDashboard } from "./pages/AdminDashboard";
import { ComparisonPage } from "./pages/ComparisonPage";
import { SearchWorkspacePage } from "./pages/SearchWorkspacePage";
import { UniversityDetailPage } from "./pages/UniversityDetailPage";
import { LoginModal } from "./components/LoginModal";
import { useAuth } from "./shared/auth";
import logoSvg from "./assets/logo.svg";

type AppView = "search" | "university" | "admin" | "comparison";

const ALL_VIEW_IDS: AppView[] = ["search", "university", "admin", "comparison"];

const NAV_LINKS: Array<{ id: AppView | null; label: string; soon?: boolean }> = [
  { id: "search", label: "Поиск вуза" },
  { id: "comparison", label: "Сравнение" },
  { id: null, label: "Специальности", soon: true },
  { id: null, label: "Калькулятор", soon: true },
  { id: null, label: "Рейтинги", soon: true },
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
  const { user, logout } = useAuth();

  const [activeView, setActiveView] = useState<AppView>(readViewFromLocation);
  const [showLogin, setShowLogin] = useState(false);

  const isAdmin = user?.email === "admin@example.com";

  useEffect(() => {
    const handleHashChange = () => setActiveView(readViewFromLocation());
    window.addEventListener("hashchange", handleHashChange);
    return () => window.removeEventListener("hashchange", handleHashChange);
  }, []);

  const handleLoginSuccess = () => {
    setShowLogin(false);
  };

  const handleLogout = async () => {
    await logout();
    navigateTo("search");
  };

  const handleLoginClick = () => {
    if (user) {
      if (isAdmin) navigateTo("admin");
    } else {
      setShowLogin(true);
    }
  };

  const displayName = user?.display_name ?? user?.email ?? null;
  const avatarLetter = displayName ? displayName[0].toUpperCase() : "А";

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
            <img src={logoSvg} alt="Абитуриент+" className="app__logo-icon" style={{ borderRadius: 10 }} />
            <div className="app__logo-text">
              <span className="app__logo-title">Абитуриент+</span>
              <span className="app__logo-sub">Навигатор в мир образования</span>
            </div>
          </button>

          <nav className="app__nav" aria-label="Навигация">
            {NAV_LINKS.map(({ id, label, soon }) => (
              <button
                key={label}
                className={`app__nav-link ${id === activeView ? "app__nav-link--active" : ""}`}
                type="button"
                title={soon ? "Раздел в разработке" : undefined}
                onClick={() => {
                  if (id) navigateTo(id);
                  else alert(`Раздел «${label}» находится в разработке.`);
                }}
              >
                {label}
                {soon && (
                  <span style={{ fontSize: "0.6rem", marginLeft: 4, opacity: 0.6, verticalAlign: "super" }}>
                    скоро
                  </span>
                )}
              </button>
            ))}
          </nav>

          <div className="app__header-actions">
            {user ? (
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <button
                  className="app__user-btn"
                  type="button"
                  onClick={handleLoginClick}
                  title={user.email}
                >
                  <div className="app__user-avatar">{avatarLetter}</div>
                  {displayName}
                </button>
                <button
                  className="app__login-btn"
                  type="button"
                  onClick={handleLogout}
                  style={{ fontSize: "0.8rem" }}
                >
                  Выйти
                </button>
              </div>
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
        {activeView === "university" && (
          <UniversityDetailPage onShowLogin={() => setShowLogin(true)} />
        )}
        {activeView === "comparison" && (
          <ComparisonPage onShowLogin={() => setShowLogin(true)} />
        )}
        {activeView === "admin" && isAdmin && <AdminDashboard onLogout={handleLogout} />}
        {activeView === "admin" && !isAdmin && <SearchWorkspacePage />}
      </main>

      {showLogin && (
        <LoginModal onClose={() => setShowLogin(false)} onSuccess={handleLoginSuccess} />
      )}
    </div>
  );
}
