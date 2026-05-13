import { useEffect, useRef, useState } from "react";

import { AdminDashboard } from "./pages/AdminDashboard";
import { ComparisonPage } from "./pages/ComparisonPage";
import { ProfilePage } from "./pages/ProfilePage";
import { ProgramsPage } from "./pages/ProgramsPage";
import { RatingsPage } from "./pages/RatingsPage";
import { SearchWorkspacePage } from "./pages/SearchWorkspacePage";
import { UniversityDetailPage } from "./pages/UniversityDetailPage";
import { AiChatWidget } from "./components/AiChatWidget";
import { LoginModal } from "./components/LoginModal";
import { useAuth } from "./shared/auth";
import logoSvg from "./assets/logo.svg";

type AppView = "search" | "university" | "admin" | "comparison" | "profile" | "ratings" | "programs";

const ALL_VIEW_IDS: AppView[] = ["search", "university", "admin", "comparison", "profile", "ratings", "programs"];

const NAV_LINKS: Array<{ id: AppView; label: string }> = [
  { id: "search", label: "Поиск вуза" },
  { id: "comparison", label: "Сравнение" },
  { id: "programs", label: "Программы" },
  { id: "ratings", label: "Рейтинги" },
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
  const [showUserMenu, setShowUserMenu] = useState(false);
  const userMenuRef = useRef<HTMLDivElement>(null);

  const isAdmin = user?.email === "admin@example.com";

  useEffect(() => {
    const handleHashChange = () => setActiveView(readViewFromLocation());
    window.addEventListener("hashchange", handleHashChange);
    return () => window.removeEventListener("hashchange", handleHashChange);
  }, []);

  useEffect(() => {
    if (!showUserMenu) return;
    const onOutside = (e: MouseEvent) => {
      if (userMenuRef.current && !userMenuRef.current.contains(e.target as Node)) {
        setShowUserMenu(false);
      }
    };
    document.addEventListener("mousedown", onOutside);
    return () => document.removeEventListener("mousedown", onOutside);
  }, [showUserMenu]);

  const handleLoginSuccess = () => {
    setShowLogin(false);
  };

  const handleLogout = async () => {
    setShowUserMenu(false);
    await logout();
    navigateTo("search");
  };

  const handleLoginClick = () => {
    if (user) {
      setShowUserMenu((v) => !v);
    } else {
      setShowLogin(true);
    }
  };

  const handleGoToProfile = () => {
    setShowUserMenu(false);
    navigateTo("profile");
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
            {NAV_LINKS.map(({ id, label }) => (
              <button
                key={id}
                className={`app__nav-link ${id === activeView ? "app__nav-link--active" : ""}`}
                type="button"
                onClick={() => navigateTo(id)}
              >
                {label}
              </button>
            ))}
          </nav>

          <div className="app__header-actions">
            {user ? (
              <div ref={userMenuRef} className="app__user-wrap">
                <button
                  className={`app__user-btn${showUserMenu ? " app__user-btn--active" : ""}`}
                  type="button"
                  onClick={handleLoginClick}
                  title={user.email}
                >
                  <div className="app__user-avatar">{avatarLetter}</div>
                  {displayName}
                  <span className="app__user-chevron">{showUserMenu ? "▲" : "▼"}</span>
                </button>

                {showUserMenu && (
                  <div className="app__user-menu" role="menu">
                    <button
                      className="app__user-menu-item"
                      type="button"
                      role="menuitem"
                      onClick={handleGoToProfile}
                    >
                      <span className="app__user-menu-icon">👤</span>
                      Кабинет
                    </button>
                    {isAdmin && (
                      <button
                        className="app__user-menu-item"
                        type="button"
                        role="menuitem"
                        onClick={() => { setShowUserMenu(false); navigateTo("admin"); }}
                      >
                        <span className="app__user-menu-icon">⚙️</span>
                        Администратор
                      </button>
                    )}
                    <div className="app__user-menu-divider" />
                    <button
                      className="app__user-menu-item app__user-menu-item--danger"
                      type="button"
                      role="menuitem"
                      onClick={handleLogout}
                    >
                      <span className="app__user-menu-icon">→</span>
                      Выйти
                    </button>
                  </div>
                )}
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
        {activeView === "search" && <SearchWorkspacePage onShowLogin={() => setShowLogin(true)} />}
        {activeView === "university" && (
          <UniversityDetailPage onShowLogin={() => setShowLogin(true)} />
        )}
        {activeView === "comparison" && (
          <ComparisonPage onShowLogin={() => setShowLogin(true)} />
        )}
        {activeView === "profile" && <ProfilePage onShowLogin={() => setShowLogin(true)} />}
        {activeView === "ratings" && <RatingsPage />}
        {activeView === "programs" && <ProgramsPage />}
        {activeView === "admin" && isAdmin && <AdminDashboard onLogout={handleLogout} />}
        {activeView === "admin" && !isAdmin && <SearchWorkspacePage onShowLogin={() => setShowLogin(true)} />}
      </main>

      {showLogin && (
        <LoginModal onClose={() => setShowLogin(false)} onSuccess={handleLoginSuccess} />
      )}
      <AiChatWidget />
    </div>
  );
}
