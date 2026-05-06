import { useState } from "react";
import { useAuth } from "../shared/auth";

interface LoginModalProps {
  onClose: () => void;
  onSuccess: () => void;
}

type Mode = "login" | "register";

export function LoginModal({ onClose, onSuccess }: LoginModalProps) {
  const { login, register, loading } = useAuth();
  const [mode, setMode] = useState<Mode>("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    setError(null);
    try {
      if (mode === "login") {
        await login(email, password);
      } else {
        await register(email, password, displayName || undefined);
      }
      onSuccess();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Произошла ошибка.");
    }
  };

  const switchMode = (m: Mode) => {
    setMode(m);
    setError(null);
  };

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal__header">
          <h2 className="modal__title">
            {mode === "login" ? "Вход" : "Регистрация"}
          </h2>
          <button className="modal__close" type="button" onClick={onClose} aria-label="Закрыть">
            ✕
          </button>
        </div>

        <div className="modal__tabs">
          <button
            className={`modal__tab${mode === "login" ? " modal__tab--active" : ""}`}
            type="button"
            onClick={() => switchMode("login")}
          >
            Вход
          </button>
          <button
            className={`modal__tab${mode === "register" ? " modal__tab--active" : ""}`}
            type="button"
            onClick={() => switchMode("register")}
          >
            Регистрация
          </button>
        </div>

        <form className="modal__body" onSubmit={handleSubmit}>
          {error && <p className="modal__error">{error}</p>}

          {mode === "register" && (
            <label className="field">
              <span className="field__label">Имя (необязательно)</span>
              <input
                className="field__control"
                type="text"
                autoComplete="name"
                value={displayName}
                onChange={(e) => { setDisplayName(e.target.value); setError(null); }}
                placeholder="Иван Иванов"
              />
            </label>
          )}

          <label className="field">
            <span className="field__label">Email</span>
            <input
              className="field__control"
              type="email"
              autoComplete={mode === "login" ? "username" : "email"}
              value={email}
              onChange={(e) => { setEmail(e.target.value); setError(null); }}
              placeholder="you@example.com"
              required
            />
          </label>

          <label className="field">
            <span className="field__label">Пароль</span>
            <input
              className="field__control"
              type="password"
              autoComplete={mode === "login" ? "current-password" : "new-password"}
              value={password}
              onChange={(e) => { setPassword(e.target.value); setError(null); }}
              placeholder="••••••"
              required
              minLength={6}
            />
          </label>

          <button className="modal__submit" type="submit" disabled={loading}>
            {loading
              ? "Загружаем…"
              : mode === "login"
              ? "Войти"
              : "Создать аккаунт"}
          </button>
        </form>
      </div>
    </div>
  );
}
