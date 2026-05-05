import { useState } from "react";

interface LoginModalProps {
  onClose: () => void;
  onSuccess: () => void;
}

export function LoginModal({ onClose, onSuccess }: LoginModalProps) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = (event: React.FormEvent) => {
    event.preventDefault();
    if (username === "admin" && password === "admin") {
      localStorage.setItem("admin_auth", "1");
      onSuccess();
    } else {
      setError("Неверный логин или пароль.");
    }
  };

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal__header">
          <h2 className="modal__title">Вход в систему</h2>
          <button className="modal__close" type="button" onClick={onClose} aria-label="Закрыть">
            ✕
          </button>
        </div>
        <form className="modal__body" onSubmit={handleSubmit}>
          {error && <p className="modal__error">{error}</p>}
          <label className="field">
            <span className="field__label">Логин</span>
            <input
              className="field__control"
              type="text"
              autoComplete="username"
              value={username}
              onChange={(e) => {
                setUsername(e.target.value);
                setError(null);
              }}
              placeholder="admin"
            />
          </label>
          <label className="field">
            <span className="field__label">Пароль</span>
            <input
              className="field__control"
              type="password"
              autoComplete="current-password"
              value={password}
              onChange={(e) => {
                setPassword(e.target.value);
                setError(null);
              }}
              placeholder="••••••"
            />
          </label>
          <button className="modal__submit" type="submit">
            Войти
          </button>
          <p className="modal__hint">demo: admin / admin</p>
        </form>
      </div>
    </div>
  );
}
