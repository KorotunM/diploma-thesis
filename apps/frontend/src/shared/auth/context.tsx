import { createContext, useCallback, useContext, useEffect, useState } from "react";
import type { ReactNode } from "react";

import type { User } from "./types";

const TOKEN_KEY = "auth_token";
const USER_KEY = "auth_user";

interface AuthContextValue {
  user: User | null;
  token: string | null;
  loading: boolean;
  login: (email: string, password: string) => Promise<void>;
  register: (email: string, password: string, displayName?: string) => Promise<void>;
  logout: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({
  children,
  backendBaseUrl,
}: {
  children: ReactNode;
  backendBaseUrl: string;
}) {
  const [token, setToken] = useState<string | null>(() => localStorage.getItem(TOKEN_KEY));
  const [user, setUser] = useState<User | null>(() => {
    const raw = localStorage.getItem(USER_KEY);
    if (!raw) return null;
    try {
      return JSON.parse(raw) as User;
    } catch {
      return null;
    }
  });
  const [loading, setLoading] = useState(false);

  const base = backendBaseUrl.replace(/\/$/, "");

  const persistAuth = (newToken: string, newUser: User) => {
    localStorage.setItem(TOKEN_KEY, newToken);
    localStorage.setItem(USER_KEY, JSON.stringify(newUser));
    setToken(newToken);
    setUser(newUser);
  };

  const clearAuth = () => {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(USER_KEY);
    setToken(null);
    setUser(null);
  };

  useEffect(() => {
    if (!token) return;
    fetch(`${base}/api/v1/auth/me`, {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then((r) => (r.ok ? r.json() : null))
      .then((data) => {
        if (data) {
          setUser(data as User);
          localStorage.setItem(USER_KEY, JSON.stringify(data));
        } else {
          clearAuth();
        }
      })
      .catch(() => clearAuth());
  }, []);

  const login = useCallback(
    async (email: string, password: string) => {
      setLoading(true);
      try {
        const res = await fetch(`${base}/api/v1/auth/login`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ email, password }),
        });
        if (!res.ok) {
          const err = await res.json().catch(() => null);
          throw new Error((err as { detail?: string })?.detail ?? "Ошибка входа.");
        }
        const data = await res.json();
        persistAuth(data.token, {
          user_id: data.user_id,
          email: data.email,
          display_name: data.display_name,
        });
      } finally {
        setLoading(false);
      }
    },
    [base],
  );

  const register = useCallback(
    async (email: string, password: string, displayName?: string) => {
      setLoading(true);
      try {
        const res = await fetch(`${base}/api/v1/auth/register`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ email, password, display_name: displayName ?? null }),
        });
        if (!res.ok) {
          const err = await res.json().catch(() => null);
          throw new Error((err as { detail?: string })?.detail ?? "Ошибка регистрации.");
        }
        const data = await res.json();
        persistAuth(data.token, {
          user_id: data.user_id,
          email: data.email,
          display_name: data.display_name,
        });
      } finally {
        setLoading(false);
      }
    },
    [base],
  );

  const logout = useCallback(async () => {
    if (token) {
      await fetch(`${base}/api/v1/auth/logout`, {
        method: "POST",
        headers: { Authorization: `Bearer ${token}` },
      }).catch(() => {});
    }
    clearAuth();
  }, [token, base]);

  return (
    <AuthContext.Provider value={{ user, token, loading, login, register, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}
