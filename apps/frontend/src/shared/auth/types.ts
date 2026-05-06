export interface User {
  user_id: string;
  email: string;
  display_name: string | null;
}

export interface AuthState {
  user: User | null;
  token: string | null;
  loading: boolean;
}
