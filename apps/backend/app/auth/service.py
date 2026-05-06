from __future__ import annotations

import hashlib
import secrets

from .models import AuthResponse, CurrentUserResponse
from .repository import AuthRepository


class EmailAlreadyTakenError(ValueError):
    pass


class InvalidCredentialsError(ValueError):
    pass


class AuthService:
    def __init__(self, repository: AuthRepository) -> None:
        self._repo = repository

    def register(
        self,
        *,
        email: str,
        password: str,
        display_name: str | None = None,
    ) -> AuthResponse:
        existing = self._repo.find_user_by_email(email)
        if existing is not None:
            raise EmailAlreadyTakenError(f"Email {email!r} is already registered.")
        password_hash = _hash_password(password)
        user = self._repo.create_user(
            email=email,
            password_hash=password_hash,
            display_name=display_name,
        )
        token = self._repo.create_session(user.user_id)
        return AuthResponse(
            token=token,
            user_id=str(user.user_id),
            email=user.email,
            display_name=user.display_name,
        )

    def login(self, *, email: str, password: str) -> AuthResponse:
        user = self._repo.find_user_by_email(email)
        if user is None or not _verify_password(password, user.password_hash):
            raise InvalidCredentialsError("Invalid email or password.")
        token = self._repo.create_session(user.user_id)
        return AuthResponse(
            token=token,
            user_id=str(user.user_id),
            email=user.email,
            display_name=user.display_name,
        )

    def logout(self, token: str) -> None:
        self._repo.delete_session_by_token(token)

    def get_current_user(self, token: str) -> CurrentUserResponse:
        user = self._repo.find_user_by_token(token)
        if user is None:
            raise InvalidCredentialsError("Session not found or expired.")
        return CurrentUserResponse(
            user_id=str(user.user_id),
            email=user.email,
            display_name=user.display_name,
        )

    def resolve_user_id(self, token: str) -> str | None:
        user = self._repo.find_user_by_token(token)
        return str(user.user_id) if user else None


_ITERATIONS = 260_000


def _hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), _ITERATIONS)
    return f"pbkdf2:sha256:{_ITERATIONS}:{salt}:{dk.hex()}"


def _verify_password(password: str, stored: str) -> bool:
    try:
        _, algo, iter_str, salt, stored_hex = stored.split(":")
        iterations = int(iter_str)
        dk = hashlib.pbkdf2_hmac(algo, password.encode(), salt.encode(), iterations)
        return secrets.compare_digest(dk.hex(), stored_hex)
    except Exception:
        return False
