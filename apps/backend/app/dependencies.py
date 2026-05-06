from __future__ import annotations

from fastapi import Depends, Header
from uuid import UUID

from libs.storage import get_postgres_session_factory

from .auth import AuthRepository, AuthService
from .cards import UniversityCardReadRepository, UniversityCardReadService
from .provenance import UniversityProvenanceReadService, UniversityProvenanceRepository
from .search import UniversitySearchRepository, UniversitySearchService
from .user import UserRepository, UserService


def get_backend_session():
    session_factory = get_postgres_session_factory(service_name="backend")
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


BACKEND_SESSION_DEPENDENCY = Depends(get_backend_session)


def get_university_card_read_service(
    session=BACKEND_SESSION_DEPENDENCY,
) -> UniversityCardReadService:
    return UniversityCardReadService(UniversityCardReadRepository(session))


def get_university_provenance_read_service(
    session=BACKEND_SESSION_DEPENDENCY,
) -> UniversityProvenanceReadService:
    return UniversityProvenanceReadService(UniversityProvenanceRepository(session))


def get_university_search_service(
    session=BACKEND_SESSION_DEPENDENCY,
) -> UniversitySearchService:
    return UniversitySearchService(UniversitySearchRepository(session))


def get_auth_service(session=BACKEND_SESSION_DEPENDENCY) -> AuthService:
    return AuthService(AuthRepository(session))


def get_user_service(session=BACKEND_SESSION_DEPENDENCY) -> UserService:
    return UserService(UserRepository(session))


def get_optional_user_id(
    authorization: str | None = Header(default=None),
    session=BACKEND_SESSION_DEPENDENCY,
) -> UUID | None:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization.removeprefix("Bearer ").strip()
    if not token:
        return None
    auth_repo = AuthRepository(session)
    user = auth_repo.find_user_by_token(token)
    return user.user_id if user else None


def get_required_user_id(
    authorization: str | None = Header(default=None),
    session=BACKEND_SESSION_DEPENDENCY,
) -> UUID:
    from fastapi import HTTPException, status

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated.")
    token = authorization.removeprefix("Bearer ").strip()
    auth_repo = AuthRepository(session)
    user = auth_repo.find_user_by_token(token)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token.")
    return user.user_id


def get_bearer_token(
    authorization: str | None = Header(default=None),
) -> str | None:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    return authorization.removeprefix("Bearer ").strip() or None
