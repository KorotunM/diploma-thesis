from __future__ import annotations

from uuid import UUID

from fastapi import Depends, Header

from libs.storage import get_postgres_session_factory

from .admin_pipeline import AdminPipelineService
from .admin_review import ReviewCaseRepository, ReviewCaseService
from .ai import AiChatService
from .auth import AuthRepository, AuthService
from .cards import UniversityCardReadRepository, UniversityCardReadService
from .locations import LocationSuggestRepository, LocationSuggestService
from .programs import ProgramDirectoryRepository, ProgramDirectoryService
from .provenance import UniversityProvenanceReadService, UniversityProvenanceRepository
from .rankings import RankingsRepository, RankingsService
from .search import UniversitySearchRepository, UniversitySearchService
from .subjects import EgeSubjectRepository
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


def get_location_suggest_service(
    session=BACKEND_SESSION_DEPENDENCY,
) -> LocationSuggestService:
    return LocationSuggestService(LocationSuggestRepository(session))


def get_rankings_service(
    session=BACKEND_SESSION_DEPENDENCY,
) -> RankingsService:
    return RankingsService(RankingsRepository(session))


def get_program_directory_service(
    session=BACKEND_SESSION_DEPENDENCY,
) -> ProgramDirectoryService:
    return ProgramDirectoryService(ProgramDirectoryRepository(session))


def get_review_case_service(
    session=BACKEND_SESSION_DEPENDENCY,
) -> ReviewCaseService:
    return ReviewCaseService(ReviewCaseRepository(session))


def get_auth_service(session=BACKEND_SESSION_DEPENDENCY) -> AuthService:
    return AuthService(AuthRepository(session))


def get_user_service(session=BACKEND_SESSION_DEPENDENCY) -> UserService:
    return UserService(UserRepository(session))


def get_ege_subject_repository(
    session=BACKEND_SESSION_DEPENDENCY,
) -> EgeSubjectRepository:
    return EgeSubjectRepository(session)


def get_ai_chat_service() -> AiChatService:
    return AiChatService()


def get_admin_pipeline_service() -> AdminPipelineService:
    return AdminPipelineService()


def get_optional_user_id(
    authorization: str | None = Header(default=None),
) -> UUID | None:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization.removeprefix("Bearer ").strip()
    if not token:
        return None
    session_factory = get_postgres_session_factory(service_name="backend")
    session = session_factory()
    try:
        auth_repo = AuthRepository(session)
        user = auth_repo.find_user_by_token(token)
        return user.user_id if user else None
    finally:
        session.close()


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
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token.",
        )
    return user.user_id


def get_bearer_token(
    authorization: str | None = Header(default=None),
) -> str | None:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    return authorization.removeprefix("Bearer ").strip() or None
