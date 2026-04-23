from fastapi import Depends

from libs.storage import get_postgres_session_factory

from .cards import UniversityCardReadRepository, UniversityCardReadService


def get_backend_session():
    session_factory = get_postgres_session_factory(service_name="backend")
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


def create_university_card_read_service(session) -> UniversityCardReadService:
    return UniversityCardReadService(UniversityCardReadRepository(session))


BACKEND_SESSION_DEPENDENCY = Depends(get_backend_session)


def get_university_card_read_service(
    session=BACKEND_SESSION_DEPENDENCY,
) -> UniversityCardReadService:
    return create_university_card_read_service(session)
