from libs.storage import get_postgres_session_factory

from .claims import ClaimBuildRepository, ClaimBuildService
from .facts import ResolvedFactGenerationService, ResolvedFactRepository
from .universities import UniversityBootstrapRepository, UniversityBootstrapService


def get_normalizer_session():
    session_factory = get_postgres_session_factory(service_name="normalizer")
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def create_claim_build_service(session) -> ClaimBuildService:
    return ClaimBuildService(ClaimBuildRepository(session))


def create_university_bootstrap_service(session) -> UniversityBootstrapService:
    return UniversityBootstrapService(UniversityBootstrapRepository(session))


def create_resolved_fact_generation_service(session) -> ResolvedFactGenerationService:
    return ResolvedFactGenerationService(ResolvedFactRepository(session))
