from libs.storage import (
    RabbitMQPublisher,
    get_platform_settings,
    get_postgres_session_factory,
    get_rabbitmq_connection,
)

from .cards import UniversityCardProjectionRepository, UniversityCardProjectionService
from .claims import ClaimBuildRepository, ClaimBuildService
from .facts import ResolvedFactGenerationService, ResolvedFactRepository
from .review_required import ReviewRequiredEmitter
from .resolution import FieldResolutionPolicyMatrix
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


def create_review_required_emitter() -> ReviewRequiredEmitter:
    settings = get_platform_settings(service_name="normalizer")
    return ReviewRequiredEmitter(
        publisher=RabbitMQPublisher(
            get_rabbitmq_connection(service_name="normalizer"),
            settings.rabbitmq,
        )
    )


def create_university_bootstrap_service(session) -> UniversityBootstrapService:
    return UniversityBootstrapService(
        UniversityBootstrapRepository(session),
        policy_matrix=FieldResolutionPolicyMatrix(),
        review_required_emitter=create_review_required_emitter(),
    )


def create_resolved_fact_generation_service(session) -> ResolvedFactGenerationService:
    return ResolvedFactGenerationService(
        ResolvedFactRepository(session),
        policy_matrix=FieldResolutionPolicyMatrix(),
    )


def create_university_card_projection_service(session) -> UniversityCardProjectionService:
    return UniversityCardProjectionService(UniversityCardProjectionRepository(session))
