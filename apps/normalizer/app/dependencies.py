from libs.storage import (
    RabbitMQConsumer,
    RabbitMQPublisher,
    declare_rabbitmq_topology,
    get_platform_settings,
    get_postgres_session_factory,
    get_rabbitmq_connection,
)

from .cards import UniversityCardProjectionRepository, UniversityCardProjectionService
from .card_updated import CardUpdatedEmitter
from .claims import ClaimBuildRepository, ClaimBuildService
from .facts import ResolvedFactGenerationService, ResolvedFactRepository
from .parse_completed import ParseCompletedProcessingService
from .resolution import FieldResolutionPolicyMatrix
from .review_required import ReviewRequiredEmitter
from .search_docs import (
    UniversitySearchDocProjectionRepository,
    UniversitySearchDocProjectionService,
)
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


def create_card_updated_emitter() -> CardUpdatedEmitter:
    settings = get_platform_settings(service_name="normalizer")
    return CardUpdatedEmitter(
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
    return UniversityCardProjectionService(
        UniversityCardProjectionRepository(session),
        search_doc_service=UniversitySearchDocProjectionService(
            UniversitySearchDocProjectionRepository(session)
        ),
    )


def create_parse_completed_processing_service(
    session,
) -> ParseCompletedProcessingService:
    return ParseCompletedProcessingService(
        claim_build_service=create_claim_build_service(session),
        university_bootstrap_service=create_university_bootstrap_service(session),
        resolved_fact_generation_service=create_resolved_fact_generation_service(session),
        university_card_projection_service=create_university_card_projection_service(
            session
        ),
        card_updated_emitter=create_card_updated_emitter(),
    )


def create_normalizer_rabbitmq_consumer() -> RabbitMQConsumer:
    settings = get_platform_settings(service_name="normalizer")
    connection = get_rabbitmq_connection(service_name="normalizer")
    declare_rabbitmq_topology(connection)
    return RabbitMQConsumer(connection, settings.rabbitmq)
