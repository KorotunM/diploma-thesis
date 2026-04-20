from libs.source_sdk.fetchers import HttpFetcher
from libs.source_sdk.stores import MinIORawArtifactStore
from libs.storage import get_minio_storage, get_postgres_session_factory

from .crawl_requests import CrawlRequestProcessingService
from .raw_artifacts import RawArtifactPersistenceService, RawArtifactRepository


def get_parser_session():
    session_factory = get_postgres_session_factory(service_name="parser")
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def create_crawl_request_processing_service(session) -> CrawlRequestProcessingService:
    raw_artifact_repository = RawArtifactRepository(session)
    raw_artifact_service = RawArtifactPersistenceService(
        raw_store=MinIORawArtifactStore(get_minio_storage(service_name="parser")),
        repository=raw_artifact_repository,
    )
    return CrawlRequestProcessingService(
        fetcher=HttpFetcher(),
        raw_artifact_service=raw_artifact_service,
    )
