from apps.parser.adapters.official_sites import OfficialSiteAdapter
from libs.source_sdk.fetchers import HttpFetcher
from libs.source_sdk.stores import MinIORawArtifactStore
from libs.storage import get_minio_storage, get_postgres_session_factory

from .crawl_requests import CrawlRequestProcessingService
from .parsed_documents import ParsedDocumentPersistenceService, ParsedDocumentRepository
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
    fetcher = HttpFetcher()
    raw_store = MinIORawArtifactStore(get_minio_storage(service_name="parser"))
    raw_artifact_repository = RawArtifactRepository(session)
    raw_artifact_service = RawArtifactPersistenceService(
        raw_store=raw_store,
        repository=raw_artifact_repository,
    )
    parsed_document_repository = ParsedDocumentRepository(session)
    parsed_document_service = ParsedDocumentPersistenceService(parsed_document_repository)
    return CrawlRequestProcessingService(
        fetcher=fetcher,
        raw_artifact_service=raw_artifact_service,
        parsed_document_service=parsed_document_service,
        source_adapters=(
            OfficialSiteAdapter(fetcher=fetcher, raw_store=raw_store),
        ),
    )
