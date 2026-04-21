from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx

from apps.parser.adapters.official_sites import OfficialSiteAdapter
from apps.parser.app.crawl_requests import CrawlRequestConsumer, CrawlRequestProcessingService
from apps.parser.app.parse_completed import ParseCompletedEmitter, ParseCompletedPublisher
from apps.parser.app.parsed_documents import (
    ParsedDocumentPersistenceService,
    ParsedDocumentRepository,
)
from apps.parser.app.raw_artifacts import RawArtifactPersistenceService, RawArtifactRepository
from libs.source_sdk.fetchers import HttpFetcher, build_mock_http_client_factory
from libs.source_sdk.stores import MinIORawArtifactStore
from libs.storage import MinIOObjectWriteResult

FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "parser_ingestion"


class MappingResult:
    def __init__(self, *, row: dict[str, Any]) -> None:
        self._row = row

    def mappings(self) -> MappingResult:
        return self

    def one(self) -> dict[str, Any]:
        return self._row


class InMemoryRawArtifactSession:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], dict[str, Any]] = {}
        self.parsed_documents: dict[tuple[str, str], dict[str, Any]] = {}
        self.extracted_fragments: dict[str, dict[str, Any]] = {}
        self.executed: list[tuple[str, dict[str, Any]]] = []
        self.commit_count = 0

    def commit(self) -> None:
        self.commit_count += 1

    def execute(self, statement: str, params: dict[str, Any]) -> MappingResult:
        normalized_statement = " ".join(statement.split()).lower()
        self.executed.append((normalized_statement, params))
        if "insert into ingestion.raw_artifact" in normalized_statement:
            return MappingResult(row=self._upsert_raw_artifact(params))
        if "insert into parsing.parsed_document" in normalized_statement:
            return MappingResult(row=self._upsert_parsed_document(params))
        if "insert into parsing.extracted_fragment" in normalized_statement:
            return MappingResult(row=self._upsert_extracted_fragment(params))
        raise AssertionError(f"Unexpected statement: {statement}")

    def _upsert_raw_artifact(self, params: dict[str, Any]) -> dict[str, Any]:
        key = (params["source_key"], params["sha256"])
        existing = self.rows.get(key)
        if existing is None:
            row = dict(params)
        else:
            existing_metadata = json.loads(existing["metadata"])
            existing_metadata.update(json.loads(params["metadata"]))
            row = {
                **existing,
                **params,
                "raw_artifact_id": existing["raw_artifact_id"],
                "metadata": json.dumps(existing_metadata, ensure_ascii=False, sort_keys=True),
            }
        self.rows[key] = row
        return row

    def _upsert_parsed_document(self, params: dict[str, Any]) -> dict[str, Any]:
        key = (str(params["raw_artifact_id"]), params["parser_version"])
        existing = self.parsed_documents.get(key)
        if existing is None:
            row = dict(params)
        else:
            existing_metadata = json.loads(existing["metadata"])
            existing_metadata.update(json.loads(params["metadata"]))
            row = {
                **existing,
                **params,
                "parsed_document_id": existing["parsed_document_id"],
                "metadata": json.dumps(existing_metadata, ensure_ascii=False, sort_keys=True),
            }
        self.parsed_documents[key] = row
        return row

    def _upsert_extracted_fragment(self, params: dict[str, Any]) -> dict[str, Any]:
        key = str(params["fragment_id"])
        existing = self.extracted_fragments.get(key)
        if existing is None:
            row = dict(params)
        else:
            existing_metadata = json.loads(existing["metadata"])
            existing_metadata.update(json.loads(params["metadata"]))
            row = {
                **existing,
                **params,
                "metadata": json.dumps(existing_metadata, ensure_ascii=False, sort_keys=True),
            }
        self.extracted_fragments[key] = row
        return row


class InMemoryMinIOStorage:
    def __init__(self) -> None:
        self.ensure_bucket_calls: list[str] = []
        self.put_calls: list[dict[str, Any]] = []
        self.objects: dict[tuple[str, str], bytes] = {}

    def ensure_bucket(self, bucket_name: str) -> bool:
        self.ensure_bucket_calls.append(bucket_name)
        return False

    def put_bytes(
        self,
        *,
        bucket_name: str,
        object_name: str,
        payload: bytes,
        content_type: str,
        metadata: dict[str, str] | None = None,
    ) -> MinIOObjectWriteResult:
        self.put_calls.append(
            {
                "bucket_name": bucket_name,
                "object_name": object_name,
                "payload": payload,
                "content_type": content_type,
                "metadata": metadata,
            }
        )
        self.objects[(bucket_name, object_name)] = payload
        return MinIOObjectWriteResult(
            bucket_name=bucket_name,
            object_name=object_name,
            etag=f"etag-{len(self.put_calls)}",
            version_id=f"v{len(self.put_calls)}",
        )


class ParserIngestionHarness:
    def __init__(
        self,
        *,
        endpoint_url: str,
        response_body: bytes,
        content_type: str = "text/html; charset=utf-8",
        enable_official_parser: bool = False,
        parse_completed_publisher: ParseCompletedPublisher | None = None,
    ) -> None:
        self.raw_artifact_session = InMemoryRawArtifactSession()
        self.minio_storage = InMemoryMinIOStorage()
        self.requested_headers: list[httpx.Headers] = []
        self._endpoint_url = endpoint_url
        self._response_body = response_body
        self._content_type = content_type
        self._enable_official_parser = enable_official_parser
        self._parse_completed_publisher = parse_completed_publisher

    def build_consumer(self) -> CrawlRequestConsumer:
        fetcher = HttpFetcher(
            client_factory=build_mock_http_client_factory(self._http_handler),
            user_agent="parser-ingestion-harness/1.0",
        )
        raw_artifact_repository = RawArtifactRepository(
            self.raw_artifact_session,
            sql_text=lambda statement: statement,
        )
        raw_artifact_service = RawArtifactPersistenceService(
            raw_store=MinIORawArtifactStore(self.minio_storage),
            repository=raw_artifact_repository,
        )
        parsed_document_service = None
        source_adapters = ()
        if self._enable_official_parser:
            parsed_document_repository = ParsedDocumentRepository(
                self.raw_artifact_session,
                sql_text=lambda statement: statement,
            )
            parsed_document_service = ParsedDocumentPersistenceService(
                parsed_document_repository
            )
            source_adapters = (OfficialSiteAdapter(fetcher=fetcher),)
        processing_service = CrawlRequestProcessingService(
            fetcher=fetcher,
            raw_artifact_service=raw_artifact_service,
            parsed_document_service=parsed_document_service,
            source_adapters=source_adapters,
            parse_completed_emitter=(
                ParseCompletedEmitter(publisher=self._parse_completed_publisher)
                if self._parse_completed_publisher is not None
                else None
            ),
        )
        return CrawlRequestConsumer(service=processing_service)

    def _http_handler(self, request: httpx.Request) -> httpx.Response:
        self.requested_headers.append(request.headers)
        if str(request.url) != self._endpoint_url:
            return httpx.Response(
                status_code=404,
                headers={"content-type": "text/html"},
                content=b"not found",
                request=request,
            )
        return httpx.Response(
            status_code=200,
            headers={
                "content-type": self._content_type,
                "etag": '"fixture-etag"',
                "last-modified": "Mon, 20 Apr 2026 12:00:00 GMT",
            },
            content=self._response_body,
            request=request,
        )


def load_json_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def load_bytes_fixture(name: str) -> bytes:
    return (FIXTURE_ROOT / name).read_bytes()
