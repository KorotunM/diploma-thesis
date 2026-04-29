from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from apps.parser.app.raw_artifacts import RawArtifactPersistenceService, RawArtifactRepository
from apps.scheduler.app.sources.endpoint_repository import SourceEndpointRepository
from apps.scheduler.app.sources.models import (
    CreateSourceEndpointRequest,
    CreateSourceRequest,
    UpdateSourceEndpointRequest,
    UpdateSourceRequest,
)
from apps.scheduler.app.sources.repository import SourceRepository
from libs.source_sdk import FetchContext, FetchedArtifact
from libs.source_sdk.stores import MinIORawArtifactStore
from libs.storage import get_minio_storage, get_postgres_session_factory
from scripts.mvp_fixtures import FixtureBundleEntry, FixtureBundleManifest
from scripts.replay import (
    NormalizationReplayService,
    ParserReplayService,
    build_normalization_replay_service,
    build_parser_replay_service,
)


class FixtureBackfillError(ValueError):
    pass


class FixtureBackfillEntryResult:
    def __init__(
        self,
        *,
        fixture_id: str,
        source_key: str,
        raw_artifact_id: str,
        parsed_document_id: str,
        university_id: str,
        card_version: int,
    ) -> None:
        self.fixture_id = fixture_id
        self.source_key = source_key
        self.raw_artifact_id = raw_artifact_id
        self.parsed_document_id = parsed_document_id
        self.university_id = university_id
        self.card_version = card_version

    def to_dict(self) -> dict[str, Any]:
        return {
            "fixture_id": self.fixture_id,
            "source_key": self.source_key,
            "raw_artifact_id": self.raw_artifact_id,
            "parsed_document_id": self.parsed_document_id,
            "university_id": self.university_id,
            "card_version": self.card_version,
        }


class FixtureBackfillResult:
    def __init__(self, *, bundle_name: str, items: list[FixtureBackfillEntryResult]) -> None:
        self.bundle_name = bundle_name
        self.items = items

    def to_dict(self) -> dict[str, Any]:
        return {
            "bundle_name": self.bundle_name,
            "items": [item.to_dict() for item in self.items],
        }


class SchedulerRegistryGateway:
    def __init__(
        self,
        *,
        session: Any,
        source_repository: SourceRepository,
        endpoint_repository: SourceEndpointRepository,
    ) -> None:
        self._session = session
        self._source_repository = source_repository
        self._endpoint_repository = endpoint_repository

    def ensure_entry(self, entry: FixtureBundleEntry) -> None:
        source = self._source_repository.get_by_key(entry.source_key)
        source_metadata = {
            "fixture_bundle": entry.fixture_id,
            "fixture_backfill": True,
        }
        if source is None:
            self._source_repository.create(
                CreateSourceRequest(
                    source_key=entry.source_key,
                    source_type=entry.source_type,
                    trust_tier=entry.trust_tier,
                    is_active=True,
                    metadata=source_metadata,
                )
            )
        else:
            self._source_repository.update(
                entry.source_key,
                UpdateSourceRequest(
                    source_type=entry.source_type,
                    trust_tier=entry.trust_tier,
                    is_active=True,
                    metadata={
                        **source.metadata,
                        **source_metadata,
                    },
                ),
            )

        endpoint = self._endpoint_repository.get_by_url(entry.source_key, entry.endpoint_url)
        request = CreateSourceEndpointRequest(
            endpoint_url=entry.endpoint_url,
            parser_profile=entry.parser_profile,
            crawl_policy=entry.crawl_policy,
        )
        if endpoint is None:
            self._endpoint_repository.create(entry.source_key, request)
        else:
            self._endpoint_repository.update(
                entry.source_key,
                endpoint.endpoint_id,
                UpdateSourceEndpointRequest(
                    endpoint_url=entry.endpoint_url,
                    parser_profile=entry.parser_profile,
                    crawl_policy=entry.crawl_policy,
                ),
            )
        self._session.commit()


class FixtureRawImporter:
    def __init__(self, service: RawArtifactPersistenceService) -> None:
        self._service = service

    async def import_entry(
        self,
        *,
        entry: FixtureBundleEntry,
        payload: bytes,
    ):
        context = FetchContext(
            crawl_run_id=entry.crawl_run_id,
            source_key=entry.source_key,
            endpoint_url=entry.endpoint_url,
            priority=entry.priority,
            trigger="replay",
            parser_profile=entry.parser_profile,
            requested_at=entry.requested_at,
            render_mode=entry.crawl_policy.render_mode,
            timeout_seconds=entry.crawl_policy.timeout_seconds,
            max_retries=entry.crawl_policy.max_retries,
            retry_backoff_seconds=entry.crawl_policy.retry_backoff_seconds,
            respect_robots_txt=entry.crawl_policy.respect_robots_txt,
            allowed_content_types=entry.crawl_policy.allowed_content_types,
            request_headers=entry.crawl_policy.request_headers,
            metadata={
                "fixture_id": entry.fixture_id,
                "fixture_backfill": True,
                "fixture_sha256": entry.sha256,
            },
        )
        artifact = FetchedArtifact(
            raw_artifact_id=entry.raw_artifact_id,
            crawl_run_id=entry.crawl_run_id,
            source_key=entry.source_key,
            source_url=entry.endpoint_url,
            final_url=entry.final_url or entry.endpoint_url,
            http_status=entry.http_status,
            content_type=entry.content_type,
            response_headers=entry.response_headers,
            content_length=len(payload),
            sha256=hashlib.sha256(payload).hexdigest(),
            fetched_at=entry.fetched_at,
            render_mode=entry.crawl_policy.render_mode,
            etag=entry.etag,
            last_modified=entry.last_modified,
            content=payload,
            metadata={
                "fixture_id": entry.fixture_id,
                "fixture_backfill": True,
            },
        )
        stored_artifact, raw_record = await self._service.persist_after_successful_fetch(
            context=context,
            artifact=artifact,
        )
        if stored_artifact.sha256 != entry.sha256:
            raise FixtureBackfillError(
                f"Fixture {entry.fixture_id} sha256 mismatch after import."
            )
        return raw_record


class FixturePayloadLoader:
    def load(self, *, manifest_path: Path, entry: FixtureBundleEntry) -> bytes:
        payload_path = manifest_path.parent / entry.normalized_fixture_file
        return payload_path.read_bytes()


class FixtureBackfillService:
    def __init__(
        self,
        *,
        registry_gateway: SchedulerRegistryGateway,
        payload_loader: FixturePayloadLoader,
        raw_importer: FixtureRawImporter,
        parser_replay_service: ParserReplayService,
        normalization_replay_service: NormalizationReplayService,
    ) -> None:
        self._registry_gateway = registry_gateway
        self._payload_loader = payload_loader
        self._raw_importer = raw_importer
        self._parser_replay_service = parser_replay_service
        self._normalization_replay_service = normalization_replay_service

    async def backfill_manifest(self, manifest_path: Path) -> FixtureBackfillResult:
        manifest = FixtureBundleManifest.read(manifest_path)
        items: list[FixtureBackfillEntryResult] = []
        for entry in manifest.entries:
            self._registry_gateway.ensure_entry(entry)
            payload = self._payload_loader.load(manifest_path=manifest_path, entry=entry)
            raw_record = await self._raw_importer.import_entry(entry=entry, payload=payload)
            parse_result = await self._parser_replay_service.replay(raw_record.raw_artifact_id)
            normalize_result = self._normalization_replay_service.replay(
                parse_result.parsed_document.parsed_document_id
            )
            items.append(
                FixtureBackfillEntryResult(
                    fixture_id=entry.fixture_id,
                    source_key=entry.source_key,
                    raw_artifact_id=str(raw_record.raw_artifact_id),
                    parsed_document_id=str(parse_result.parsed_document.parsed_document_id),
                    university_id=str(
                        normalize_result.bootstrap_result.university.university_id
                    ),
                    card_version=normalize_result.projection_result.card_version.card_version,
                )
            )
        return FixtureBackfillResult(bundle_name=manifest.bundle_name, items=items)


def build_fixture_backfill_service(
    *,
    scheduler_session,
    parser_session,
    normalizer_session,
) -> FixtureBackfillService:
    storage = get_minio_storage(service_name="parser")
    raw_repository = RawArtifactRepository(parser_session)
    raw_importer = FixtureRawImporter(
        RawArtifactPersistenceService(
            raw_store=MinIORawArtifactStore(storage),
            repository=raw_repository,
        )
    )
    return FixtureBackfillService(
        registry_gateway=SchedulerRegistryGateway(
            session=scheduler_session,
            source_repository=SourceRepository(scheduler_session),
            endpoint_repository=SourceEndpointRepository(scheduler_session),
        ),
        payload_loader=FixturePayloadLoader(),
        raw_importer=raw_importer,
        parser_replay_service=build_parser_replay_service(parser_session),
        normalization_replay_service=build_normalization_replay_service(normalizer_session),
    )


@contextmanager
def managed_session(service_name: str):
    session_factory = get_postgres_session_factory(service_name=service_name)
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.backfill",
        description="Import a fixture bundle and rebuild parser/normalizer projections.",
    )
    parser.add_argument("manifest_path")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    manifest_path = Path(args.manifest_path)
    with managed_session("scheduler") as scheduler_session, managed_session(
        "parser"
    ) as parser_session, managed_session("normalizer") as normalizer_session:
        service = build_fixture_backfill_service(
            scheduler_session=scheduler_session,
            parser_session=parser_session,
            normalizer_session=normalizer_session,
        )
        result = asyncio.run(service.backfill_manifest(manifest_path))
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
        return 0
