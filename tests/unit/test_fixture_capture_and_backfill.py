from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from apps.parser.app.raw_artifacts import RawArtifactRecord
from scripts.backfill.workflow import (
    FixtureBackfillService,
    FixturePayloadLoader,
)
from scripts.fixture_capture.workflow import FixtureCaptureService
from scripts.mvp_fixtures import FixtureBundleEntry, FixtureBundleManifest, default_mvp_source_specs


class FakeRawArtifactRepository:
    def __init__(self, records: dict[UUID, RawArtifactRecord]) -> None:
        self.records = records

    def get_by_id(self, raw_artifact_id: UUID) -> RawArtifactRecord | None:
        return self.records.get(raw_artifact_id)


class FakeStorage:
    def __init__(self, payloads: dict[tuple[str, str], bytes]) -> None:
        self.payloads = payloads
        self.calls: list[tuple[str, str]] = []

    def get_bytes(self, *, bucket_name: str, object_name: str) -> bytes:
        self.calls.append((bucket_name, object_name))
        return self.payloads[(bucket_name, object_name)]


class FakeRegistryGateway:
    def __init__(self) -> None:
        self.entries: list[FixtureBundleEntry] = []

    def ensure_entry(self, entry: FixtureBundleEntry) -> None:
        self.entries.append(entry)


class FakeRawImporter:
    def __init__(self) -> None:
        self.calls: list[tuple[FixtureBundleEntry, bytes]] = []

    async def import_entry(self, *, entry: FixtureBundleEntry, payload: bytes):
        self.calls.append((entry, payload))
        return SimpleNamespace(raw_artifact_id=entry.raw_artifact_id)


class FakeParserReplayService:
    def __init__(self) -> None:
        self.calls: list[UUID] = []

    async def replay(self, raw_artifact_id: UUID):
        self.calls.append(raw_artifact_id)
        return SimpleNamespace(
            parsed_document=SimpleNamespace(parsed_document_id=uuid4())
        )


class FakeNormalizationReplayService:
    def __init__(self) -> None:
        self.calls: list[UUID] = []

    def replay(self, parsed_document_id: UUID):
        self.calls.append(parsed_document_id)
        return SimpleNamespace(
            bootstrap_result=SimpleNamespace(
                university=SimpleNamespace(university_id=uuid4())
            ),
            projection_result=SimpleNamespace(
                card_version=SimpleNamespace(card_version=1)
            ),
        )


def build_raw_record(*, spec, raw_artifact_id: UUID, payload: bytes) -> RawArtifactRecord:
    return RawArtifactRecord(
        raw_artifact_id=raw_artifact_id,
        crawl_run_id=uuid4(),
        source_key=spec.source_key,
        source_url=spec.endpoint_url,
        final_url=spec.endpoint_url,
        http_status=200,
        content_type=spec.content_type,
        content_length=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        storage_bucket="raw-json" if spec.content_type == "application/json" else "raw-html",
        storage_object_key=f"{spec.source_key}/{spec.fixture_file_name}",
        etag=f"etag-{spec.fixture_id}",
        last_modified="Tue, 29 Apr 2026 09:00:00 GMT",
        fetched_at=datetime(2026, 4, 29, 9, 0, tzinfo=UTC),
        metadata={
            "parser_profile": spec.parser_profile,
            "requested_at": "2026-04-29T08:59:00+00:00",
            "response_headers": {"content-type": spec.content_type},
        },
    )


def test_fixture_capture_service_writes_bundle_for_three_mvp_sources(tmp_path: Path) -> None:
    specs = default_mvp_source_specs()
    payloads_by_id = {
        "official": b"<html>official</html>",
        "aggregator": b'{"kind":"aggregator"}',
        "ranking": b'{"kind":"ranking"}',
    }
    records: dict[UUID, RawArtifactRecord] = {}
    payloads: dict[tuple[str, str], bytes] = {}
    raw_ids_by_fixture_id: dict[str, UUID] = {}
    for spec in specs:
        raw_artifact_id = uuid4()
        payload = payloads_by_id[spec.fixture_id]
        record = build_raw_record(
            spec=spec,
            raw_artifact_id=raw_artifact_id,
            payload=payload,
        )
        records[raw_artifact_id] = record
        payloads[(record.storage_bucket, record.storage_object_key)] = payload
        raw_ids_by_fixture_id[spec.fixture_id] = raw_artifact_id

    service = FixtureCaptureService(
        raw_artifact_repository=FakeRawArtifactRepository(records),
        storage=FakeStorage(payloads),
        output_root=tmp_path,
    )

    manifest_path = service.capture_mvp_bundle(
        bundle_name="capture-demo",
        raw_artifact_ids_by_fixture_id=raw_ids_by_fixture_id,
    )

    assert manifest_path == tmp_path / "capture-demo" / "manifest.json"
    manifest = FixtureBundleManifest.read(manifest_path)
    assert manifest.bundle_name == "capture-demo"
    assert [entry.fixture_id for entry in manifest.entries] == [
        "official",
        "aggregator",
        "ranking",
    ]
    for entry in manifest.entries:
        payload = payloads_by_id[entry.fixture_id]
        assert (manifest_path.parent / entry.fixture_file).read_bytes() == payload
        assert entry.content_length == len(payload)


@pytest.mark.asyncio
async def test_fixture_backfill_service_imports_and_replays_all_mvp_entries(
    tmp_path: Path,
) -> None:
    specs = default_mvp_source_specs()
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir(parents=True)
    entries: list[FixtureBundleEntry] = []
    payloads_by_fixture_id: dict[str, bytes] = {}
    for spec in specs:
        payload = json.dumps({"fixture_id": spec.fixture_id}).encode("utf-8")
        fixture_file = f"{spec.fixture_id}.json"
        (bundle_dir / fixture_file).write_bytes(payload)
        payloads_by_fixture_id[spec.fixture_id] = payload
        entries.append(
            FixtureBundleEntry(
                fixture_id=spec.fixture_id,
                source_key=spec.source_key,
                source_type=spec.source_type,
                trust_tier=spec.trust_tier,
                endpoint_url=spec.endpoint_url,
                parser_profile=spec.parser_profile,
                content_type=spec.content_type,
                fixture_file=fixture_file,
                priority=spec.priority,
                crawl_policy=spec.crawl_policy,
                raw_artifact_id=uuid4(),
                crawl_run_id=uuid4(),
                requested_at=datetime(2026, 4, 29, 9, 0, tzinfo=UTC),
                fetched_at=datetime(2026, 4, 29, 9, 1, tzinfo=UTC),
                sha256=hashlib.sha256(payload).hexdigest(),
                content_length=len(payload),
                final_url=spec.endpoint_url,
                http_status=200,
                response_headers={"content-type": spec.content_type},
            )
        )
    manifest_path = bundle_dir / "manifest.json"
    FixtureBundleManifest(bundle_name="backfill-demo", entries=entries).write(manifest_path)

    registry = FakeRegistryGateway()
    importer = FakeRawImporter()
    parser_replay = FakeParserReplayService()
    normalizer_replay = FakeNormalizationReplayService()
    service = FixtureBackfillService(
        registry_gateway=registry,
        payload_loader=FixturePayloadLoader(),
        raw_importer=importer,
        parser_replay_service=parser_replay,
        normalization_replay_service=normalizer_replay,
    )

    result = await service.backfill_manifest(manifest_path)

    assert result.bundle_name == "backfill-demo"
    assert len(result.items) == 3
    assert [entry.fixture_id for entry in registry.entries] == [
        "official",
        "aggregator",
        "ranking",
    ]
    assert len(importer.calls) == 3
    for entry, payload in importer.calls:
        assert payload == payloads_by_fixture_id[entry.fixture_id]
    assert len(parser_replay.calls) == 3
    assert len(normalizer_replay.calls) == 3
