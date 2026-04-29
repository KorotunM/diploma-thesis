from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from uuid import UUID

from apps.parser.app.raw_artifacts import RawArtifactRecord, RawArtifactRepository
from libs.storage import MinIOStorageClient, get_minio_storage, get_postgres_session_factory
from scripts.mvp_fixtures import (
    FixtureBundleEntry,
    FixtureBundleManifest,
    MvpSourceSpec,
    default_mvp_source_specs,
)


class FixtureCaptureError(ValueError):
    pass


class FixtureCaptureService:
    def __init__(
        self,
        *,
        raw_artifact_repository: RawArtifactRepository,
        storage: MinIOStorageClient,
        output_root: Path,
    ) -> None:
        self._raw_artifact_repository = raw_artifact_repository
        self._storage = storage
        self._output_root = output_root

    def capture_mvp_bundle(
        self,
        *,
        bundle_name: str,
        raw_artifact_ids_by_fixture_id: dict[str, UUID],
    ) -> Path:
        specs = default_mvp_source_specs()
        bundle_dir = self._output_root / bundle_name
        bundle_dir.mkdir(parents=True, exist_ok=True)

        entries: list[FixtureBundleEntry] = []
        for spec in specs:
            raw_artifact_id = raw_artifact_ids_by_fixture_id.get(spec.fixture_id)
            if raw_artifact_id is None:
                raise FixtureCaptureError(
                    f"Missing raw_artifact_id for fixture_id={spec.fixture_id}."
                )
            entry, payload = self._capture_entry(spec=spec, raw_artifact_id=raw_artifact_id)
            fixture_path = bundle_dir / spec.fixture_file_name
            fixture_path.write_bytes(payload)
            entries.append(
                entry.model_copy(
                    update={"fixture_file": fixture_path.name},
                )
            )

        manifest = FixtureBundleManifest(bundle_name=bundle_name, entries=entries)
        manifest_path = bundle_dir / "manifest.json"
        manifest.write(manifest_path)
        return manifest_path

    def _capture_entry(
        self,
        *,
        spec: MvpSourceSpec,
        raw_artifact_id: UUID,
    ) -> tuple[FixtureBundleEntry, bytes]:
        raw_record = self._raw_artifact_repository.get_by_id(raw_artifact_id)
        if raw_record is None:
            raise FixtureCaptureError(f"Raw artifact {raw_artifact_id} was not found.")

        self._validate_record(spec=spec, raw_record=raw_record)
        payload = self._storage.get_bytes(
            bucket_name=raw_record.storage_bucket,
            object_name=raw_record.storage_object_key,
        )
        actual_sha256 = hashlib.sha256(payload).hexdigest()
        if actual_sha256 != raw_record.sha256:
            raise FixtureCaptureError(
                "Captured raw artifact sha256 mismatch: "
                f"expected {raw_record.sha256}, got {actual_sha256}."
            )

        response_headers = raw_record.metadata.get("response_headers")
        return (
            FixtureBundleEntry(
                fixture_id=spec.fixture_id,
                source_key=spec.source_key,
                source_type=spec.source_type,
                trust_tier=spec.trust_tier,
                endpoint_url=spec.endpoint_url,
                parser_profile=spec.parser_profile,
                content_type=raw_record.content_type,
                fixture_file=spec.fixture_file_name,
                priority=spec.priority,
                crawl_policy=spec.crawl_policy,
                raw_artifact_id=raw_record.raw_artifact_id,
                crawl_run_id=raw_record.crawl_run_id,
                requested_at=self._requested_at(raw_record),
                fetched_at=raw_record.fetched_at,
                sha256=raw_record.sha256,
                content_length=len(payload),
                final_url=raw_record.final_url,
                http_status=raw_record.http_status,
                etag=raw_record.etag,
                last_modified=raw_record.last_modified,
                response_headers=(
                    response_headers if isinstance(response_headers, dict) else {}
                ),
                metadata={
                    "storage_bucket": raw_record.storage_bucket,
                    "storage_object_key": raw_record.storage_object_key,
                    "captured_from_raw_artifact_id": str(raw_record.raw_artifact_id),
                },
            ),
            payload,
        )

    @staticmethod
    def _requested_at(raw_record: RawArtifactRecord):
        value = raw_record.metadata.get("requested_at")
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except Exception:
                return raw_record.fetched_at
        return raw_record.fetched_at

    @staticmethod
    def _validate_record(
        *,
        spec: MvpSourceSpec,
        raw_record: RawArtifactRecord,
    ) -> None:
        if raw_record.source_key != spec.source_key:
            raise FixtureCaptureError(
                f"Raw artifact {raw_record.raw_artifact_id} belongs to {raw_record.source_key}, "
                f"expected {spec.source_key}."
            )
        parser_profile = raw_record.metadata.get("parser_profile")
        if parser_profile is not None and parser_profile != spec.parser_profile:
            raise FixtureCaptureError(
                f"Raw artifact {raw_record.raw_artifact_id} has parser_profile={parser_profile}, "
                f"expected {spec.parser_profile}."
            )


def build_fixture_capture_service(
    *,
    output_root: Path,
    session=None,
    storage: MinIOStorageClient | None = None,
) -> FixtureCaptureService:
    resolved_session = session
    if resolved_session is None:
        session_factory = get_postgres_session_factory(service_name="parser")
        resolved_session = session_factory()
    return FixtureCaptureService(
        raw_artifact_repository=RawArtifactRepository(resolved_session),
        storage=storage or get_minio_storage(service_name="parser"),
        output_root=output_root,
    )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.fixture_capture",
        description="Capture three MVP raw artifacts into a reusable fixture bundle.",
    )
    parser.add_argument("--bundle-name", required=True)
    parser.add_argument(
        "--output-dir",
        default="tests/fixtures/mvp_bundle",
    )
    parser.add_argument("--official-raw-artifact-id", required=True, type=UUID)
    parser.add_argument("--aggregator-raw-artifact-id", required=True, type=UUID)
    parser.add_argument("--ranking-raw-artifact-id", required=True, type=UUID)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    session_factory = get_postgres_session_factory(service_name="parser")
    session = session_factory()
    try:
        service = build_fixture_capture_service(
            output_root=Path(args.output_dir),
            session=session,
        )
        manifest_path = service.capture_mvp_bundle(
            bundle_name=args.bundle_name,
            raw_artifact_ids_by_fixture_id={
                "official": args.official_raw_artifact_id,
                "aggregator": args.aggregator_raw_artifact_id,
                "ranking": args.ranking_raw_artifact_id,
            },
        )
        print(
            json.dumps(
                {
                    "bundle_name": args.bundle_name,
                    "manifest_path": str(manifest_path),
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    finally:
        session.close()
