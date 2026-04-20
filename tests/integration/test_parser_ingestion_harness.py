import hashlib
import json

from libs.contracts.events import CrawlRequestEvent
from libs.source_sdk.stores import build_sha256_object_key
from tests.integration.parser_ingestion_harness import (
    ParserIngestionHarness,
    load_bytes_fixture,
    load_json_fixture,
)


def test_fixture_driven_parser_ingestion_harness_persists_raw_artifact() -> None:
    event_payload = load_json_fixture("crawl_request_official_site.json")
    html_payload = load_bytes_fixture("official_site_admissions.html")
    event = CrawlRequestEvent.model_validate(event_payload)
    expected_sha256 = hashlib.sha256(html_payload).hexdigest()
    expected_object_key = build_sha256_object_key(
        source_key=event.payload.source_key,
        sha256=expected_sha256,
        content_type="text/html; charset=utf-8",
    )
    harness = ParserIngestionHarness(
        endpoint_url=event.payload.endpoint_url,
        response_body=html_payload,
    )
    consumer = harness.build_consumer()

    result = consumer.handle_message(event_payload)

    assert result.event_id == event.header.event_id
    assert result.trace_id == event.header.trace_id
    assert result.crawl_run_id == event.payload.crawl_run_id
    assert result.source_key == "msu-official"
    assert result.endpoint_url == "https://example.edu/admissions"
    assert result.parser_profile == "official_site.default"
    assert result.metadata["idempotency_key"] == f"msu-official:{expected_sha256}"
    assert result.metadata["raw_bucket"] == "raw-html"
    assert result.metadata["raw_object_key"] == expected_object_key

    assert harness.requested_headers[0]["user-agent"] == "parser-ingestion-harness/1.0"
    assert harness.requested_headers[0]["x-fixture-run"] == "parser-ingestion"
    assert harness.minio_storage.ensure_bucket_calls == ["raw-html"]
    assert harness.minio_storage.put_calls[0]["bucket_name"] == "raw-html"
    assert harness.minio_storage.put_calls[0]["object_name"] == expected_object_key
    assert harness.minio_storage.put_calls[0]["payload"] == html_payload
    assert harness.minio_storage.put_calls[0]["metadata"]["sha256"] == expected_sha256
    assert harness.minio_storage.objects[("raw-html", expected_object_key)] == html_payload

    assert len(harness.raw_artifact_session.rows) == 1
    assert harness.raw_artifact_session.commit_count == 1
    raw_record = result.raw_artifact
    assert raw_record.sha256 == expected_sha256
    assert raw_record.storage_bucket == "raw-html"
    assert raw_record.storage_object_key == expected_object_key
    assert raw_record.etag == '"fixture-etag"'
    assert raw_record.last_modified == "Mon, 20 Apr 2026 12:00:00 GMT"
    assert raw_record.metadata["response_headers"]["etag"] == '"fixture-etag"'
    assert raw_record.metadata["minio_etag"] == "etag-1"


def test_fixture_driven_parser_ingestion_harness_is_idempotent_for_same_payload() -> None:
    event_payload = load_json_fixture("crawl_request_official_site.json")
    html_payload = load_bytes_fixture("official_site_admissions.html")
    harness = ParserIngestionHarness(
        endpoint_url=event_payload["payload"]["endpoint_url"],
        response_body=html_payload,
    )
    consumer = harness.build_consumer()

    first = consumer.handle_message(event_payload)
    second = consumer.handle_message(event_payload)

    assert first.raw_artifact.raw_artifact_id == second.raw_artifact.raw_artifact_id
    assert first.raw_artifact.sha256 == second.raw_artifact.sha256
    assert len(harness.raw_artifact_session.rows) == 1
    assert len(harness.minio_storage.put_calls) == 2
    assert harness.raw_artifact_session.commit_count == 2
    stored_metadata = next(iter(harness.raw_artifact_session.rows.values()))["metadata"]
    assert json.loads(stored_metadata)["minio_etag"] == "etag-2"
