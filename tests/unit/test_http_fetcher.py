import hashlib
from uuid import uuid4

import httpx
import pytest

from libs.source_sdk import FetchContext
from libs.source_sdk.fetchers import (
    HttpFetcher,
    UnsupportedContentTypeError,
    build_mock_http_client_factory,
    content_media_type,
)


def test_content_media_type_normalizes_header_value() -> None:
    assert content_media_type("Text/HTML; charset=UTF-8") == "text/html"


@pytest.mark.asyncio
async def test_http_fetcher_captures_status_headers_etag_and_last_modified() -> None:
    crawl_run_id = uuid4()
    requested_urls: list[str] = []
    requested_headers: list[httpx.Headers] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_urls.append(str(request.url))
        requested_headers.append(request.headers)
        if request.url.path == "/start":
            return httpx.Response(
                status_code=302,
                headers={"location": "https://example.edu/final"},
                request=request,
            )
        return httpx.Response(
            status_code=200,
            headers={
                "Content-Type": "text/html; charset=utf-8",
                "ETag": '"abc123"',
                "Last-Modified": "Mon, 20 Apr 2026 10:00:00 GMT",
                "X-Source-Trace": "trace-1",
            },
            content=b"<html>ok</html>",
            request=request,
        )

    fetcher = HttpFetcher(
        client_factory=build_mock_http_client_factory(handler),
        user_agent="parser-test/1.0",
    )
    context = FetchContext(
        crawl_run_id=crawl_run_id,
        source_key="msu-official",
        endpoint_url="https://example.edu/start",
        parser_profile="official_site.default",
        timeout_seconds=45,
        request_headers={"X-Crawl-Run": str(crawl_run_id)},
    )

    artifact = await fetcher.fetch(context)

    assert requested_urls == ["https://example.edu/start", "https://example.edu/final"]
    assert requested_headers[0]["user-agent"] == "parser-test/1.0"
    assert requested_headers[0]["x-crawl-run"] == str(crawl_run_id)
    assert artifact.crawl_run_id == crawl_run_id
    assert artifact.source_key == "msu-official"
    assert artifact.source_url == "https://example.edu/start"
    assert artifact.final_url == "https://example.edu/final"
    assert artifact.http_status == 200
    assert artifact.content_type == "text/html; charset=utf-8"
    assert artifact.response_headers["content-type"] == "text/html; charset=utf-8"
    assert artifact.response_headers["x-source-trace"] == "trace-1"
    assert artifact.etag == '"abc123"'
    assert artifact.last_modified == "Mon, 20 Apr 2026 10:00:00 GMT"
    assert artifact.content_length == len(b"<html>ok</html>")
    assert artifact.sha256 == hashlib.sha256(b"<html>ok</html>").hexdigest()
    assert artifact.content == b"<html>ok</html>"
    assert artifact.metadata["request_headers"] == {
        "user-agent": "parser-test/1.0",
        "x-crawl-run": str(crawl_run_id),
    }
    assert artifact.metadata["response_reason_phrase"] == "OK"


@pytest.mark.asyncio
async def test_http_fetcher_rejects_disallowed_content_type() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            headers={"Content-Type": "image/png"},
            content=b"png",
            request=request,
        )

    fetcher = HttpFetcher(client_factory=build_mock_http_client_factory(handler))
    context = FetchContext(
        crawl_run_id=uuid4(),
        source_key="msu-official",
        endpoint_url="https://example.edu/logo.png",
        allowed_content_types=["text/html", "application/json"],
    )

    with pytest.raises(UnsupportedContentTypeError) as exc_info:
        await fetcher.fetch(context)

    assert exc_info.value.content_type == "image/png"
    assert exc_info.value.allowed_content_types == ["text/html", "application/json"]
