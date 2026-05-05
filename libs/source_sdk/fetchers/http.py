from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx

from libs.source_sdk.base_adapter import FetchContext, FetchedArtifact
from libs.source_sdk.fetchers.rate_limiter import SourceRateLimiter


def utc_now() -> datetime:
    return datetime.now(UTC)


def normalize_header_name(name: str) -> str:
    return name.strip().lower()


def normalize_response_headers(headers: httpx.Headers) -> dict[str, str]:
    return {normalize_header_name(key): value for key, value in headers.items()}


def content_media_type(content_type: str) -> str:
    return content_type.split(";", 1)[0].strip().lower()


class UnsupportedContentTypeError(ValueError):
    def __init__(self, content_type: str, allowed_content_types: list[str]) -> None:
        super().__init__(
            f"Unsupported response content type '{content_type}'. "
            f"Allowed content types: {', '.join(allowed_content_types)}"
        )
        self.content_type = content_type
        self.allowed_content_types = allowed_content_types


class HttpFetcher:
    def __init__(
        self,
        *,
        client_factory: Callable[..., httpx.AsyncClient] = httpx.AsyncClient,
        follow_redirects: bool = True,
        user_agent: str = "diploma-parser/0.1",
        rate_limiter: SourceRateLimiter | None = None,
    ) -> None:
        self._client_factory = client_factory
        self._follow_redirects = follow_redirects
        self._user_agent = user_agent
        self._rate_limiter = rate_limiter

    async def fetch(self, context: FetchContext) -> FetchedArtifact:
        if self._rate_limiter is not None:
            await self._rate_limiter.acquire(context.source_key)
        request_headers = self._build_request_headers(context)
        timeout = httpx.Timeout(
            connect=10.0,
            read=float(context.timeout_seconds),
            write=10.0,
            pool=5.0,
        )
        async with self._client_factory(
            follow_redirects=self._follow_redirects,
            timeout=timeout,
            headers=request_headers,
        ) as client:
            response = await client.get(context.endpoint_url)

        response_headers = normalize_response_headers(response.headers)
        content_type = response_headers.get("content-type", "application/octet-stream")
        self._validate_content_type(content_type, context.allowed_content_types)
        content = response.content
        return FetchedArtifact(
            crawl_run_id=context.crawl_run_id,
            source_key=context.source_key,
            source_url=context.endpoint_url,
            final_url=str(response.url),
            http_status=response.status_code,
            content_type=content_type,
            response_headers=response_headers,
            content_length=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            fetched_at=utc_now(),
            render_mode="http",
            etag=response_headers.get("etag"),
            last_modified=response_headers.get("last-modified"),
            content=content,
            metadata={
                "parser_profile": context.parser_profile,
                "request_headers": request_headers,
                "response_reason_phrase": response.reason_phrase,
            },
        )

    def _build_request_headers(self, context: FetchContext) -> dict[str, str]:
        headers: dict[str, str] = {"user-agent": self._user_agent}
        for name, value in context.request_headers.items():
            headers[normalize_header_name(name)] = value
        return headers

    @staticmethod
    def _validate_content_type(content_type: str, allowed_content_types: list[str]) -> None:
        if not allowed_content_types:
            return
        media_type = content_media_type(content_type)
        allowed_media_types = [content_media_type(value) for value in allowed_content_types]
        if media_type not in allowed_media_types:
            raise UnsupportedContentTypeError(content_type, allowed_content_types)


def build_mock_http_client_factory(
    handler: Callable[[httpx.Request], httpx.Response],
) -> Callable[..., httpx.AsyncClient]:
    def factory(**kwargs: Any) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(handler), **kwargs)

    return factory
