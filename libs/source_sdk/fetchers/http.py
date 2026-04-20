import hashlib
from datetime import UTC, datetime

import httpx

from libs.source_sdk.base_adapter import FetchContext, FetchedArtifact


def utc_now() -> datetime:
    return datetime.now(UTC)


class HttpFetcher:
    async def fetch(self, context: FetchContext) -> FetchedArtifact:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=float(context.timeout_seconds),
            headers=context.request_headers,
        ) as client:
            response = await client.get(context.endpoint_url)
        content = response.content
        return FetchedArtifact(
            crawl_run_id=context.crawl_run_id,
            source_key=context.source_key,
            source_url=str(response.url),
            final_url=str(response.url),
            http_status=response.status_code,
            content_type=response.headers.get("content-type", "application/octet-stream"),
            content_length=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            fetched_at=utc_now(),
            render_mode="http",
            etag=response.headers.get("etag"),
            last_modified=response.headers.get("last-modified"),
            content=content,
            metadata={
                "requested_url": context.endpoint_url,
                "parser_profile": context.parser_profile,
            },
        )
