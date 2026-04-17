from datetime import datetime, timezone
import hashlib

import httpx

from libs.source_sdk.base_adapter import FetchedArtifact


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class HttpFetcher:
    async def fetch(self, url: str) -> FetchedArtifact:
        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            response = await client.get(url)
        return FetchedArtifact(
            source_url=str(response.url),
            content_type=response.headers.get("content-type", "application/octet-stream"),
            sha256=hashlib.sha256(response.content).hexdigest(),
            fetched_at=utc_now(),
            metadata={
                "status_code": response.status_code,
                "etag": response.headers.get("etag"),
                "last_modified": response.headers.get("last-modified"),
            },
        )
