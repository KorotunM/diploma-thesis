"""Async per-source token-bucket rate limiter for outbound HTTP fetches.

Real university sites and aggregators rate-limit aggressively. Without this
limiter, parser workers will get IP-banned during a demo. The limiter is
keyed by `source_key` so each source gets its own budget.

Default rate is read from `PLATFORM_DEFAULT_FETCH_RATE_PER_SECOND` (default
1.0 req/s). Per-source overrides can be passed in the `CrawlPolicy.metadata`
under the key `crawl_rate_per_second` (future extension).
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field

DEFAULT_RATE_ENV = "PLATFORM_DEFAULT_FETCH_RATE_PER_SECOND"
DEFAULT_RATE = 1.0
DEFAULT_BURST = 2


def _read_default_rate() -> float:
    raw = os.environ.get(DEFAULT_RATE_ENV)
    if raw is None:
        return DEFAULT_RATE
    try:
        rate = float(raw)
    except ValueError:
        return DEFAULT_RATE
    if rate <= 0:
        return DEFAULT_RATE
    return rate


@dataclass
class _Bucket:
    rate_per_second: float
    capacity: float
    tokens: float
    last_refill: float
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class SourceRateLimiter:
    """Async token-bucket per source_key.

    `acquire(source_key)` blocks until at least one token is available, then
    returns. Workers must call `acquire` immediately before each outbound HTTP
    request.
    """

    def __init__(
        self,
        *,
        default_rate_per_second: float | None = None,
        default_burst: int = DEFAULT_BURST,
        clock: callable = time.monotonic,
    ) -> None:
        self._default_rate = default_rate_per_second or _read_default_rate()
        self._default_burst = max(1, default_burst)
        self._clock = clock
        self._buckets: dict[str, _Bucket] = {}
        self._registry_lock = asyncio.Lock()

    async def acquire(self, source_key: str, *, tokens: float = 1.0) -> None:
        bucket = await self._get_or_create_bucket(source_key)
        async with bucket.lock:
            while True:
                self._refill(bucket)
                if bucket.tokens >= tokens:
                    bucket.tokens -= tokens
                    return
                deficit = tokens - bucket.tokens
                wait_seconds = deficit / bucket.rate_per_second
                await asyncio.sleep(wait_seconds)

    def configure(
        self,
        source_key: str,
        *,
        rate_per_second: float,
        burst: int | None = None,
    ) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be positive")
        capacity = float(burst) if burst is not None else float(self._default_burst)
        self._buckets[source_key] = _Bucket(
            rate_per_second=rate_per_second,
            capacity=capacity,
            tokens=capacity,
            last_refill=self._clock(),
        )

    async def _get_or_create_bucket(self, source_key: str) -> _Bucket:
        existing = self._buckets.get(source_key)
        if existing is not None:
            return existing
        async with self._registry_lock:
            existing = self._buckets.get(source_key)
            if existing is not None:
                return existing
            bucket = _Bucket(
                rate_per_second=self._default_rate,
                capacity=float(self._default_burst),
                tokens=float(self._default_burst),
                last_refill=self._clock(),
            )
            self._buckets[source_key] = bucket
            return bucket

    def _refill(self, bucket: _Bucket) -> None:
        now = self._clock()
        elapsed = max(0.0, now - bucket.last_refill)
        bucket.tokens = min(
            bucket.capacity,
            bucket.tokens + elapsed * bucket.rate_per_second,
        )
        bucket.last_refill = now


__all__ = ["SourceRateLimiter", "DEFAULT_RATE_ENV"]
