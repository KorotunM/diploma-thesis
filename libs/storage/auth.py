"""Admin API authentication middleware.

Reads `Authorization: Bearer <token>` and compares it (constant-time) to the
configured `PLATFORM_ADMIN_API_KEY` environment variable. Health endpoints
(/healthz, /readyz, /metrics) are always allowed.

For production, replace this with a real OIDC/JWT validator.
"""

from __future__ import annotations

import hmac
import logging
import os
from collections.abc import Iterable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp

logger = logging.getLogger(__name__)

PUBLIC_PATH_PREFIXES: tuple[str, ...] = (
    "/healthz",
    "/readyz",
    "/metrics",
    "/docs",
    "/openapi.json",
    "/redoc",
)

ADMIN_API_KEY_ENV = "PLATFORM_ADMIN_API_KEY"


class AdminApiKeyMiddleware(BaseHTTPMiddleware):
    """Require a Bearer token on routes whose path starts with any protected prefix."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        protected_prefixes: Iterable[str] = ("/admin",),
        api_key: str | None = None,
        public_prefixes: Iterable[str] = PUBLIC_PATH_PREFIXES,
    ) -> None:
        super().__init__(app)
        self._protected = tuple(protected_prefixes)
        self._public = tuple(public_prefixes)
        self._api_key = api_key if api_key is not None else os.environ.get(ADMIN_API_KEY_ENV)
        if self._api_key is None or not self._api_key.strip():
            logger.warning(
                "admin_api_key_missing",
                extra={"hint": f"Set {ADMIN_API_KEY_ENV} to enable admin auth."},
            )

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        path = request.url.path

        if any(path == prefix or path.startswith(prefix + "/") for prefix in self._public):
            return await call_next(request)

        if not any(path.startswith(prefix) for prefix in self._protected):
            return await call_next(request)

        if not self._api_key:
            logger.error(
                "admin_request_blocked_unconfigured",
                extra={"path": path, "method": request.method},
            )
            return JSONResponse(
                status_code=503,
                content={
                    "error": "admin_api_unconfigured",
                    "detail": (
                        f"Admin API is disabled because {ADMIN_API_KEY_ENV} is not set."
                    ),
                },
            )

        provided = self._extract_bearer(request)
        if provided is None:
            return JSONResponse(
                status_code=401,
                content={"error": "missing_credentials", "detail": "Bearer token required."},
                headers={"WWW-Authenticate": 'Bearer realm="admin"'},
            )

        if not hmac.compare_digest(provided, self._api_key):
            logger.warning(
                "admin_request_invalid_key",
                extra={"path": path, "method": request.method},
            )
            return JSONResponse(
                status_code=403,
                content={"error": "invalid_credentials", "detail": "Invalid admin API key."},
            )

        return await call_next(request)

    @staticmethod
    def _extract_bearer(request: Request) -> str | None:
        header = request.headers.get("authorization", "")
        scheme, _, token = header.partition(" ")
        if scheme.lower() != "bearer" or not token.strip():
            return None
        return token.strip()


__all__ = ["AdminApiKeyMiddleware", "ADMIN_API_KEY_ENV", "PUBLIC_PATH_PREFIXES"]
