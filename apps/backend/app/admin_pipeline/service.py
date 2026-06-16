from __future__ import annotations

import os

import httpx

from .models import (
    PipelineRerunRequest,
    PipelineRerunResponse,
    PipelineRunsResponse,
    PipelineSourceItem,
    PipelineSourcesResponse,
)

DEFAULT_SCHEDULER_URL = "http://scheduler:8001"
DEFAULT_TIMEOUT_SECONDS = 15.0


class SchedulerAdminError(RuntimeError):
    """Raised when the scheduler admin API is unreachable or returns an error."""

    def __init__(self, message: str, *, status_code: int = 502) -> None:
        super().__init__(message)
        self.status_code = status_code


class AdminPipelineService:
    """Server-side proxy to the scheduler's protected admin API.

    The admin API key never leaves the backend — the browser only talks to the
    backend, which attaches the bearer token when calling the scheduler. This keeps
    the control-plane credential off the client while letting admins drive re-runs.
    """

    def __init__(
        self,
        *,
        scheduler_url: str | None = None,
        api_key: str | None = None,
        timeout_seconds: float | None = None,
    ) -> None:
        self._scheduler_url = (
            scheduler_url or os.getenv("PLATFORM_SCHEDULER_URL") or DEFAULT_SCHEDULER_URL
        ).rstrip("/")
        self._api_key = api_key or os.getenv("PLATFORM_ADMIN_API_KEY")
        self._timeout_seconds = timeout_seconds or DEFAULT_TIMEOUT_SECONDS

    def rerun(self, request: PipelineRerunRequest) -> PipelineRerunResponse:
        payload = self._request(
            "POST",
            "/admin/v1/pipeline/rerun",
            json=request.model_dump(mode="json"),
        )
        return PipelineRerunResponse.model_validate(payload)

    def list_runs(self, *, limit: int = 50) -> PipelineRunsResponse:
        payload = self._request(
            "GET",
            "/admin/v1/pipeline/runs",
            params={"limit": limit},
        )
        return PipelineRunsResponse.model_validate(payload)

    def list_sources(self) -> PipelineSourcesResponse:
        payload = self._request(
            "GET",
            "/admin/v1/sources",
            params={"limit": 200, "include_inactive": "true"},
        )
        items = payload.get("items", []) if isinstance(payload, dict) else []
        return PipelineSourcesResponse(
            items=[PipelineSourceItem.model_validate(item) for item in items]
        )

    def _request(self, method: str, path: str, **kwargs: object) -> dict:
        if not self._api_key:
            raise SchedulerAdminError(
                "Admin API key is not configured (PLATFORM_ADMIN_API_KEY).",
                status_code=503,
            )
        headers = {"Authorization": f"Bearer {self._api_key}"}
        url = f"{self._scheduler_url}{path}"
        try:
            with httpx.Client(timeout=self._timeout_seconds) as client:
                response = client.request(method, url, headers=headers, **kwargs)  # type: ignore[arg-type]
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise SchedulerAdminError(
                f"Scheduler admin API returned {exc.response.status_code}.",
                status_code=502,
            ) from exc
        except httpx.HTTPError as exc:
            raise SchedulerAdminError("Scheduler admin API is unreachable.") from exc
        return response.json()
