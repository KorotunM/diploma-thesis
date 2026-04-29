from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

import httpx


class ComposeDemoSmokeError(RuntimeError):
    pass


@dataclass(frozen=True)
class ComposeDemoSmokeConfig:
    frontend_base_url: str = "http://localhost:5173"
    scheduler_base_url: str = "http://localhost:8001"
    parser_base_url: str = "http://localhost:8002"
    normalizer_base_url: str = "http://localhost:8003"
    backend_base_url: str = "http://localhost:8004"
    search_query: str = "Example University"
    timeout_seconds: float = 10.0


@dataclass(frozen=True)
class SmokeStepResult:
    name: str
    url: str
    status_code: int
    detail: str


@dataclass(frozen=True)
class ComposeDemoSmokeResult:
    checked_at: str
    search_query: str
    university_id: str
    canonical_name: str
    frontend_deep_link_url: str
    steps: list[SmokeStepResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["steps"] = [asdict(step) for step in self.steps]
        return payload


class ComposeDemoSmokeRunner:
    def __init__(
        self,
        *,
        client_factory: Callable[..., httpx.Client] = httpx.Client,
    ) -> None:
        self._client_factory = client_factory

    def run(self, config: ComposeDemoSmokeConfig) -> ComposeDemoSmokeResult:
        steps: list[SmokeStepResult] = []
        with self._client_factory(timeout=config.timeout_seconds, follow_redirects=True) as client:
            self._check_frontend_shell(
                client,
                f"{config.frontend_base_url.rstrip('/')}/",
                steps,
                "frontend shell",
            )
            self._check_runtime_config(
                client,
                f"{config.frontend_base_url.rstrip('/')}/runtime-config.js",
                steps,
            )

            for service_name, base_url in (
                ("scheduler", config.scheduler_base_url),
                ("parser", config.parser_base_url),
                ("normalizer", config.normalizer_base_url),
                ("backend", config.backend_base_url),
            ):
                self._check_healthz(client, service_name, base_url, steps)

            search_url = (
                f"{config.backend_base_url.rstrip('/')}/api/v1/search"
                f"?query={quote(config.search_query)}&page=1&page_size=10"
            )
            search_body = self._get_json(client, search_url, steps, "backend search")
            items = search_body.get("items")
            if not isinstance(items, list) or len(items) == 0:
                raise ComposeDemoSmokeError(
                    f"Backend search returned no items for query '{config.search_query}'."
                )

            first_item = items[0]
            university_id = self._require_string(first_item, "university_id", "search item")
            canonical_name = self._require_string(first_item, "canonical_name", "search item")

            card_url = f"{config.backend_base_url.rstrip('/')}/api/v1/universities/{university_id}"
            card_body = self._get_json(client, card_url, steps, "backend card")
            canonical_value = self._read_nested_string(
                card_body,
                ("canonical_name", "value"),
                "card canonical_name.value",
            )
            if not isinstance(card_body.get("field_attribution"), dict):
                raise ComposeDemoSmokeError(
                    "Backend card response is missing field_attribution."
                )

            provenance_url = (
                f"{config.backend_base_url.rstrip('/')}/api/v1/universities/{university_id}/provenance"
            )
            provenance_body = self._get_json(client, provenance_url, steps, "backend provenance")
            if not isinstance(provenance_body.get("raw_artifacts"), list):
                raise ComposeDemoSmokeError(
                    "Backend provenance response is missing raw_artifacts."
                )
            if not isinstance(provenance_body.get("chain"), list):
                raise ComposeDemoSmokeError("Backend provenance response is missing chain.")

            frontend_deep_link_url = (
                f"{config.frontend_base_url.rstrip('/')}/?query={quote(config.search_query)}"
                f"&university_id={quote(university_id)}"
            )
            self._check_frontend_shell(
                client,
                frontend_deep_link_url,
                steps,
                "frontend deep link",
            )

        return ComposeDemoSmokeResult(
            checked_at=datetime.now(UTC).isoformat(),
            search_query=config.search_query,
            university_id=university_id,
            canonical_name=canonical_value or canonical_name,
            frontend_deep_link_url=frontend_deep_link_url,
            steps=steps,
        )

    def _check_healthz(
        self,
        client: httpx.Client,
        service_name: str,
        base_url: str,
        steps: list[SmokeStepResult],
    ) -> None:
        url = f"{base_url.rstrip('/')}/healthz"
        body = self._get_json(client, url, steps, f"{service_name} healthz")
        if body.get("service") != service_name:
            raise ComposeDemoSmokeError(
                f"{service_name} healthz returned unexpected service name: {body.get('service')!r}."
            )
        if not isinstance(body.get("dependencies"), dict):
            raise ComposeDemoSmokeError(
                f"{service_name} healthz returned no dependency map."
            )

    def _check_runtime_config(
        self,
        client: httpx.Client,
        url: str,
        steps: list[SmokeStepResult],
    ) -> None:
        text = self._get_text(client, url, steps, "frontend runtime config")
        if "__APP_RUNTIME_CONFIG__" not in text:
            raise ComposeDemoSmokeError(
                "Frontend runtime-config.js does not define __APP_RUNTIME_CONFIG__."
            )

    def _check_frontend_shell(
        self,
        client: httpx.Client,
        url: str,
        steps: list[SmokeStepResult],
        step_name: str,
    ) -> None:
        text = self._get_text(client, url, steps, step_name)
        expected_markers = (
            "<title>University Aggregator</title>",
            '<div id="root"></div>',
            'src="/runtime-config.js"',
            'src="/src/main.tsx"',
        )
        for marker in expected_markers:
            if marker not in text:
                raise ComposeDemoSmokeError(
                    f"Frontend shell at {url} is missing expected marker {marker!r}."
                )

    def _get_json(
        self,
        client: httpx.Client,
        url: str,
        steps: list[SmokeStepResult],
        step_name: str,
    ) -> dict[str, Any]:
        response = client.get(url)
        detail = self._response_detail(response)
        steps.append(
            SmokeStepResult(
                name=step_name,
                url=str(response.request.url),
                status_code=response.status_code,
                detail=detail,
            )
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise ComposeDemoSmokeError(
                f"{step_name} failed with status {response.status_code}: {detail}"
            ) from exc
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise ComposeDemoSmokeError(f"{step_name} did not return valid JSON.") from exc
        if not isinstance(payload, dict):
            raise ComposeDemoSmokeError(f"{step_name} returned non-object JSON.")
        return payload

    def _get_text(
        self,
        client: httpx.Client,
        url: str,
        steps: list[SmokeStepResult],
        step_name: str,
    ) -> str:
        response = client.get(url)
        detail = self._response_detail(response)
        steps.append(
            SmokeStepResult(
                name=step_name,
                url=str(response.request.url),
                status_code=response.status_code,
                detail=detail,
            )
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise ComposeDemoSmokeError(
                f"{step_name} failed with status {response.status_code}: {detail}"
            ) from exc
        return response.text

    @staticmethod
    def _response_detail(response: httpx.Response) -> str:
        content_type = response.headers.get("content-type", "unknown")
        return f"content-type={content_type}"

    @staticmethod
    def _require_string(
        payload: dict[str, Any],
        key: str,
        context: str,
    ) -> str:
        value = payload.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ComposeDemoSmokeError(f"{context} is missing non-empty string field '{key}'.")
        return value

    @staticmethod
    def _read_nested_string(
        payload: dict[str, Any],
        path: tuple[str, ...],
        context: str,
    ) -> str:
        current: Any = payload
        for key in path:
            if not isinstance(current, dict):
                raise ComposeDemoSmokeError(f"{context} is missing nested object at '{key}'.")
            current = current.get(key)
        if not isinstance(current, str) or not current.strip():
            raise ComposeDemoSmokeError(f"{context} is missing non-empty string value.")
        return current


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tests.e2e.compose_demo_smoke",
        description="Smoke-check a compose-up MVP stack from frontend shell to delivery APIs.",
    )
    parser.add_argument("--frontend-base-url", default="http://localhost:5173")
    parser.add_argument("--scheduler-base-url", default="http://localhost:8001")
    parser.add_argument("--parser-base-url", default="http://localhost:8002")
    parser.add_argument("--normalizer-base-url", default="http://localhost:8003")
    parser.add_argument("--backend-base-url", default="http://localhost:8004")
    parser.add_argument("--search-query", default="Example University")
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    config = ComposeDemoSmokeConfig(
        frontend_base_url=args.frontend_base_url,
        scheduler_base_url=args.scheduler_base_url,
        parser_base_url=args.parser_base_url,
        normalizer_base_url=args.normalizer_base_url,
        backend_base_url=args.backend_base_url,
        search_query=args.search_query,
        timeout_seconds=args.timeout_seconds,
    )
    try:
        result = ComposeDemoSmokeRunner().run(config)
    except ComposeDemoSmokeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
