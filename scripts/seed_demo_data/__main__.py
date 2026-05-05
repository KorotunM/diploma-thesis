"""Seed the live MVP demo:

1. Bootstrap source registry (idempotent, via existing source_bootstrap workflow).
2. Trigger one manual crawl per implemented endpoint via the scheduler admin API.
3. Wait until all triggered runs reach a terminal state.
4. Print a summary.

This script is intended for local demos and CI. It assumes the docker-compose
stack is up and the scheduler is reachable. Auth credentials come from the
`PLATFORM_ADMIN_API_KEY` env var.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest

from apps.scheduler.app.persistence import sql_text as _sql_text
from scripts.source_bootstrap.workflow import (
    LiveSourceSeedResult,
    build_live_source_seed_service,
    managed_session,
)

DEFAULT_SCHEDULER_URL = os.environ.get(
    "PLATFORM_SCHEDULER_URL",
    "http://localhost:8001",
)
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_POLL_INTERVAL_SECONDS = 3.0
DEFAULT_POLL_MAX_SECONDS = 180.0


@dataclass(frozen=True, slots=True)
class TriggeredRun:
    source_key: str
    endpoint_url: str
    parser_profile: str
    run_id: str | None
    error: str | None = None


class SeedHttpError(RuntimeError):
    pass


def _admin_headers() -> dict[str, str]:
    api_key = os.environ.get("PLATFORM_ADMIN_API_KEY", "").strip()
    if not api_key:
        raise SeedHttpError(
            "PLATFORM_ADMIN_API_KEY is not set. Set it in infra/env/local/app.env or export it."
        )
    return {
        "authorization": f"Bearer {api_key}",
        "content-type": "application/json",
        "accept": "application/json",
    }


def _http_request(
    method: str,
    url: str,
    *,
    body: dict[str, Any] | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    payload = None
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
    req = urlrequest.Request(url=url, method=method, headers=_admin_headers(), data=payload)
    try:
        with urlrequest.urlopen(req, timeout=timeout) as response:
            text = response.read().decode("utf-8")
    except urlerror.HTTPError as exc:
        body_text = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        raise SeedHttpError(f"{method} {url} returned {exc.code}: {body_text}") from exc
    except urlerror.URLError as exc:
        raise SeedHttpError(f"{method} {url} failed: {exc.reason}") from exc
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SeedHttpError(f"{method} {url} returned non-JSON body: {text!r}") from exc


def _list_endpoints(
    scheduler_url: str,
    source_key: str,
) -> list[dict[str, Any]]:
    url = f"{scheduler_url.rstrip('/')}/admin/v1/sources/{source_key}/endpoints?limit=50&offset=0"
    response = _http_request("GET", url)
    return list(response.get("items", []))


def _trigger_crawl(
    scheduler_url: str,
    source_key: str,
    endpoint_id: str,
) -> str | None:
    url = f"{scheduler_url.rstrip('/')}/admin/v1/crawl-jobs"
    response = _http_request(
        "POST",
        url,
        body={"source_key": source_key, "endpoint_id": endpoint_id, "priority": "high"},
    )
    pipeline_run = response.get("pipeline_run") or {}
    return pipeline_run.get("run_id")


def _bootstrap_sources_and_get_crawled() -> tuple[LiveSourceSeedResult, set[str]]:
    with managed_session("scheduler") as session:
        result = build_live_source_seed_service(session).bootstrap()
        rows = session.execute(
            _sql_text(
                """
                SELECT DISTINCT metadata->>'endpoint_id'
                FROM ops.pipeline_run
                WHERE status = 'succeeded'
                  AND metadata->>'endpoint_id' IS NOT NULL
                """
            )
        ).fetchall()
        already_crawled = {row[0] for row in rows}
    return result, already_crawled


def _trigger_demo_crawls(
    scheduler_url: str,
    source_keys: tuple[str, ...],
    *,
    skip_endpoint_ids: set[str] | None = None,
) -> list[TriggeredRun]:
    skip = skip_endpoint_ids or set()
    triggered: list[TriggeredRun] = []
    for source_key in source_keys:
        try:
            endpoints = _list_endpoints(scheduler_url, source_key)
        except SeedHttpError as exc:
            triggered.append(
                TriggeredRun(
                    source_key=source_key,
                    endpoint_url="(unknown)",
                    parser_profile="(unknown)",
                    run_id=None,
                    error=str(exc),
                )
            )
            continue
        for endpoint in endpoints:
            endpoint_id = endpoint.get("endpoint_id")
            endpoint_url = endpoint.get("endpoint_url", "")
            parser_profile = endpoint.get("parser_profile", "")
            if not endpoint_id or "<" in endpoint_url:
                continue
            if endpoint_id in skip:
                print(
                    f"[seed] Skipping {source_key} / {endpoint_url} — already succeeded.",
                    file=sys.stderr,
                )
                continue
            try:
                run_id = _trigger_crawl(scheduler_url, source_key, endpoint_id)
            except SeedHttpError as exc:
                triggered.append(
                    TriggeredRun(
                        source_key=source_key,
                        endpoint_url=endpoint_url,
                        parser_profile=parser_profile,
                        run_id=None,
                        error=str(exc),
                    )
                )
                continue
            triggered.append(
                TriggeredRun(
                    source_key=source_key,
                    endpoint_url=endpoint_url,
                    parser_profile=parser_profile,
                    run_id=run_id,
                )
            )
    return triggered


def _print_summary(
    seed_result: LiveSourceSeedResult,
    triggered: list[TriggeredRun],
) -> None:
    successes = [run for run in triggered if run.error is None]
    failures = [run for run in triggered if run.error is not None]
    summary = {
        "sources_registered": seed_result.source_count,
        "endpoints_registered": seed_result.endpoint_count,
        "crawls_triggered": len(successes),
        "crawls_failed_to_trigger": len(failures),
        "triggered_runs": [
            {
                "source_key": run.source_key,
                "parser_profile": run.parser_profile,
                "endpoint_url": run.endpoint_url,
                "run_id": run.run_id,
            }
            for run in successes
        ],
        "trigger_failures": [
            {
                "source_key": run.source_key,
                "endpoint_url": run.endpoint_url,
                "error": run.error,
            }
            for run in failures
        ],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.seed_demo_data",
        description="Bootstrap demo sources and trigger one crawl per endpoint.",
    )
    parser.add_argument(
        "--scheduler-url",
        default=DEFAULT_SCHEDULER_URL,
        help="Base URL of the scheduler service (default: %(default)s).",
    )
    parser.add_argument(
        "--skip-trigger",
        action="store_true",
        help="Only bootstrap sources, do not trigger any crawls.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Trigger crawls even for endpoints that have already succeeded.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)

    print("[seed] Bootstrapping source registry...", file=sys.stderr)
    seed_result, already_crawled = _bootstrap_sources_and_get_crawled()
    skip = set() if args.force else already_crawled
    print(
        f"[seed] Registered {seed_result.source_count} sources, "
        f"{seed_result.endpoint_count} endpoints. "
        f"Already succeeded: {len(already_crawled)} endpoint(s)"
        + (" (--force: will re-trigger all)." if args.force else "."),
        file=sys.stderr,
    )

    if args.skip_trigger:
        _print_summary(seed_result, triggered=[])
        return 0

    print(
        f"[seed] Waiting for scheduler at {args.scheduler_url} to be reachable...",
        file=sys.stderr,
    )
    deadline = time.monotonic() + 120.0
    while time.monotonic() < deadline:
        try:
            _http_request("GET", f"{args.scheduler_url.rstrip('/')}/healthz", timeout=5.0)
            break
        except SeedHttpError:
            time.sleep(3.0)
    else:
        print(
            f"[seed] Scheduler at {args.scheduler_url} did not become healthy in time.",
            file=sys.stderr,
        )
        return 2

    print("[seed] Triggering crawls for new endpoints...", file=sys.stderr)
    triggered = _trigger_demo_crawls(
        args.scheduler_url,
        seed_result.source_keys,
        skip_endpoint_ids=skip,
    )
    _print_summary(seed_result, triggered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
