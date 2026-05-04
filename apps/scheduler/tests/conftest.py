"""Test bootstrap for scheduler app smoke tests.

Sets a deterministic admin API key before the FastAPI app is imported, so the
AdminApiKeyMiddleware is configured (otherwise it would return 503 in tests).
Existing tests pass the matching `Authorization: Bearer <SCHEDULER_TEST_ADMIN_API_KEY>`
header via the `admin_auth_headers` fixture.
"""

from __future__ import annotations

import os

import pytest

SCHEDULER_TEST_ADMIN_API_KEY = "scheduler-tests-admin-key"

os.environ.setdefault("PLATFORM_ADMIN_API_KEY", SCHEDULER_TEST_ADMIN_API_KEY)


@pytest.fixture
def admin_auth_headers() -> dict[str, str]:
    return {"authorization": f"Bearer {SCHEDULER_TEST_ADMIN_API_KEY}"}
