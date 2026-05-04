---
description: Verify the MVP end-to-end — run the smoke test suite, then check the public API and frontend
---

Verify the MVP is working end-to-end:

1. Run unit tests: `pytest tests/unit -q`
2. Run integration tests: `pytest tests/integration -q`
3. Run E2E smoke test: `pytest tests/e2e/test_compose_demo_smoke.py -q`
4. If the stack is up, hit the public API:
   - `curl http://localhost:8004/api/v1/search?query=`
   - `curl http://localhost:8004/healthz` and `/readyz` for all four services (8001..8004)
5. Verify admin auth is enforced:
   - `curl -i http://localhost:8001/admin/v1/sources` should return 401
   - `curl -i -H "Authorization: Bearer $PLATFORM_ADMIN_API_KEY" http://localhost:8001/admin/v1/sources` should return 200
6. Check the frontend renders at `http://localhost:5173` (no Vite HMR banner — should be served from nginx)

Report any failure with: which step failed, the command output, and the file path of the most likely culprit.
