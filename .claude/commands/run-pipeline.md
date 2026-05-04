---
description: Run the local pipeline end-to-end — bring up the stack, seed sources, trigger crawls, follow logs
---

Run the full local data pipeline:

1. `make up` — bring up the docker compose stack
2. Wait until all services are healthy (`docker compose ps` showing `healthy` for postgres/rabbitmq/minio)
3. `python -m scripts.source_bootstrap` — register the 8 sources in the DB (idempotent)
4. `python -m scripts.seed_demo_data` — trigger one crawl per source via the admin API
5. `docker compose -f infra/docker-compose/docker-compose.yml logs -f parser-worker normalizer-worker` — follow worker output

Expected outcome: within 2–3 minutes you should see:
- Raw artifacts appearing in MinIO at `http://localhost:9001` under the `raw-artifacts` bucket
- Rows in `parsing.parsed_document` and `normalize.claim`
- Generated cards in `delivery.university_card`
- Search returning results at `http://localhost:8004/api/v1/search?query=`

If a crawl fails (real sites may rate-limit or block), the pipeline_run row will show status=`failed` with the error in metadata. That's expected for some sources on first run — the architecture is designed to retry safely.
