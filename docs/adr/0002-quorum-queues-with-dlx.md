# ADR 0002: Quorum queues with retry/DLX topology

- Status: accepted
- Date: 2026-04-22

## Context

The pipeline is async — scheduler enqueues, parser/normalizer consume. Real-world parsers fail for many reasons: source site is down, rate-limit returns 429, HTML structure changed. We need a queue topology that:
- Survives a broker restart (durable)
- Distinguishes transient from permanent failures
- Doesn't lose messages on consumer crash
- Doesn't infinitely retry obviously broken messages
- Allows manual recovery / replay

## Decision

Use **RabbitMQ quorum queues** (not classic mirrored queues, not streams) with a three-tier retry topology per work type:

```
<work>.jobs (main exchange, direct)
   ├── <work>.high   ─┐
   └── <work>.bulk   ─┤
                      ├── on nack →
<work>.retry (retry exchange)
   ├── <work>.high.retry  (TTL=30s, dead-letter back to .high)
   └── <work>.bulk.retry  (TTL=30s, dead-letter back to .bulk)
                      ├── on retry exhausted →
<work>.dead (dead-letter exchange)
   ├── <work>.high.dead   (manual recovery)
   └── <work>.bulk.dead
```

Configuration (`libs/storage/rabbitmq/topology.py`):
- All queues are `x-queue-type: quorum`
- Main queues nack with `requeue=False` after `MAX_RETRIES` attempts (default 3)
- Retry queues use `x-message-ttl=30000` and `x-dead-letter-exchange` pointing back at main
- Dead queues are inspected manually via the management UI

## Consequences

- Positive: quorum queues replicate to multiple nodes (Raft-based) — survives one-node failures.
- Positive: explicit retry path with bounded attempts, never infinite.
- Positive: dead-letter queues let operators see what's broken without losing data.
- Negative: ~6 queues per work type instead of 1. Inspection takes more clicks.
- Negative: quorum queues are heavier than classic — but we have only 3 work types so cost is negligible.

## Alternatives considered

**Classic queues with `requeue=True`** — simple but loses messages on broker crash, can infinitely loop.

**Streams** — append-only log, great for replay. Overkill: we don't need historical replay of work events; we replay from raw_artifact.

**SQS / cloud queue** — not used because the project runs locally for MVP demos.

## References

- `libs/storage/rabbitmq/topology.py`
- https://www.rabbitmq.com/quorum-queues.html
- https://www.rabbitmq.com/dlx.html
