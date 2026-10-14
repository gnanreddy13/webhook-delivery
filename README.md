# webhook-delivery

A small webhook delivery service in Go and PostgreSQL. You register endpoints and send events. The service fans each event out to every subscribed endpoint, signs each request, retries failures with exponential backoff, and gives up after a configurable number of attempts.

Postgres is the only infrastructure. The queue is the `deliveries` table, and workers claim rows with `SELECT ... FOR UPDATE SKIP LOCKED`.

> ## Security notice
>
> This is a **single-tenant MVP**. It is **not meant to be exposed to the public internet**.
>
> - **No SSRF protection yet.** Anyone holding the API key can register any URL, including `localhost`, private networks and cloud metadata addresses, and the workers will POST to it.
> - One shared API key protects every route except `/healthz`. There are no per-user keys, scopes or rate limits.
> - Endpoint signing secrets are stored in plaintext in Postgres.
> - By default everything binds to `127.0.0.1`: Postgres in `docker-compose.yml`, the API, and the fake receiver.
>
> Run it on a trusted machine or a private network.

## Quick start

Requirements: Go 1.25+ (the `go.mod` minimum; tested only with Go 1.27.1), Docker (with Compose v2) and `curl`. From a fresh clone:

```bash
cp .env.example .env
docker compose up -d --wait
./scripts/demo.sh
```

The demo **wipes the `endpoints`, `events` and `deliveries` tables** of the local compose database. It then:

1. Builds and starts the server and a fake receiver. The receiver fails 30% of requests and verifies signatures.
2. Registers two endpoints: `/hook`, which is flaky, and `/fail`, which always returns 500.
3. Sends 200 events and then re-sends 50 of them as duplicates.
4. Waits for the queue to drain and prints the results.

Expected outcome: every `/hook` delivery is `delivered` after about 1.43 attempts on average. Every `/fail` delivery is `dead` after 6 attempts. There are no duplicate delivery rows and no bad signatures. The demo shortens backoff so it finishes in about 10 seconds.

Before you use it for anything beyond the demo, set a real `API_KEY` in `.env` (for example with `openssl rand -hex 32`).

## API

Every request except `GET /healthz` needs `Authorization: Bearer $API_KEY`.

```bash
# Register an endpoint. The signing secret is returned only in this response.
curl -s -H "Authorization: Bearer $API_KEY" -X POST localhost:8080/endpoints \
  -d '{"url":"http://127.0.0.1:9090/hook","event_types":["order.created"]}'
# 201 {"id":"…","url":"…","event_types":["order.created"],"secret":"whsec_…","created_at":"…"}

# Publish an event. Returns 202. Re-sending the same id is a no-op.
curl -s -H "Authorization: Bearer $API_KEY" -X POST localhost:8080/events \
  -d '{"id":"evt_123","type":"order.created","payload":{"order_id":42}}'
# 202 {"id":"evt_123","duplicate":false,"deliveries":1}

# Inspect deliveries
curl -s -H "Authorization: Bearer $API_KEY" "localhost:8080/deliveries?status=dead&limit=50"
curl -s -H "Authorization: Bearer $API_KEY" localhost:8080/deliveries/1
```

| Route | Notes |
|---|---|
| `GET /healthz` | No auth. |
| `POST /endpoints` | `url` must be absolute http(s). `event_types` must be non-empty. Returns a fresh `whsec_…` secret. |
| `POST /events` | `id`, `type` and `payload` (any non-null JSON) are required. Bodies are limited to 1 MiB. |
| `GET /deliveries` | Optional `status` (`pending`, `delivered` or `dead`) and `limit` (1–1000, default 100). Newest first. |
| `GET /deliveries/{id}` | Returns `status`, `attempts`, `next_attempt_at`, `last_status_code`, `last_error` and timestamps. |

### What a receiver gets

```
POST <endpoint url>
Content-Type: application/json
Webhook-Id: evt_123                 # event id: use it to dedupe
Webhook-Delivery-Id: 17
Webhook-Timestamp: 1760457600       # unix seconds
Webhook-Signature: v1=<hex HMAC-SHA256(secret, "<timestamp>.<raw body>")>

{"id":"evt_123","type":"order.created","created_at":"…","payload":{"order_id":42}}
```

To verify a request, recompute the HMAC over the timestamp, a `.`, and the **raw** body. Compare it with the header in constant time, and reject timestamps that are too old. `internal/signing.Verify` does all of this, and `cmd/receiver` uses it.

Any 2xx response counts as delivered. Anything else counts as a failure, including 3xx (redirects are not followed), timeouts after 5s, and connection errors.

## Design

```
POST /events ──► one transaction:
                   INSERT event (ON CONFLICT (id) DO NOTHING)
                   INSERT one deliveries row per subscribed endpoint
                 ──► 202

worker goroutines (WORKER_COUNT) loop:
  claim:   UPDATE deliveries SET next_attempt_at = now() + lease
           WHERE id IN (SELECT … WHERE status='pending' AND next_attempt_at <= now()
                        ORDER BY next_attempt_at LIMIT n FOR UPDATE SKIP LOCKED)
  send:    signed POST, 5s timeout
  record:  2xx → delivered
           otherwise → attempts+1, next_attempt_at = now() + backoff, or dead at MAX_ATTEMPTS
```

- **Transactional outbox.** The event and its deliveries are written in a single transaction. Either the API returns 202 and every delivery row exists, or nothing was written. Nothing is lost between "accepted" and "queued".
- **Idempotent ingest.** `events.id` is the primary key, and inserts use `ON CONFLICT DO NOTHING`. A repeated ID returns 202 with `"duplicate": true` and creates nothing. This holds under concurrency (see the tests). `UNIQUE (event_id, endpoint_id)` on `deliveries` also guarantees at most one delivery row per endpoint.
- **Claiming with a lease.** `FOR UPDATE SKIP LOCKED` lets many workers, in one process or many, pull from the same table without blocking each other or taking the same row. Claiming a row moves its `next_attempt_at` forward by `LEASE_DURATION` (default 30s). The row stays `pending` but is invisible to other workers. If a worker crashes, the row becomes due again when the lease expires. No separate in-flight state or reaper is needed.
- **Lease as a fencing token.** A worker records its result only if `next_attempt_at` still equals the value it set when it claimed the row. A worker whose lease expired cannot overwrite the result of whoever re-claimed the row.
- **Batching.** `CLAIM_BATCH_SIZE` defaults to 1. With larger batches, a worker hands back any rows whose remaining lease could not cover a full attempt. Without that, a slow receiver could make a row's lease expire while it is still queued in a worker's batch. I hit this bug while testing and fixed it.
- **Backoff.** `BACKOFF_BASE × 2^(attempt-1)`, capped at `BACKOFF_MAX`, with "equal jitter": the delay is uniform in `[d/2, d)`. Jitter spreads retries so a recovering receiver isn't hit by everything at once.
- **Graceful shutdown.** On SIGINT or SIGTERM the HTTP server stops taking requests, and each worker finishes and records its current attempt. Rows that were claimed but not yet sent are released, so they are due immediately.
- **Delivery semantics: at-least-once.** If the process dies after the receiver handled a request but before the result was written, that delivery is sent again after its lease expires. Receivers should dedupe on `Webhook-Id`. Ordering is not guaranteed.

### Why Postgres instead of Kafka

- **One system, one transaction.** The outbox guarantee depends on the event and its delivery rows committing together. With Kafka you would need an outbox table plus a relay anyway, or a dual write that can lose or duplicate messages.
- **Per-message state is the core of the problem.** Webhooks need per-delivery retry schedules, per-endpoint failure handling, dead letters, and queries like "show me the dead deliveries for endpoint X". In Postgres those are rows, indexes and `UPDATE`s. Kafka is an append-only log with per-partition offsets: delayed per-message retries need extra retry topics or a scheduler, and querying status means building a separate store.
- **Enough throughput for this stage.** On a laptop this design delivered more than 4,000/s with 32 workers (see the benchmark below). That's far beyond what a single-tenant MVP needs.
- **Operationally simpler.** One database to run, back up and inspect with SQL.

Kafka (or similar) becomes worth it at much higher volume, or when many independent consumers need the same event stream. See the roadmap.

## Configuration

All settings come from environment variables. `.env.example` lists every one, and the scripts load `.env`.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | local compose DB | Postgres connection string |
| `DB_MAX_CONNS` | 20 | pgx pool size. Keep it above `WORKER_COUNT` plus API headroom. |
| `LISTEN_ADDR` | `127.0.0.1:8080` | API listen address |
| `API_KEY` | required | Bearer token for every route except `/healthz` |
| `WORKER_COUNT` | 4 | Delivery worker goroutines |
| `MAX_ATTEMPTS` | 8 | Attempts before a delivery is marked `dead` |
| `BACKOFF_BASE` / `BACKOFF_MAX` | `1s` / `10m` | Exponential backoff base and cap |
| `DELIVERY_TIMEOUT` | `5s` | Per-attempt HTTP timeout |
| `LEASE_DURATION` | `30s` | How long a claimed row stays invisible to other workers. Must be at least `DELIVERY_TIMEOUT` + 2s. |
| `POLL_INTERVAL` | `250ms` | Idle wait when no deliveries are due |
| `CLAIM_BATCH_SIZE` | 1 | Rows claimed per query |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` / `POSTGRES_PORT` | | Used by `docker-compose.yml` |
| `RECEIVER_ADDR` / `RECEIVER_FAIL_RATE` | `127.0.0.1:9090` / `0.3` | Fake receiver. It also reads `RECEIVER_SECRET` and `RECEIVER_DELAY`. |

## Running things

```bash
go build -o bin/server ./cmd/server && set -a && . ./.env && set +a && ./bin/server
```

The server runs migrations (`migrations/*.sql`, applied in order and tracked in `schema_migrations`) on startup.

**Tests.** Integration tests need the compose Postgres and use a separate `webhook_test` database. They are skipped if Postgres is unreachable.

```bash
go test -race ./...
```

| Test | What it checks |
|---|---|
| `TestFailingReceiverEndsDead` | An httptest receiver that always returns 503. The delivery ends `dead` after exactly `MAX_ATTEMPTS` signed attempts, with `last_status_code = 503`. |
| `TestRetryThenDelivered` | The receiver fails twice, then returns 204. The delivery ends `delivered` on attempt 3. |
| `TestDuplicateEventsAreIgnored` | 50 concurrent POSTs with the same event ID. 1 event, 1 delivery per endpoint (2 total), and each endpoint receives exactly 1 request. |
| `TestConcurrentWorkersDeliverEachRowOnce` | 300 deliveries and 16 workers. Every delivery is sent exactly once. |
| `TestAuthRequired` | 401 without a key, and `/healthz` is open. |
| Unit tests | Signature sign and verify (tampering, wrong secret, stale timestamp) and backoff bounds. |

**Benchmark** (wipes the demo tables):

```bash
./bench/run.sh
# or: BENCH_WORKERS="4 16" BENCH_FAIL_RATES="0.3" BENCH_EVENTS=10000 ./bench/run.sh
```

## Benchmark results

**Conditions:**
- Hardware: the author's MacBook (Apple M5 Pro, 15 cores, 24 GB, macOS 27).
- Postgres 16 in Docker Desktop on the same machine, with default settings and a 40-connection pool.
- Server, fake receiver and load generator all run on the same machine over localhost. The receiver does no work and does not verify signatures in the bench, though every request is still signed.
- 10,000 events per run, all of one type, sent by 32 concurrent HTTP clients, with 1 subscribed endpoint (so 10,000 deliveries).
- Retry settings shortened for the bench: `BACKOFF_BASE=100ms`, `BACKOFF_MAX=2s`, `MAX_ATTEMPTS=8`.
- Two full runs of the whole matrix. Both are shown because throughput at low worker counts varied noticeably between runs.

These are loopback numbers with a receiver that responds instantly. Real receivers that take 50–500 ms per request lower per-worker throughput accordingly. Treat these figures as a ceiling for the queueing machinery, not a capacity plan.

**Receiver always succeeds: delivery throughput by worker count**

| Workers | Deliveries/s (run 1 / run 2) | POST /events p50 | POST /events p95 |
|---|---|---|---|
| 1 | 594 / 1,099 | 4.5 ms | 5.6 ms |
| 4 | 1,740 / 2,516 | 4.5–4.7 ms | 5.8 ms |
| 16 | 3,018 / 3,939 | 5.4–5.5 ms | 7.1–8.2 ms |
| 32 | 4,395 / 4,263 | 6.6–6.7 ms | 8.3–8.4 ms |

"Deliveries/s" is delivered rows divided by the time from the first event's `created_at` to the last `delivered_at`. Ingest ran at about 4,700–7,000 events/s and overlapped with delivery. Ingest gets slower as more workers compete for Postgres, which is why POST latency rises with worker count.

**Receiver fails 30% of requests at random (HTTP 500)**

| Workers | Success rate (run 1 / run 2) | Avg attempts per delivery | Dead | Time to drain |
|---|---|---|---|---|
| 1 | 100% / 99.99% | 1.417 / 1.427 | 0 / 1 | 21.0 s / 17.9 s |
| 4 | 100% / 100% | 1.430 / 1.419 | 0 / 0 | 11.3 s / 10.4 s |
| 16 | 100% / 100% | 1.415 / 1.432 | 0 / 0 | 6.1 s / 9.3 s |
| 32 | 100% / 99.99% | 1.426 / 1.435 | 0 / 1 | 8.4 s / 8.7 s |

Across 80,000 deliveries at a 30% failure rate, 2 ended dead. That matches expectations: the chance of failing 8 times in a row is 0.3⁸ ≈ 0.0066%, or about 5 expected across 80,000. The average attempts match the theoretical 1/0.7 ≈ 1.43. POST /events latency was the same as in the success-only runs (p50 4.5–6.5 ms, p95 5.6–8.0 ms). With failures, drain time comes mostly from the retry backoff schedule (up to about 8 s of cumulative backoff for an unlucky delivery), not from worker capacity. That is why 16 and 32 workers land within run-to-run noise of each other.

## Limitations

- A delivery's attempt history is only its latest `last_status_code` and `last_error`.
- No endpoint management beyond create (no list, update, disable or delete), and no secret rotation.
- No ordering guarantees, and no per-endpoint concurrency limit.
- `GET /deliveries` is a simple newest-first list (no cursor pagination or per-endpoint filter).
- Old events and deliveries are never deleted.

## Roadmap

- **SSRF protection:** resolve and block private, loopback, link-local and metadata IPs at connect time (not only at registration), and allowlist ports.
- **Multi-tenancy:** accounts, per-tenant API keys, and tenant-scoped endpoints and deliveries.
- **Secret encryption:** envelope-encrypt endpoint secrets (KMS or a master key), and support rotation with two active secrets.
- **Rate limits:** per-API-key ingest limits and per-endpoint delivery rate and concurrency limits.
- **Circuit breaker:** stop hammering an endpoint that keeps failing, pause and resume it, and auto-disable it after prolonged failure.
- **Per-attempt history:** a `delivery_attempts` table with status, latency, error, and a truncated response body.
- **Replay endpoint:** `POST /deliveries/{id}/retry` for dead deliveries, and bulk replay by endpoint and time range.
- **Retention cleanup:** a periodic job that deletes or archives old delivered and dead rows (or partitions tables by time).
- **Kafka at scale:** keep the Postgres outbox as the source of truth, but relay it into Kafka (or similar) once volume or the number of consumers outgrows one database. Delivery workers would then scale out as consumer groups.

## License

MIT, see [LICENSE](LICENSE).
