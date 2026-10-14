# webhook-delivery

A webhook delivery service in Go and PostgreSQL. You register endpoints and send events. The service fans each event out to every subscribed endpoint, signs each request, retries failures with exponential backoff, and gives up after a configurable number of attempts.

Postgres is the only infrastructure. The queue is the `deliveries` table, and workers claim rows with `FOR UPDATE SKIP LOCKED`.

> ## Security notice
>
> This is a **single-tenant** service: one shared API key, and one organization's endpoints. It is built to run as a service other systems call, provided these hold:
>
> - **Serve it over TLS.** Either set `TLS_CERT_FILE` and `TLS_KEY_FILE`, or put it behind a TLS-terminating reverse proxy and set `ALLOW_INSECURE_HTTP=true`. The server refuses plain HTTP on a non-loopback address otherwise.
> - **Keep `ALLOW_PRIVATE_DESTINATIONS` off** (the default). With it off, SSRF protection blocks webhooks to loopback, private, link-local (including cloud metadata), CGNAT and other reserved addresses. The check runs on the actual IP being connected to, so DNS rebinding can't bypass it. Only the local demo and bench turn it on.
> - **Guard the secrets in `.env`.** Anyone with `API_KEY` can register endpoints and read delivery data. `SECRET_ENCRYPTION_KEY` decrypts the endpoint signing secrets; if it is lost, those secrets can't be recovered.
>
> Not included: per-client API keys or scopes, multi-tenancy, and API rate limiting. See [Limitations](#limitations).

## Quick start

Requirements: Go 1.25+ (tested with 1.25.14 and 1.27.1; see [Platforms tested](#platforms-tested)), Docker (with Compose v2) and `curl`. From a fresh clone:

```bash
./scripts/init-env.sh
docker compose up -d --wait
./scripts/demo.sh
```

`init-env.sh` writes `.env` (mode 600) with a random `API_KEY`, `SECRET_ENCRYPTION_KEY` and Postgres password, and never overwrites an existing `.env`.

The demo only runs against a local database, and it **wipes the `endpoints`, `events` and `deliveries` tables**. It then:

1. Builds and starts the server and a fake receiver. The receiver fails 30% of requests and verifies signatures.
2. Registers two endpoints: `/hook`, which is flaky, and `/fail`, which always returns 500.
3. Sends 200 events and then re-sends 50 of them as duplicates.
4. Waits for the queue to drain and prints the results.

Expected outcome: every `/hook` delivery is `delivered` after about 1.43 attempts on average. Every `/fail` delivery is `dead` after 6 attempts. There are no duplicate delivery rows and no bad signatures. Because the receiver runs on localhost, the demo turns on `ALLOW_PRIVATE_DESTINATIONS`. It also turns off the circuit breaker and shortens backoff, so dead deliveries show up within seconds.

## Deploying

The server is a single binary (or container) plus Postgres. Several replicas can share one database: workers in different processes coordinate through row locks. Two processes were tested; see [Other verified behaviour](#other-verified-behaviour).

```bash
# Container image: static binary on distroless, runs as non-root (about 21 MB)
docker build -t webhook-delivery .

# Or run server and Postgres together locally (port published on 127.0.0.1 only)
docker compose --profile app up -d --build
```

Checklist:

- **TLS:** see the security notice above.
- **Database:** for a database on another host, use `sslmode=verify-full` in `DATABASE_URL`. Backups and HA are up to you. Migrations run automatically at startup under a Postgres advisory lock.
- **Health checks:** `GET /healthz` (process up) and `GET /readyz` (database reachable) need no API key.
- **Metrics:** `GET /metrics` (Prometheus text format; needs the API key as a bearer token).
- **Key rotation:** to rotate `SECRET_ENCRYPTION_KEY`, move the old key to `SECRET_ENCRYPTION_KEY_PREVIOUS`, set a new key, and restart. Secrets are re-encrypted at startup, after which the previous key can be removed. `API_KEY` rotation needs a restart and has no overlap period.
- **Shutdown:** SIGTERM stops intake. Each worker finishes its current attempt (bounded by `DELIVERY_TIMEOUT`), and claimed rows that weren't sent yet are released.

## API

Every request except `GET /healthz` and `GET /readyz` needs `Authorization: Bearer $API_KEY`.

```bash
# Register an endpoint. The signing secret is returned only here (and on rotate-secret).
curl -s -H "Authorization: Bearer $API_KEY" -X POST localhost:8080/endpoints \
  -d '{"url":"https://example.com/webhooks","event_types":["order.created"]}'
# 201 {"id":"…","url":"…","event_types":["order.created"],"secret":"whsec_…", …}

# Publish an event. Returns 202. Re-sending the same id is a no-op.
curl -s -H "Authorization: Bearer $API_KEY" -X POST localhost:8080/events \
  -d '{"id":"evt_123","type":"order.created","payload":{"order_id":42}}'
# 202 {"id":"evt_123","duplicate":false,"deliveries":1}

curl -s -H "Authorization: Bearer $API_KEY" "localhost:8080/deliveries?status=dead&limit=50"
curl -s -H "Authorization: Bearer $API_KEY" -X POST localhost:8080/deliveries/17/retry
```

| Route | Notes |
|---|---|
| `GET /healthz`, `GET /readyz` | No auth. `/readyz` returns 503 if the database is unreachable. |
| `POST /endpoints` | `url` must be absolute http(s), with no credentials and no private or loopback IP or `localhost`. `event_types` must be non-empty. Returns a fresh `whsec_…` secret. |
| `GET /endpoints`, `GET /endpoints/{id}` | Never include the secret. Show `disabled_at`, `consecutive_failures` and `paused_until` (circuit breaker). |
| `POST /endpoints/{id}/disable` / `enable` | A disabled endpoint gets no new deliveries, and its pending ones wait. Enabling also resets the circuit breaker. |
| `POST /endpoints/{id}/rotate-secret` | Returns a new secret. The old one stops being used immediately. |
| `POST /events` | `id`, `type` and `payload` (any non-null JSON) are required. Bodies are limited to 1 MiB. |
| `GET /deliveries` | Optional `status` (`pending`, `delivered` or `dead`) and `limit` (1–1000, default 100). Newest first. |
| `GET /deliveries/{id}` | `status`, `attempts`, `next_attempt_at` (null once delivered or dead), `last_status_code`, `last_error` and timestamps. |
| `POST /deliveries/{id}/retry` | Re-queues a **dead** delivery with fresh attempts. Returns 409 otherwise. |
| `GET /metrics` | Prometheus text format. |

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

Any 2xx response counts as delivered. Anything else counts as a failure, including 3xx (redirects are not followed), 4xx, timeouts (`DELIVERY_TIMEOUT`, default 5s), connection errors, and destinations blocked by SSRF protection.

## Design

```
POST /events ──► one transaction:
                   INSERT event (ON CONFLICT (id) DO NOTHING)
                   INSERT one deliveries row per subscribed, enabled endpoint
                 ──► 202

worker goroutines (WORKER_COUNT), in any number of processes, loop:
  claim:   claim_deliveries(): lock eligible endpoints, then lease the oldest due
           row of each, up to ENDPOINT_MAX_INFLIGHT leased rows per endpoint
  send:    signed POST, SSRF-checked dial, DELIVERY_TIMEOUT
  record:  2xx → delivered, endpoint breaker reset
           otherwise → attempts+1 and retry after backoff (dead at MAX_ATTEMPTS);
                       endpoint failure count +1, paused if it reaches BREAKER_THRESHOLD
retention (hourly): delete finished deliveries, then orphaned events, older than RETENTION_PERIOD
```

- **Transactional outbox.** The event and its deliveries are written in a single transaction. Either the API returns 202 and every delivery row exists, or nothing was written.
- **Idempotent ingest.** `events.id` is the primary key, and inserts use `ON CONFLICT DO NOTHING`. A repeated ID returns 202 with `"duplicate": true` and creates nothing, even under concurrency (see the tests). The dedupe window equals `RETENTION_PERIOD`: once retention deletes an event, its ID can be accepted again.
- **Claiming with a lease.** Claiming a row moves its `next_attempt_at` forward by `LEASE_DURATION` (default 30s), which hides it from other workers. It also sets `lease_expires_at`, which counts the row as in flight. If a worker crashes, both expire and the row becomes due again. No reaper is needed.
- **Lease as a fencing token.** A worker records its result only if `next_attempt_at` still equals the value it set when it claimed the row. A worker whose lease expired cannot overwrite the result of whoever re-claimed the row.
- **Per-endpoint isolation.** One slow or broken endpoint can't stall the others:
  - **In-flight cap.** At most `ENDPOINT_MAX_INFLIGHT` attempts per endpoint run at once. `claim_deliveries` (`migrations/003_claim_function.sql`) keeps this exact under concurrency. It locks eligible endpoint rows with `SKIP LOCKED`, then counts leases in a second statement whose fresh snapshot sees every earlier claim. An earlier single-statement version overshot the cap in 3 of 10 test runs.
  - **Fair claiming.** Each claim takes the oldest due row *per endpoint*, so a large backlog for one endpoint doesn't delay the others.
  - **Circuit breaker.** After `BREAKER_THRESHOLD` consecutive failures the endpoint is paused for `BREAKER_COOLDOWN`, doubling on each further failure up to `BREAKER_MAX_COOLDOWN`. While it's paused, its deliveries are not attempted and don't use up attempts. A success resets the breaker.
  - **Trade-off.** Claims for one endpoint are serialized, which caps a single endpoint at roughly 1,100–1,300 deliveries/s on the test machine. Total throughput still scales across endpoints (see the benchmark).
- **SSRF protection.** `internal/netguard` checks the resolved IP of every outbound connection, inside the dialer. No proxy is used, so the check sees the real destination, and redirects are not followed. URLs that are obviously internal are also rejected at registration.
- **Secrets at rest.** Endpoint signing secrets are stored encrypted with AES-256-GCM (`v1:` + base64 of nonce‖ciphertext). Workers decrypt a secret only to sign a request. A delivery whose secret can't be decrypted fails instead of being sent unsigned.
- **Backoff.** `BACKOFF_BASE × 2^(attempt-1)`, capped at `BACKOFF_MAX`, with "equal jitter": the delay is uniform in `[d/2, d)`.
- **Delivery semantics: at-least-once.** If the process dies after the receiver handled a request but before the result was written, the delivery is sent again after its lease expires. Receivers should dedupe on `Webhook-Id`. Ordering is not guaranteed.

### Why Postgres instead of Kafka

- **One system, one transaction.** The outbox guarantee depends on the event and its delivery rows committing together. With Kafka you would need an outbox table plus a relay anyway, or a dual write that can lose or duplicate messages.
- **Per-message state is the core of the problem.** Webhooks need per-delivery retry schedules, per-endpoint pausing and concurrency limits, dead letters, and queries like "show me the dead deliveries for endpoint X". In Postgres those are rows, indexes and `UPDATE`s. Kafka is an append-only log with per-partition offsets: delayed per-message retries need extra retry topics or a scheduler, and querying status means building a separate store.
- **Enough throughput for a single tenant.** On a laptop: about 3,200 deliveries/s across 16 endpoints with an instant receiver, and about 2,260/s with 128 workers against a 50 ms receiver (see the benchmark).
- **Operationally simpler.** One database to run, back up and inspect with SQL.

Kafka (or similar) becomes worth it at much higher volume, or when many independent consumers need the same event stream. See the roadmap.

## Configuration

All settings come from environment variables. `.env.example` documents each one, `init-env.sh` fills in the secrets, and the scripts load `.env`.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | required | Postgres connection string |
| `DB_MAX_CONNS` | 20 | pgx pool size. Keep it above `WORKER_COUNT` plus API headroom. |
| `API_KEY` | required | Bearer token, at least 32 characters |
| `SECRET_ENCRYPTION_KEY` | required | 64 hex chars. Encrypts endpoint secrets. |
| `SECRET_ENCRYPTION_KEY_PREVIOUS` | empty | The old key, only while rotating |
| `LISTEN_ADDR` | `127.0.0.1:8080` | API listen address |
| `TLS_CERT_FILE` / `TLS_KEY_FILE` | empty | Serve HTTPS directly |
| `ALLOW_INSECURE_HTTP` | false | Allow plain HTTP on a non-loopback address (behind a TLS proxy) |
| `ALLOW_PRIVATE_DESTINATIONS` | false | Disables SSRF protection. Local development only. |
| `WORKER_COUNT` | 16 | Delivery workers per process |
| `ENDPOINT_MAX_INFLIGHT` | 4 | Max concurrent attempts per endpoint |
| `BREAKER_THRESHOLD` | 5 | Consecutive failures that pause an endpoint. 0 disables the breaker. |
| `BREAKER_COOLDOWN` / `BREAKER_MAX_COOLDOWN` | `30s` / `10m` | First pause and maximum pause |
| `MAX_ATTEMPTS` | 8 | Attempts before a delivery is marked `dead` |
| `BACKOFF_BASE` / `BACKOFF_MAX` | `1s` / `10m` | Retry backoff base and cap |
| `DELIVERY_TIMEOUT` | `5s` | Per-attempt HTTP timeout |
| `LEASE_DURATION` | `30s` | Claim lease. Must be at least `DELIVERY_TIMEOUT` + 2s. |
| `POLL_INTERVAL` | `250ms` | Maximum idle wait when nothing is claimable |
| `CLAIM_BATCH_SIZE` | 1 | Rows per claim (at most one per endpoint) |
| `RETENTION_PERIOD` / `RETENTION_INTERVAL` | `720h` / `1h` | Delete finished deliveries and their events after this. 0 disables. |
| `POSTGRES_*` | | Used by `docker-compose.yml` |
| `RECEIVER_*` | | Fake receiver: `ADDR`, `FAIL_RATE`, `SECRET`, `DELAY` |

## Running things

```bash
go build -o bin/server ./cmd/server && set -a && . ./.env && set +a && ./bin/server
```

**Tests.** The integration tests need the compose Postgres and use a separate `webhook_test` database. They read `DATABASE_URL` from the environment or from `.env`, and are skipped if neither is available.

```bash
go test -race ./...
```

**Benchmark.** Like the demo, it only runs against a local database and wipes the tables (the bench binary takes `-allow-remote-db` to override this):

```bash
./bench/run.sh                                    # 1 endpoint, workers 1/4/16/32, 0% and 30% failures
BENCH_ENDPOINTS=16 BENCH_FAIL_RATES=0 ./bench/run.sh
RECEIVER_DELAY=50ms BENCH_DB_MAX_CONNS=80 BENCH_ENDPOINTS=16 BENCH_WORKERS="16 32 64 128" BENCH_FAIL_RATES=0 ./bench/run.sh
```

| Test | What it checks |
|---|---|
| `TestFailingReceiverEndsDead` | An httptest receiver that always returns 503. The delivery ends `dead` after exactly `MAX_ATTEMPTS` signed attempts, with `last_status_code = 503`. |
| `TestRetryThenDelivered` | The receiver fails twice, then returns 204. The delivery ends `delivered` on attempt 3. |
| `TestDuplicateEventsAreIgnored` | 50 concurrent POSTs with the same event ID. 1 event, 1 delivery per endpoint, and each endpoint receives exactly 1 request. |
| `TestConcurrentWorkersDeliverEachRowOnce` | 300 deliveries and 16 workers. Every delivery is sent exactly once. |
| `TestSlowEndpointDoesNotStallOthers` | An endpoint that hangs until the timeout, 8 workers, and a cap of 2. A healthy endpoint's 40 deliveries finish quickly (104–186 ms in 5 runs; the test fails above 2 s), and the hanging endpoint never has more than 2 requests in flight (checked in 10 of 10 strict runs). |
| `TestCircuitBreaker` | The breaker opens after 3 failures. No requests are sent while it's paused, and no delivery dies. After recovery all are delivered and the breaker resets. |
| `TestSSRFProtection` | Internal URLs are rejected at registration. A worker refuses to connect to a loopback receiver even when the row already exists, and the receiver gets 0 requests. |
| `TestSecretsEncryptedAtRest` | Secrets are stored only as ciphertext. Legacy plaintext rows are encrypted at startup, and a key rotation re-seals every row. |
| `TestEndpointManagementAndRetry` | List and get hide secrets. Disabled endpoints get no deliveries. Rotate-secret works, and the receiver only accepts the new secret. Retry re-queues a dead delivery (409 for one that isn't dead, 404 for an unknown id). |
| `TestRetention` | Deletes old delivered and dead rows and orphaned events, keeps pending and recent rows, and a deleted event ID can be accepted again. |
| `TestMetricsAndReadiness` | `/readyz` works, `/metrics` needs the key, and the counters and gauges match what happened. |
| `TestAuthRequired` | 401 without a key. |
| Unit tests | Signing (tampering, wrong secret, stale timestamp), backoff bounds, SSRF address classes (including IPv4-mapped and NAT64), dialing `localhost` blocked at connect time, AES-GCM sealing, rotation and tamper detection, and config validation (key length, TLS rules, required settings). |

## Benchmark results

**Conditions (all runs):**
- Hardware: the author's MacBook (Apple M5 Pro, 15 cores, 24 GB, macOS 27).
- Postgres 16 in Docker Desktop on the same machine, default settings. Pool of 40 connections, or 80 for the slow-receiver runs.
- Server, fake receiver and load generator all on the same machine over localhost.
- 10,000 events per run, sent by 32 concurrent HTTP clients, with exactly one delivery per event. With 16 endpoints, events are spread evenly across them.
- Bench settings (`bench/run.sh`):
  - shortened retries: `BACKOFF_BASE=100ms`, `BACKOFF_MAX=2s`, `MAX_ATTEMPTS=8`;
  - circuit breaker off, and `ENDPOINT_MAX_INFLIGHT` equal to the worker count, so the queue itself is measured;
  - `ALLOW_PRIVATE_DESTINATIONS=true`, because the receiver is on localhost;
  - every request signed, but the receiver doesn't verify signatures.

"Deliveries/s" is delivered rows divided by the time from the first event's `created_at` to the last `delivered_at`. Ingest overlaps with delivery.

These are loopback numbers. Treat them as a ceiling for the queueing machinery, not a capacity plan.

### 1. Instant receiver, 16 endpoints: total throughput (3 runs)

| Workers | Deliveries/s, median | Range | POST /events p50 | POST /events p95 |
|---|---|---|---|---|
| 1 | 461 | 455–737 | 4.4–4.6 ms | 5.5–6.0 ms |
| 4 | 1,597 | 1,406–1,893 | 4.6–4.8 ms | 6.1 ms |
| 16 | 2,815 | 2,330–2,833 | 5.4 ms | 7.4–8.1 ms |
| 32 | 3,230 | 2,873–3,237 | 6.7–6.8 ms | 9.0–9.8 ms |

### 2. Instant receiver, 1 endpoint: the per-endpoint ceiling (3 runs)

| Workers | Deliveries/s, median | Range |
|---|---|---|
| 1 | 716 | 479–732 |
| 4 | 1,197 | 1,071–1,286 |
| 16 | 1,200 | 1,118–1,235 |
| 32 | 1,202 | 1,185–1,237 |

Beyond about 4 workers, a single endpoint stays around 1,100–1,300/s, because its claims are serialized to enforce the in-flight cap exactly (see Design). POST /events p95 was 5.7–6.6 ms in these runs.

**Earlier version, for comparison.** The previous version claimed the globally oldest row with a single `SKIP LOCKED` query and had no per-endpoint isolation. It reached a median of about 4,500/s on one endpoint with 32 workers (5 runs, same machine). The current version trades that peak for per-endpoint isolation: exact caps, fair claiming, and the circuit breaker.

### 3. Receiver taking 50 ms per request, 16 endpoints (`RECEIVER_DELAY=50ms`, 1 run each)

| Workers | Deliveries/s | Ideal (workers ÷ 50 ms) | % of ideal | Time to drain |
|---|---|---|---|---|
| 16 | 269 | 320 | 84% | 37.1 s |
| 32 | 569 | 640 | 89% | 17.6 s |
| 64 | 1,149 | 1,280 | 90% | 8.8 s |
| 128 | 2,261 | 2,560 | 88% | 4.5 s |

With realistic receiver latency, workers spend almost all their time waiting on HTTP, so throughput scales linearly with workers. Size `WORKER_COUNT` roughly as target rate × receiver latency. 128 workers ran on 80 database connections, because a worker only holds a connection while claiming or recording.

### 4. Instant receiver failing 30% of requests at random, 16 endpoints (1 run)

| Workers | Success rate | Dead | Avg attempts per delivery | Time to drain |
|---|---|---|---|---|
| 1 | 99.98% | 2 | 1.450 | 25.6 s |
| 4 | 99.99% | 1 | 1.442 | 15.8 s |
| 16 | 100% | 0 | 1.435 | 9.5 s |
| 32 | 99.99% | 1 | 1.416 | 9.0 s |

4 of 40,000 deliveries ended dead. At 8 attempts the expected number is 40,000 × 0.3⁸ ≈ 2.6. Average attempts match the theoretical 1/0.7 ≈ 1.43. With failures, drain time comes mostly from the retry backoff schedule.

### Other verified behaviour

- **Two server processes, one database:** 400 events across 4 endpoints, cap 3, and a 100 ms receiver. All 400 were delivered on the first attempt with 0 duplicates. Each endpoint peaked at exactly 3 concurrent requests, and the processes split the work 203/197.
- **Crash recovery:** `kill -9` while 4 deliveries were in flight, then restart. The endpoint stayed at its cap until the dead process's leases expired (`LEASE_DURATION`), then all 8 deliveries completed. In 4 of 5 runs, each delivery was recorded on its first attempt. In 1 run, one delivery needed a second attempt; the cause wasn't determined, and the final state was correct. The receiver saw the 4 interrupted requests twice, as expected for at-least-once delivery.
- **Database outage:** `/readyz` returns 503 while `/healthz` stays 200. `/metrics` reports `webhook_queue_stats_up 0`, the workers log and back off, and everything recovers when Postgres returns.
- **TLS and SSRF on a live server:**
  - HTTPS serves with a certificate, and plain HTTP on `0.0.0.0` is refused at startup.
  - `127.0.0.1` and `169.254.169.254` are rejected at registration.
  - In the container, `host.docker.internal` (which resolves to a private IP) passed registration but was blocked when the worker connected.

### Platforms tested

- macOS 27 (Apple Silicon), Go 1.27.1: demo, tests with `-race`, all benchmarks, and the container image.
- Linux (Debian 13, arm64, in a `golang:1.25` container on Docker Desktop), Go 1.25.14: `go vet` and all tests with `-race`. Native x86-64 Linux has not been tested.

## Limitations

- **Single tenant, single API key:** no per-client keys, scopes or API rate limiting.
- **Per-endpoint throughput:** a single endpoint tops out around 1,100–1,300 deliveries/s on the test machine, because its claims are serialized (see Design).
- **Endpoint count:** claiming considers every eligible endpoint, and the benchmarks used at most 16. Very large endpoint counts (tens of thousands) are untested.
- **Delivery guarantees:** at-least-once and unordered.
- **Attempt history:** only the latest attempt's `last_status_code` and `last_error` are kept.
- **Circuit breaker:** keeps probing a dead endpoint at `BREAKER_MAX_COOLDOWN` intervals indefinitely. There's no automatic disable, so disable it through the API.
- **Secret rotation:** no overlap window. The old secret stops being used immediately.
- **Encryption key:** `SECRET_ENCRYPTION_KEY` is read from the environment (no KMS integration).

## Roadmap

- **Multi-tenancy:** accounts, per-tenant API keys with scopes, and tenant-scoped endpoints and deliveries.
- **Rate limits:** per-key ingest limits, and per-endpoint request-rate limits on top of the concurrency cap.
- **Smarter endpoint health:** honor `Retry-After` on 429 and 503, auto-disable after prolonged failure, and notify the owner.
- **Per-attempt history:** a `delivery_attempts` table with status, latency, error, and a truncated response body.
- **Bulk replay:** by endpoint and time range, on top of the single-delivery retry.
- **Secrets:** KMS-backed key management, and dual-secret signing during rotation.
- **Kafka at scale:** keep the Postgres outbox as the source of truth, and relay it into Kafka (or similar) once volume or the number of consumers outgrows one database.

## License

MIT, see [LICENSE](LICENSE).
