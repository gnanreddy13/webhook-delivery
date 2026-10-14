[← back to the readme](../README.md)

# benchmarks...

**conditions (all runs):**
- my macbook (apple m5 pro, 15 cores, 24 GB, macos 27).
- postgres 16 in docker desktop on the same machine, default settings. pool of 40 connections, or 80 for the slow-receiver runs.
- server, fake receiver and load generator all on the same machine over localhost.
- 10,000 events per run, sent by 32 concurrent http clients, exactly one delivery per event. with 16 endpoints, events are spread evenly across them.
- bench settings (`bench/run.sh`):
  - shortened retries: `BACKOFF_BASE=100ms`, `BACKOFF_MAX=2s`, `MAX_ATTEMPTS=8`;
  - circuit breaker off, and `ENDPOINT_MAX_INFLIGHT` equal to the worker count, so the queue itself is what's measured;
  - `ALLOW_PRIVATE_DESTINATIONS=true`, because the receiver is on localhost;
  - every request signed, but the receiver doesn't check signatures.

"deliveries/s" is delivered rows divided by the time from the first event's `created_at` to the last `delivered_at`. ingest overlaps with delivery.

these are loopback numbers. read them as a ceiling for the queueing machinery, not a capacity plan.

to reproduce them, see [testing](testing.md#running--testing).

## 1. instant receiver, 16 endpoints: total throughput (3 runs)

| workers | deliveries/s, median | range | POST /events p50 | POST /events p95 |
|---|---|---|---|---|
| 1 | 461 | 455–737 | 4.4–4.6 ms | 5.5–6.0 ms |
| 4 | 1,597 | 1,406–1,893 | 4.6–4.8 ms | 6.1 ms |
| 16 | 2,815 | 2,330–2,833 | 5.4 ms | 7.4–8.1 ms |
| 32 | 3,230 | 2,873–3,237 | 6.7–6.8 ms | 9.0–9.8 ms |

## 2. instant receiver, 1 endpoint: the per-endpoint ceiling (3 runs)

| workers | deliveries/s, median | range |
|---|---|---|
| 1 | 716 | 479–732 |
| 4 | 1,197 | 1,071–1,286 |
| 16 | 1,200 | 1,118–1,235 |
| 32 | 1,202 | 1,185–1,237 |

past about 4 workers, a single endpoint sits around 1,100–1,300/s, because its claims are serialized to keep the in-flight cap exact (see [how it works](design.md)). POST /events p95 was 5.7–6.6 ms in these runs.

**earlier version, for comparison.** the previous version claimed the globally oldest row with a single `SKIP LOCKED` query and had no per-endpoint isolation. it reached a median of about 4,500/s on one endpoint with 32 workers (5 runs, same machine). the current version trades that peak for isolation: exact caps, fair claiming, and the circuit breaker.

## 3. receiver taking 50 ms per request, 16 endpoints (`RECEIVER_DELAY=50ms`, 1 run each)

| workers | deliveries/s | ideal (workers ÷ 50 ms) | % of ideal | time to drain |
|---|---|---|---|---|
| 16 | 269 | 320 | 84% | 37.1 s |
| 32 | 569 | 640 | 89% | 17.6 s |
| 64 | 1,149 | 1,280 | 90% | 8.8 s |
| 128 | 2,261 | 2,560 | 88% | 4.5 s |

with realistic receiver latency, workers spend almost all their time waiting on http, so throughput scales linearly with workers. size `WORKER_COUNT` roughly as target rate × receiver latency. 128 workers ran fine on 80 database connections, because a worker only holds a connection while claiming or recording.

## 4. instant receiver failing 30% of requests at random, 16 endpoints (1 run)

| workers | success rate | dead | avg attempts per delivery | time to drain |
|---|---|---|---|---|
| 1 | 99.98% | 2 | 1.450 | 25.6 s |
| 4 | 99.99% | 1 | 1.442 | 15.8 s |
| 16 | 100% | 0 | 1.435 | 9.5 s |
| 32 | 99.99% | 1 | 1.416 | 9.0 s |

4 of 40,000 deliveries ended dead. at 8 attempts the expected number is 40,000 × 0.3⁸ ≈ 2.6. average attempts match the theoretical 1/0.7 ≈ 1.43. with failures, drain time is mostly the retry backoff schedule.

## other verified behaviour...

- **two server processes, one database:** 400 events across 4 endpoints, cap 3, 100 ms receiver. all 400 delivered on the first attempt, 0 duplicates. each endpoint peaked at exactly 3 concurrent requests, and the processes split the work 203/197.
- **crash recovery:** `kill -9` with 4 deliveries in flight, then restart. the endpoint stayed at its cap until the dead process's leases expired (`LEASE_DURATION`), then all 8 deliveries completed. in 4 of 5 runs each delivery was recorded on its first attempt. in 1 run one delivery needed a second attempt; i didn't find the cause, and the final state was correct. the receiver saw the 4 interrupted requests twice, as expected for at-least-once delivery.
- **database outage:** `/readyz` returns 503 while `/healthz` stays 200. `/metrics` reports `webhook_queue_stats_up 0`, the workers log and back off, and everything recovers when postgres comes back.
- **tls & ssrf on a live server:**
  - https serves with a certificate, and plain http on `0.0.0.0` is refused at startup.
  - `127.0.0.1` and `169.254.169.254` are rejected at registration.
  - in the container, `host.docker.internal` (resolves to a private ip) passed registration but was blocked when the worker connected.

## platforms tested...

- macos 27 (apple silicon), go 1.27.1: demo, tests with `-race`, all benchmarks, and the container image.
- linux (debian 13, arm64, in a `golang:1.25` container on docker desktop), go 1.25.14: `go vet` and all tests with `-race`.
- native x86-64 linux, through [ci](../.github/workflows/ci.yml) on github's ubuntu 24.04 runners on every push:
  - gofmt, `go vet` and all tests with `-race` against a postgres 16 service, on go 1.25.14 and the latest stable go (1.27.2 at the time of writing). in ci the integration tests fail instead of skipping if postgres is unreachable.
  - the quick start above (`init-env.sh`, `docker compose up`, `demo.sh`) and a container image build, on go 1.25.0.
