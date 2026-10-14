[← back to the readme](../README.md)

# how it works...

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

- **transactional outbox.** the event and its deliveries go in one transaction. either the api returns 202 and every delivery row exists, or nothing was written.
- **idempotent ingest.** `events.id` is the primary key and inserts use `ON CONFLICT DO NOTHING`. a repeated id returns 202 with `"duplicate": true` and creates nothing, even under concurrency (see the tests). the dedupe window is `RETENTION_PERIOD`: once retention deletes an event, its id can be accepted again.
- **claiming with a lease.** claiming a row moves its `next_attempt_at` forward by `LEASE_DURATION` (default 30s), which hides it from other workers. it also sets `lease_expires_at`, which counts it as in flight. if a worker crashes, both expire and the row is due again. no reaper needed.
- **the lease is also a fencing token.** a worker only records its result if `next_attempt_at` still equals what it set when it claimed the row. a worker whose lease expired can't overwrite whoever re-claimed the row.
- **per-endpoint isolation.** one slow or broken endpoint can't stall the others:
  - **in-flight cap.** at most `ENDPOINT_MAX_INFLIGHT` attempts per endpoint at once. `claim_deliveries` (`migrations/003_claim_function.sql`) keeps this exact under concurrency: it locks eligible endpoint rows with `SKIP LOCKED`, then counts leases in a second statement whose fresh snapshot sees every earlier claim. my first single-statement version overshot the cap in 3 of 10 test runs.
  - **fair claiming.** each claim takes the oldest due row *per endpoint*, so a big backlog on one endpoint doesn't delay the rest.
  - **circuit breaker.** after `BREAKER_THRESHOLD` consecutive failures the endpoint is paused for `BREAKER_COOLDOWN`, doubling on each further failure up to `BREAKER_MAX_COOLDOWN`. while it's paused, its deliveries aren't attempted and don't burn attempts. one success resets it.
  - **the trade-off.** claims for one endpoint are serialized, which caps a single endpoint at roughly 1,100–1,300 deliveries/s on my machine. total throughput still scales across endpoints (see [benchmarks](benchmarks.md)).
- **ssrf protection.** `internal/netguard` checks the resolved ip of every outbound connection, inside the dialer. no proxy is used, so the check sees the real destination, and redirects aren't followed. obviously internal urls are also rejected at registration.
- **secrets at rest.** endpoint signing secrets are stored encrypted with AES-256-GCM (`v1:` + base64 of nonce‖ciphertext). workers decrypt a secret only to sign a request. a delivery whose secret can't be decrypted fails instead of going out unsigned.
- **backoff.** `BACKOFF_BASE × 2^(attempt-1)`, capped at `BACKOFF_MAX`, with "equal jitter": the delay is uniform in `[d/2, d)`.
- **at-least-once delivery.** if the process dies after the receiver handled a request but before the result was written, the delivery goes out again after its lease expires. receivers should dedupe on `Webhook-Id`. ordering isn't guaranteed.
