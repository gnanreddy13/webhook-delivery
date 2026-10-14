[← back to the readme](../README.md)

# running & testing...

```bash
go build -o bin/server ./cmd/server && set -a && . ./.env && set +a && ./bin/server
```

**tests.** the integration tests need the compose postgres and use a separate `webhook_test` database. they read `DATABASE_URL` from the environment or from `.env`, and skip if neither is there.

```bash
go test -race ./...
```

**benchmarks.** like the demo, they only run against a local database and wipe the tables (the bench binary takes `-allow-remote-db` to override that):

```bash
./bench/run.sh                                    # 1 endpoint, workers 1/4/16/32, 0% and 30% failures
BENCH_ENDPOINTS=16 BENCH_FAIL_RATES=0 ./bench/run.sh
RECEIVER_DELAY=50ms BENCH_DB_MAX_CONNS=80 BENCH_ENDPOINTS=16 BENCH_WORKERS="16 32 64 128" BENCH_FAIL_RATES=0 ./bench/run.sh
```

| test | what it checks |
|---|---|
| `TestFailingReceiverEndsDead` | an httptest receiver that always returns 503. the delivery ends `dead` after exactly `MAX_ATTEMPTS` signed attempts, with `last_status_code = 503`. |
| `TestRetryThenDelivered` | the receiver fails twice, then returns 204. the delivery ends `delivered` on attempt 3. |
| `TestDuplicateEventsAreIgnored` | 50 concurrent POSTs with the same event id. 1 event, 1 delivery per endpoint, and each endpoint gets exactly 1 request. |
| `TestConcurrentWorkersDeliverEachRowOnce` | 300 deliveries and 16 workers. every delivery is sent exactly once. |
| `TestSlowEndpointDoesNotStallOthers` | an endpoint that answers after 400 ms (its oldest backlog queued first), 8 workers, a cap of 2. a healthy endpoint's 40 deliveries finish within 500 ms (42–68 ms in 10 runs), and the slow endpoint never has more than 2 attempts in flight, sampled from the database and seen by the receiver (10 of 10 runs). |
| `TestHangingEndpointDoesNotStallOthers` | same, but the endpoint never answers, so every attempt times out. the sender never has more than 2 in flight (10 of 10 runs), and the healthy endpoint isn't held back. the receiver briefly sees 4, because abandoned requests linger on its side; that's logged, not asserted. |
| `TestClaimSkipsEndpointsAtCap` | the endpoint with the oldest work is at its cap; a single claim must hand out the other endpoint's delivery. fails without migration 005 (the claim came back empty). |
| `TestCircuitBreaker` | the breaker opens after 3 failures. no requests go out while it's paused, and no delivery dies. after recovery everything is delivered and the breaker resets. |
| `TestSSRFProtection` | internal urls are rejected at registration. a worker refuses to connect to a loopback receiver even when the row already exists, and the receiver gets 0 requests. |
| `TestSecretsEncryptedAtRest` | secrets are stored only as ciphertext. legacy plaintext rows get encrypted at startup, and a key rotation re-seals every row. |
| `TestEndpointManagementAndRetry` | list and get hide secrets. disabled endpoints get no deliveries. rotate-secret works, and the receiver only accepts the new secret. retry re-queues a dead delivery (409 for one that isn't dead, 404 for an unknown id). |
| `TestRetention` | deletes old delivered and dead rows and orphaned events, keeps pending and recent rows, and a deleted event id can be accepted again. |
| `TestMetricsAndReadiness` | `/readyz` works, `/metrics` needs the key, and the counters and gauges match what happened. |
| `TestAuthRequired` | 401 without a key. |
| unit tests | signing (tampering, wrong secret, stale timestamp), backoff bounds, ssrf address classes (incl. ipv4-mapped and nat64), dialing `localhost` blocked at connect time, AES-GCM sealing, rotation and tamper detection, and config validation (key length, tls rules, required settings). |
