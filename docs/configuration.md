[← back to the readme](../README.md)

# config...

everything comes from environment variables. `.env.example` documents each one, `init-env.sh` fills in the secrets, and the scripts load `.env`.

| variable | default | meaning |
|---|---|---|
| `DATABASE_URL` | required | postgres connection string |
| `DB_MAX_CONNS` | 20 | pgx pool size. keep it above `WORKER_COUNT` plus api headroom. |
| `API_KEY` | required | bearer token, at least 32 characters |
| `SECRET_ENCRYPTION_KEY` | required | 64 hex chars. encrypts endpoint secrets. |
| `SECRET_ENCRYPTION_KEY_PREVIOUS` | empty | the old key, only while rotating |
| `LISTEN_ADDR` | `127.0.0.1:8080` | api listen address |
| `TLS_CERT_FILE` / `TLS_KEY_FILE` | empty | serve https directly |
| `ALLOW_INSECURE_HTTP` | false | allow plain http on a non-loopback address (behind a tls proxy) |
| `ALLOW_PRIVATE_DESTINATIONS` | false | turns off ssrf protection. local development only. |
| `WORKER_COUNT` | 16 | delivery workers per process |
| `ENDPOINT_MAX_INFLIGHT` | 4 | max concurrent attempts per endpoint |
| `BREAKER_THRESHOLD` | 5 | consecutive failures that pause an endpoint. 0 turns the breaker off. |
| `BREAKER_COOLDOWN` / `BREAKER_MAX_COOLDOWN` | `30s` / `10m` | first pause and longest pause |
| `MAX_ATTEMPTS` | 8 | attempts before a delivery is marked `dead` |
| `BACKOFF_BASE` / `BACKOFF_MAX` | `1s` / `10m` | retry backoff base and cap |
| `DELIVERY_TIMEOUT` | `5s` | per-attempt http timeout |
| `LEASE_DURATION` | `30s` | claim lease. must be at least `DELIVERY_TIMEOUT` + 2s. |
| `POLL_INTERVAL` | `250ms` | longest idle wait when nothing is claimable |
| `CLAIM_BATCH_SIZE` | 1 | rows per claim (at most one per endpoint) |
| `RETENTION_PERIOD` / `RETENTION_INTERVAL` | `720h` / `1h` | delete finished deliveries and their events after this. 0 turns it off. |
| `POSTGRES_*` | | used by `docker-compose.yml` |
| `RECEIVER_*` | | fake receiver: `ADDR`, `FAIL_RATE`, `SECRET`, `DELAY` |
