[← back to the readme](../README.md)

# api...

every request except `GET /healthz` and `GET /readyz` needs `Authorization: Bearer $API_KEY`.

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

| route | notes |
|---|---|
| `GET /healthz`, `GET /readyz` | no auth. `/readyz` returns 503 if the database is unreachable. |
| `POST /endpoints` | `url` must be absolute http(s), with no credentials, no private or loopback ip, and no `localhost`. `event_types` must be non-empty. returns a fresh `whsec_…` secret. |
| `GET /endpoints`, `GET /endpoints/{id}` | never include the secret. show `disabled_at`, `consecutive_failures` and `paused_until` (circuit breaker). |
| `POST /endpoints/{id}/disable` / `enable` | a disabled endpoint gets no new deliveries, and its pending ones wait. enabling also resets the circuit breaker. |
| `POST /endpoints/{id}/rotate-secret` | returns a new secret. the old one stops being used immediately. |
| `POST /events` | `id`, `type` and `payload` (any non-null json) are required. bodies are capped at 1 MiB. |
| `GET /deliveries` | optional `status` (`pending`, `delivered` or `dead`) and `limit` (1–1000, default 100). newest first. |
| `GET /deliveries/{id}` | `status`, `attempts`, `next_attempt_at` (null once delivered or dead), `last_status_code`, `last_error` and timestamps. |
| `POST /deliveries/{id}/retry` | re-queues a **dead** delivery with fresh attempts. 409 otherwise. |
| `GET /metrics` | prometheus text format. |

## what a receiver gets...

```
POST <endpoint url>
Content-Type: application/json
Webhook-Id: evt_123                 # event id: use it to dedupe
Webhook-Delivery-Id: 17
Webhook-Timestamp: 1760457600       # unix seconds
Webhook-Signature: v1=<hex HMAC-SHA256(secret, "<timestamp>.<raw body>")>

{"id":"evt_123","type":"order.created","created_at":"…","payload":{"order_id":42}}
```

to verify a request, recompute the hmac over the timestamp, a `.`, and the **raw** body. compare it with the header in constant time, and reject timestamps that are too old. `internal/signing.Verify` does all of that, and `cmd/receiver` uses it.

any 2xx counts as delivered. anything else is a failure: 3xx (redirects aren't followed), 4xx, timeouts (`DELIVERY_TIMEOUT`, default 5s), connection errors, and destinations blocked by ssrf protection.
