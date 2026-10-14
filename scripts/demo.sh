#!/usr/bin/env bash
# End-to-end demo: server + fake receiver + Postgres.
#
# WARNING: wipes the endpoints/events/deliveries tables of the docker-compose
# database before running.
#
# Registers two endpoints for "order.created":
#   /hook  flaky receiver (fails RECEIVER_FAIL_RATE of requests), checks signatures
#   /fail  always returns 500, so its deliveries end up dead
# then sends DEMO_EVENTS events (plus duplicates) and waits for the queue to drain.
set -euo pipefail
cd "$(dirname "$0")/.."

[[ -f .env ]] || { echo "no .env: run ./scripts/init-env.sh first" >&2; exit 1; }
set -a; . ./.env; set +a

# The demo wipes tables; refuse to run against anything but a local database.
DB_HOST=$(sed -E 's#^[a-z]+://([^@/]*@)?(\[[^]]*\]|[^:/?]*).*#\2#' <<<"$DATABASE_URL")
case "$DB_HOST" in
  localhost|127.0.0.1|"[::1]") ;;
  *) echo "refusing to run: DATABASE_URL host '$DB_HOST' is not local (the demo wipes tables)" >&2; exit 1 ;;
esac

# The fake receiver runs on localhost, so the demo must turn SSRF protection
# off. Never set this in a real deployment.
export ALLOW_PRIVATE_DESTINATIONS=true
# Circuit breaker off so the always-failing /fail endpoint reaches "dead"
# within seconds instead of being paused (the breaker has its own tests).
export BREAKER_THRESHOLD=0

# Short backoff so retries and dead letters happen within seconds.
export BACKOFF_BASE="${DEMO_BACKOFF_BASE:-100ms}" BACKOFF_MAX="${DEMO_BACKOFF_MAX:-2s}"
export MAX_ATTEMPTS="${DEMO_MAX_ATTEMPTS:-6}"
EVENTS="${DEMO_EVENTS:-200}"
API="http://${LISTEN_ADDR}"
RECV="http://${RECEIVER_ADDR}"
AUTH="Authorization: Bearer ${API_KEY}"
LOG_DIR="$(mktemp -d)"

psql() { docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -X -q "$@"; }

cleanup() {
  [[ -n "${RECV_PID:-}" ]] && kill "$RECV_PID" 2>/dev/null || true
  [[ -n "${SERVER_PID:-}" ]] && kill -TERM "$SERVER_PID" 2>/dev/null && wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "==> building"
go build -o bin/server ./cmd/server
go build -o bin/receiver ./cmd/receiver

echo "==> starting server (workers=$WORKER_COUNT, max_attempts=$MAX_ATTEMPTS, backoff=$BACKOFF_BASE..$BACKOFF_MAX); logs in $LOG_DIR"
./bin/server >"$LOG_DIR/server.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 50); do curl -sf "$API/healthz" >/dev/null && break; sleep 0.2; done
curl -sf "$API/healthz" >/dev/null || { cat "$LOG_DIR/server.log"; exit 1; }

echo "==> resetting demo tables"
psql -c "TRUNCATE deliveries, events, endpoints"

echo "==> registering endpoints"
register() {
  curl -sf -H "$AUTH" -X POST "$API/endpoints" \
    -d "{\"url\":\"$RECV$1\",\"event_types\":[\"order.created\"]}"
}
HOOK_JSON=$(register /hook)
register /fail >/dev/null
SECRET=$(sed -E 's/.*"secret":"([^"]+)".*/\1/' <<<"$HOOK_JSON")

echo "==> starting receiver (fail rate $RECEIVER_FAIL_RATE)"
RECEIVER_SECRET="$SECRET" ./bin/receiver >"$LOG_DIR/receiver.log" 2>&1 &
RECV_PID=$!
for _ in $(seq 50); do curl -sf "$RECV/stats" >/dev/null && break; sleep 0.1; done

echo "==> sending $EVENTS events, then re-sending the first 50 (duplicates)"
send() {
  curl -sf -o /dev/null -w '%{http_code}\n' -H "$AUTH" -X POST "$API/events" \
    -d "{\"id\":\"evt_$1\",\"type\":\"order.created\",\"payload\":{\"order_id\":$1,\"amount\":$((RANDOM % 1000))}}"
}
codes=$( { for i in $(seq "$EVENTS"); do send "$i"; done; for i in $(seq 50); do send "$i"; done; } | sort | uniq -c)
echo "    HTTP status counts: $(echo $codes)"

echo "==> waiting for pending deliveries to drain"
for _ in $(seq 120); do
  pending=$(psql -tA -c "SELECT count(*) FROM deliveries WHERE status = 'pending'")
  [[ "$pending" == 0 ]] && break
  sleep 0.5
done

echo "==> results"
psql -c "SELECT substring(ep.url from '[^/]+$') AS endpoint, d.status, count(*) AS deliveries,
                round(avg(d.attempts), 2) AS avg_attempts, max(d.attempts) AS max_attempts
         FROM deliveries d JOIN endpoints ep ON ep.id = d.endpoint_id
         GROUP BY 1, 2 ORDER BY 1, 2"
psql -c "SELECT count(*) AS events, (SELECT count(*) FROM deliveries) AS deliveries,
                (SELECT count(*) FROM (SELECT event_id, endpoint_id FROM deliveries
                  GROUP BY 1, 2 HAVING count(*) > 1) x) AS duplicate_delivery_rows
         FROM events"
echo "receiver /stats: $(curl -s "$RECV/stats")"
DEAD_ID=$(psql -tA -c "SELECT id FROM deliveries WHERE status = 'dead' ORDER BY id LIMIT 1")
echo "sample dead delivery (GET /deliveries/$DEAD_ID):"
curl -s -H "$AUTH" "$API/deliveries/$DEAD_ID"
echo
[[ "$pending" == 0 ]] || { echo "FAIL: $pending deliveries still pending"; exit 1; }
echo "==> demo complete"
