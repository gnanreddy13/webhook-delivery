#!/usr/bin/env bash
# Runs the benchmark for each worker count and receiver failure rate.
# Requires `docker compose up -d`. WARNING: truncates the demo tables.
#
#   BENCH_WORKERS="1 4 16 32" BENCH_FAIL_RATES="0 0.3" BENCH_EVENTS=10000 bench/run.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -f .env ]]; then set -a; . ./.env; set +a; else set -a; . ./.env.example; set +a; fi

WORKERS_LIST="${BENCH_WORKERS:-1 4 16 32}"
FAIL_RATES="${BENCH_FAIL_RATES:-0 0.3}"
EVENTS="${BENCH_EVENTS:-10000}"
CONCURRENCY="${BENCH_CONCURRENCY:-32}"
# Short backoff so a 30%-failure run drains in seconds rather than minutes.
export BACKOFF_BASE="${BENCH_BACKOFF_BASE:-100ms}" BACKOFF_MAX="${BENCH_BACKOFF_MAX:-2s}"
export MAX_ATTEMPTS="${BENCH_MAX_ATTEMPTS:-8}"
export DB_MAX_CONNS="${DB_MAX_CONNS:-40}"
LOG_DIR="$(mktemp -d)"

go build -o bin/server ./cmd/server
go build -o bin/receiver ./cmd/receiver
go build -o bin/bench ./bench

cleanup() {
  [[ -n "${RECV_PID:-}" ]] && kill "$RECV_PID" 2>/dev/null || true
  [[ -n "${SERVER_PID:-}" ]] && kill -TERM "$SERVER_PID" 2>/dev/null && wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "events=$EVENTS concurrency=$CONCURRENCY max_attempts=$MAX_ATTEMPTS backoff=$BACKOFF_BASE..$BACKOFF_MAX db_max_conns=$DB_MAX_CONNS"
echo "host: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), logs in $LOG_DIR"

for rate in $FAIL_RATES; do
  # The receiver does not check signatures here (the endpoint secret is
  # generated per run); signing still happens on every attempt.
  RECEIVER_FAIL_RATE="$rate" ./bin/receiver >"$LOG_DIR/receiver-$rate.log" 2>&1 &
  RECV_PID=$!
  for w in $WORKERS_LIST; do
    WORKER_COUNT="$w" ./bin/server >"$LOG_DIR/server-$rate-$w.log" 2>&1 &
    SERVER_PID=$!
    for _ in $(seq 50); do curl -sf "http://$LISTEN_ADDR/healthz" >/dev/null && break; sleep 0.2; done
    ./bin/bench -api "http://$LISTEN_ADDR" -receiver "http://$RECEIVER_ADDR/hook" \
      -events "$EVENTS" -concurrency "$CONCURRENCY" -label "workers=$w receiver_fail_rate=$rate"
    kill -TERM "$SERVER_PID"; wait "$SERVER_PID" 2>/dev/null || true; SERVER_PID=
  done
  kill "$RECV_PID"; wait "$RECV_PID" 2>/dev/null || true; RECV_PID=
done
