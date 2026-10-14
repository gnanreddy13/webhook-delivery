// Package tests holds integration tests that need the docker-compose
// Postgres. They use a separate "webhook_test" database, created on demand,
// and are skipped if Postgres is not reachable.
package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gnanreddy13/webhook-delivery/internal/api"
	"github.com/gnanreddy13/webhook-delivery/internal/db"
	"github.com/gnanreddy13/webhook-delivery/internal/signing"
	"github.com/gnanreddy13/webhook-delivery/internal/store"
	"github.com/gnanreddy13/webhook-delivery/internal/worker"
)

const apiKey = "test-api-key"

// testDB returns a pool on a freshly migrated and emptied test database.
func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	adminURL := envOr("DATABASE_URL", "postgres://webhook:change-me-local-only@127.0.0.1:5432/webhook?sslmode=disable")
	admin, err := db.Connect(ctx, adminURL, 0)
	if err != nil {
		t.Skipf("Postgres not reachable (start it with docker compose up -d): %v", err)
	}
	_, err = admin.Exec(ctx, "CREATE DATABASE webhook_test")
	admin.Close()
	var pgErr *pgconn.PgError
	if err != nil && !(errors.As(err, &pgErr) && pgErr.Code == "42P04") { // 42P04 = already exists
		t.Fatalf("create test database: %v", err)
	}

	u, _ := url.Parse(adminURL)
	u.Path = "/webhook_test"
	pool, err := db.Connect(ctx, u.String(), 30)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE deliveries, events, endpoints"); err != nil {
		t.Fatal(err)
	}
	return pool
}

// startWorkers runs a worker pool with fast retries until the test ends.
func startWorkers(t *testing.T, st *store.Store, workers, maxAttempts int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	p := worker.New(worker.Config{
		Workers:      workers,
		BatchSize:    1,
		MaxAttempts:  maxAttempts,
		BackoffBase:  10 * time.Millisecond,
		BackoffMax:   50 * time.Millisecond,
		Timeout:      2 * time.Second,
		Lease:        10 * time.Second,
		PollInterval: 10 * time.Millisecond,
	}, st)
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for condition")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func countWhere(t *testing.T, pool *pgxpool.Pool, where string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM deliveries WHERE "+where).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// apiCall sends an authenticated JSON request to the API handler.
func apiCall(t *testing.T, srv *httptest.Server, method, path, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func registerEndpoint(t *testing.T, srv *httptest.Server, target string) (id, secret string) {
	t.Helper()
	code, out := apiCall(t, srv, "POST", "/endpoints",
		fmt.Sprintf(`{"url":%q,"event_types":["order.created"]}`, target))
	if code != http.StatusCreated {
		t.Fatalf("register endpoint: HTTP %d %v", code, out)
	}
	return out["id"].(string), out["secret"].(string)
}

func TestAuthRequired(t *testing.T) {
	pool := testDB(t)
	srv := httptest.NewServer(api.NewRouter(store.New(pool), apiKey))
	defer srv.Close()

	for _, path := range []string{"/deliveries", "/deliveries/1"} {
		resp, _ := http.Get(srv.URL + path)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("GET %s without key: HTTP %d, want 401", path, resp.StatusCode)
		}
	}
	resp, _ := http.Post(srv.URL+"/events", "application/json", strings.NewReader(`{}`))
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("POST /events without key: HTTP %d, want 401", resp.StatusCode)
	}
	resp, _ = http.Get(srv.URL + "/healthz")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /healthz: HTTP %d, want 200", resp.StatusCode)
	}
}

// A receiver that always fails must end in "dead" after exactly MaxAttempts
// signed attempts, with the last status code recorded.
func TestFailingReceiverEndsDead(t *testing.T) {
	pool := testDB(t)
	st := store.New(pool)
	srv := httptest.NewServer(api.NewRouter(st, apiKey))
	defer srv.Close()

	var hits atomic.Int32
	var secret string
	var badSigs atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if signing.Verify(secret, r.Header.Get(signing.TimestampHeader),
			r.Header.Get(signing.SignatureHeader), body, time.Minute, time.Now()) != nil {
			badSigs.Add(1)
		}
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer receiver.Close()

	_, secret = registerEndpoint(t, srv, receiver.URL)
	const maxAttempts = 4
	startWorkers(t, st, 2, maxAttempts)

	code, _ := apiCall(t, srv, "POST", "/events", `{"id":"evt_fail","type":"order.created","payload":{"n":1}}`)
	if code != http.StatusAccepted {
		t.Fatalf("POST /events: HTTP %d", code)
	}
	waitFor(t, 10*time.Second, func() bool { return countWhere(t, pool, "status = 'dead'") == 1 })

	_, list := apiCall(t, srv, "GET", "/deliveries?status=dead", "")
	ds := list["deliveries"].([]any)
	if len(ds) != 1 {
		t.Fatalf("want 1 dead delivery, got %d", len(ds))
	}
	d := ds[0].(map[string]any)
	if d["next_attempt_at"] != nil {
		t.Errorf("dead delivery should have null next_attempt_at, got %v", d["next_attempt_at"])
	}
	if d["attempts"].(float64) != maxAttempts || d["last_status_code"].(float64) != 503 || d["last_error"] != "HTTP 503" {
		t.Errorf("unexpected dead delivery: %v", d)
	}
	if got := hits.Load(); got != maxAttempts {
		t.Errorf("receiver got %d requests, want %d", got, maxAttempts)
	}
	if badSigs.Load() != 0 {
		t.Errorf("%d requests had invalid signatures", badSigs.Load())
	}
}

// A receiver that fails twice and then succeeds ends "delivered" on attempt 3.
func TestRetryThenDelivered(t *testing.T) {
	pool := testDB(t)
	st := store.New(pool)
	srv := httptest.NewServer(api.NewRouter(st, apiKey))
	defer srv.Close()

	var hits atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()

	registerEndpoint(t, srv, receiver.URL)
	startWorkers(t, st, 2, 5)
	apiCall(t, srv, "POST", "/events", `{"id":"evt_retry","type":"order.created","payload":{}}`)
	waitFor(t, 10*time.Second, func() bool { return countWhere(t, pool, "status = 'delivered'") == 1 })

	if n := countWhere(t, pool, "attempts = 3 AND last_status_code = 204 AND last_error IS NULL"); n != 1 {
		t.Errorf("want delivered on attempt 3 with status 204")
	}
}

// Sending the same event ID many times, concurrently, must store one event,
// create one delivery per endpoint, and deliver each exactly once.
func TestDuplicateEventsAreIgnored(t *testing.T) {
	pool := testDB(t)
	st := store.New(pool)
	srv := httptest.NewServer(api.NewRouter(st, apiKey))
	defer srv.Close()

	var mu sync.Mutex
	received := map[string]int{} // endpoint path -> requests
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		received[r.URL.Path]++
		mu.Unlock()
	}))
	defer receiver.Close()

	registerEndpoint(t, srv, receiver.URL+"/a")
	registerEndpoint(t, srv, receiver.URL+"/b")

	const sends = 50
	var wg sync.WaitGroup
	var accepted, firsts atomic.Int32
	for range sends {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// No t.Fatal here: it must not be called from another goroutine.
			req, _ := http.NewRequest("POST", srv.URL+"/events",
				strings.NewReader(`{"id":"evt_dup","type":"order.created","payload":{"x":1}}`))
			req.Header.Set("Authorization", "Bearer "+apiKey)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			var out map[string]any
			json.NewDecoder(resp.Body).Decode(&out)
			if resp.StatusCode == http.StatusAccepted {
				accepted.Add(1)
			}
			if out["duplicate"] == false {
				firsts.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != sends || firsts.Load() != 1 {
		t.Fatalf("accepted=%d first-time=%d, want %d and 1", accepted.Load(), firsts.Load(), sends)
	}
	if n := countWhere(t, pool, "event_id = 'evt_dup'"); n != 2 {
		t.Fatalf("want 2 delivery rows (one per endpoint), got %d", n)
	}

	_, pending := apiCall(t, srv, "GET", "/deliveries?status=pending", "")
	for _, d := range pending["deliveries"].([]any) {
		if d.(map[string]any)["next_attempt_at"] == nil {
			t.Errorf("pending delivery should have next_attempt_at set")
		}
	}

	startWorkers(t, st, 8, 3)
	waitFor(t, 10*time.Second, func() bool { return countWhere(t, pool, "status = 'delivered'") == 2 })
	time.Sleep(200 * time.Millisecond) // give any stray duplicate send a chance to show up
	mu.Lock()
	defer mu.Unlock()
	if received["/a"] != 1 || received["/b"] != 1 {
		t.Errorf("want exactly one request per endpoint, got %v", received)
	}
}

// Many workers competing for many rows: SKIP LOCKED must hand each row to
// exactly one worker.
func TestConcurrentWorkersDeliverEachRowOnce(t *testing.T) {
	pool := testDB(t)
	st := store.New(pool)
	srv := httptest.NewServer(api.NewRouter(st, apiKey))
	defer srv.Close()

	var mu sync.Mutex
	seen := map[string]int{}
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.Header.Get("Webhook-Delivery-Id")]++
		mu.Unlock()
	}))
	defer receiver.Close()
	registerEndpoint(t, srv, receiver.URL)

	const events = 300
	for i := range events {
		apiCall(t, srv, "POST", "/events", fmt.Sprintf(`{"id":"evt_%d","type":"order.created","payload":{}}`, i))
	}
	startWorkers(t, st, 16, 3)
	waitFor(t, 20*time.Second, func() bool { return countWhere(t, pool, "status = 'delivered'") == events })

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != events {
		t.Errorf("receiver saw %d distinct deliveries, want %d", len(seen), events)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("delivery %s sent %d times", id, n)
		}
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
