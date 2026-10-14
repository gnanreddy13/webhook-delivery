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
	"github.com/gnanreddy13/webhook-delivery/internal/metrics"
	"github.com/gnanreddy13/webhook-delivery/internal/retention"
	"github.com/gnanreddy13/webhook-delivery/internal/secretbox"
	"github.com/gnanreddy13/webhook-delivery/internal/signing"
	"github.com/gnanreddy13/webhook-delivery/internal/store"
	"github.com/gnanreddy13/webhook-delivery/internal/worker"
)

const apiKey = "test-api-key"

var testBox, _ = secretbox.New(strings.Repeat("ab", 32), "")

// testDB returns a pool on a freshly migrated and emptied test database.
func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	adminURL := databaseURL()
	if adminURL == "" {
		skipOrFail(t, "DATABASE_URL not set and no ../.env found (run ./scripts/init-env.sh)")
	}
	admin, err := db.Connect(ctx, adminURL, 0)
	if err != nil {
		skipOrFail(t, fmt.Sprintf("Postgres not reachable (start it with docker compose up -d): %v", err))
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

// newRouter returns the API with SSRF checks off, because the test receivers
// are httptest servers on 127.0.0.1. TestSSRFProtection covers the checks.
func newRouter(st *store.Store) http.Handler {
	return api.NewRouter(st, api.Options{APIKey: apiKey, AllowPrivateDestinations: true})
}

// startWorkers runs a worker pool with fast retries until the test ends.
// The per-endpoint cap and circuit breaker are effectively off unless a
// test enables them through startPool.
func startWorkers(t *testing.T, st *store.Store, workers, maxAttempts int) {
	t.Helper()
	startPool(t, st, func(c *worker.Config) { c.Workers, c.MaxAttempts = workers, maxAttempts })
}

func startWorkersWith(t *testing.T, st *store.Store, workers, maxAttempts int, allowPrivate bool) {
	t.Helper()
	startPool(t, st, func(c *worker.Config) {
		c.Workers, c.MaxAttempts, c.AllowPrivateDestinations = workers, maxAttempts, allowPrivate
	})
}

func startPool(t *testing.T, st *store.Store, configure func(*worker.Config)) {
	t.Helper()
	cfg := worker.Config{
		Workers:      4,
		BatchSize:    1,
		MaxAttempts:  5,
		BackoffBase:  10 * time.Millisecond,
		BackoffMax:   50 * time.Millisecond,
		Timeout:      2 * time.Second,
		Lease:        10 * time.Second,
		PollInterval: 10 * time.Millisecond,

		AllowPrivateDestinations: true,
		EndpointMaxInflight:      1000,
	}
	configure(&cfg)
	ctx, cancel := context.WithCancel(context.Background())
	p := worker.New(cfg, st)
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
	srv := httptest.NewServer(newRouter(store.New(pool, testBox)))
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
	st := store.New(pool, testBox)
	srv := httptest.NewServer(newRouter(st))
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
	st := store.New(pool, testBox)
	srv := httptest.NewServer(newRouter(st))
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
	st := store.New(pool, testBox)
	srv := httptest.NewServer(newRouter(st))
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
	st := store.New(pool, testBox)
	srv := httptest.NewServer(newRouter(st))
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

// skipOrFail skips locally but fails in CI, so a broken database setup
// there can't make the integration tests silently pass.
func skipOrFail(t *testing.T, msg string) {
	t.Helper()
	if os.Getenv("CI") != "" {
		t.Fatal(msg)
	}
	t.Skip(msg)
}

// databaseURL returns $DATABASE_URL, falling back to the value in the
// repository's .env so `go test ./...` works after ./scripts/init-env.sh.
func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	data, err := os.ReadFile("../.env")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "DATABASE_URL="); ok {
			return v
		}
	}
	return ""
}

// With SSRF protection on, the API rejects internal URLs, and a worker
// refuses to connect to an internal address even if such a row exists.
func TestSSRFProtection(t *testing.T) {
	pool := testDB(t)
	st := store.New(pool, testBox)
	srv := httptest.NewServer(api.NewRouter(st, api.Options{APIKey: apiKey}))
	defer srv.Close()

	for _, target := range []string{"http://127.0.0.1:9/x", "http://localhost/x", "http://169.254.169.254/latest/meta-data", "http://10.0.0.1/x"} {
		code, out := apiCall(t, srv, "POST", "/endpoints", fmt.Sprintf(`{"url":%q,"event_types":["order.created"]}`, target))
		if code != http.StatusBadRequest {
			t.Errorf("registering %s: HTTP %d %v, want 400", target, code, out)
		}
	}

	var hits atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer receiver.Close()
	// Bypass the API check (e.g. a hostname that later resolves internally).
	if _, err := st.CreateEndpoint(context.Background(), receiver.URL, []string{"order.created"}, "whsec_x"); err != nil {
		t.Fatal(err)
	}
	startWorkersWith(t, st, 2, 2, false)
	apiCall(t, srv, "POST", "/events", `{"id":"evt_ssrf","type":"order.created","payload":{}}`)
	waitFor(t, 10*time.Second, func() bool { return countWhere(t, pool, "status = 'dead'") == 1 })

	if n := countWhere(t, pool, "last_error LIKE '%is not allowed%'"); n != 1 {
		t.Errorf("want dead delivery with a 'not allowed' error")
	}
	if hits.Load() != 0 {
		t.Errorf("internal receiver was reached %d times", hits.Load())
	}
}

// Secrets are stored encrypted, legacy plaintext rows get encrypted at
// startup, and a key rotation re-seals old rows with the new key.
func TestSecretsEncryptedAtRest(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	st := store.New(pool, testBox)
	ep, err := st.CreateEndpoint(ctx, "https://example.com/a", []string{"x"}, "whsec_new")
	if err != nil {
		t.Fatal(err)
	}
	// A row as it would have been written before encryption existed.
	if _, err := pool.Exec(ctx, `INSERT INTO endpoints (url, event_types, secret) VALUES ('https://example.com/b', '{x}', 'whsec_legacy')`); err != nil {
		t.Fatal(err)
	}
	stored := func() []string {
		rows, _ := pool.Query(ctx, `SELECT secret FROM endpoints ORDER BY url`)
		defer rows.Close()
		var out []string
		for rows.Next() {
			var s string
			rows.Scan(&s)
			out = append(out, s)
		}
		return out
	}
	if s := stored(); strings.Contains(s[0], "whsec_new") || !secretbox.IsSealed(s[0]) {
		t.Fatalf("new secret stored unencrypted: %q", s[0])
	}

	if n, err := st.ReencryptSecrets(ctx); err != nil || n != 1 {
		t.Fatalf("legacy migration: n=%d err=%v, want 1 row", n, err)
	}
	for _, s := range stored() {
		if !secretbox.IsSealed(s) || strings.Contains(s, "whsec_") {
			t.Fatalf("secret still plaintext after migration: %q", s)
		}
	}

	// Rotate keys: new current key, old key as previous.
	rotated, _ := secretbox.New(strings.Repeat("cd", 32), strings.Repeat("ab", 32))
	st2 := store.New(pool, rotated)
	if n, err := st2.ReencryptSecrets(ctx); err != nil || n != 2 {
		t.Fatalf("rotation: n=%d err=%v, want 2 rows", n, err)
	}
	newOnly, _ := secretbox.New(strings.Repeat("cd", 32), "")
	for _, s := range stored() {
		if _, _, err := newOnly.Open(s); err != nil {
			t.Fatalf("after rotation a secret still needs the old key: %v", err)
		}
	}
	_ = ep
}

func registerFor(t *testing.T, srv *httptest.Server, target, eventType string) (id, secret string) {
	t.Helper()
	code, out := apiCall(t, srv, "POST", "/endpoints", fmt.Sprintf(`{"url":%q,"event_types":[%q]}`, target, eventType))
	if code != http.StatusCreated {
		t.Fatalf("register endpoint: HTTP %d %v", code, out)
	}
	return out["id"].(string), out["secret"].(string)
}

// isolationResult is what runIsolation observed.
type isolationResult struct {
	healthyDone  time.Duration // time until all healthy deliveries were delivered
	senderPeak   int           // max leased (in-flight) deliveries for the slow endpoint, sampled from the DB
	receiverPeak int           // max concurrent requests the slow receiver saw
	slowAttempts int           // requests the slow receiver got
}

// runIsolation queues 40 deliveries for a slow endpoint (oldest) and 40 for
// a healthy one, starts 8 workers with an in-flight cap of 2 and a 1s
// delivery timeout, and measures how quickly the healthy endpoint is
// served and how many attempts the slow endpoint ever has in flight.
func runIsolation(t *testing.T, slowHandler func(http.ResponseWriter, *http.Request)) isolationResult {
	t.Helper()
	pool := testDB(t)
	st := store.New(pool, testBox)
	srv := httptest.NewServer(newRouter(st))
	defer srv.Close()

	var inflight, receiverPeak, slowAttempts atomic.Int32
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slowAttempts.Add(1)
		n := inflight.Add(1)
		defer inflight.Add(-1)
		for {
			m := receiverPeak.Load()
			if n <= m || receiverPeak.CompareAndSwap(m, n) {
				break
			}
		}
		slowHandler(w, r)
	}))
	defer slow.Close()
	fast := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer fast.Close()

	slowID, _ := registerFor(t, srv, slow.URL, "slow.event")
	registerFor(t, srv, fast.URL, "fast.event")
	// Queue the slow endpoint's backlog first so it is the oldest work.
	for i := range 40 {
		apiCall(t, srv, "POST", "/events", fmt.Sprintf(`{"id":"slow_%d","type":"slow.event","payload":{}}`, i))
	}
	for i := range 40 {
		apiCall(t, srv, "POST", "/events", fmt.Sprintf(`{"id":"fast_%d","type":"fast.event","payload":{}}`, i))
	}

	// Sample the sender's own view of in-flight attempts: live leases.
	var senderPeak atomic.Int32
	stopSampling := make(chan struct{})
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		for {
			select {
			case <-stopSampling:
				return
			default:
			}
			var n int32
			pool.QueryRow(context.Background(),
				`SELECT count(*) FROM deliveries WHERE endpoint_id = $1 AND lease_expires_at > clock_timestamp()`,
				slowID).Scan(&n)
			if n > senderPeak.Load() {
				senderPeak.Store(n)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	const workers, limit = 8, 2
	start := time.Now()
	startPool(t, st, func(c *worker.Config) {
		c.Workers, c.EndpointMaxInflight = workers, limit
		c.Timeout, c.MaxAttempts = time.Second, 100
	})
	waitFor(t, 5*time.Second, func() bool {
		return countWhere(t, pool, "status = 'delivered' AND event_id LIKE 'fast_%'") == 40
	})
	res := isolationResult{healthyDone: time.Since(start)}
	time.Sleep(1500 * time.Millisecond) // keep sampling the slow endpoint for a while
	close(stopSampling)
	<-samplerDone
	res.senderPeak = int(senderPeak.Load())
	res.receiverPeak = int(receiverPeak.Load())
	res.slowAttempts = int(slowAttempts.Load())
	t.Logf("healthy endpoint: 40 deliveries in %v; slow endpoint: sender peak %d, receiver peak %d, %d requests (cap %d, workers %d)",
		res.healthyDone.Round(time.Millisecond), res.senderPeak, res.receiverPeak, res.slowAttempts, limit, workers)

	if res.healthyDone > 500*time.Millisecond {
		t.Errorf("healthy endpoint took %v; the slow endpoint is holding it back", res.healthyDone)
	}
	if res.senderPeak > limit {
		t.Errorf("sender had %d attempts in flight to the slow endpoint, cap is %d", res.senderPeak, limit)
	}
	if res.slowAttempts == 0 {
		t.Error("slow endpoint was never attempted")
	}
	return res
}

// A slow endpoint (answers after 400ms, within the timeout) gets at most
// the cap's worth of concurrent requests, and doesn't delay a healthy
// endpoint. Because it answers, the receiver's view matches the sender's.
func TestSlowEndpointDoesNotStallOthers(t *testing.T) {
	res := runIsolation(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(400 * time.Millisecond):
		}
	})
	if res.receiverPeak > 2 {
		t.Errorf("slow receiver saw %d concurrent requests, cap is 2", res.receiverPeak)
	}
}

// An endpoint that never answers: every attempt times out. The sender
// still never has more than the cap in flight and the healthy endpoint is
// unaffected. The receiver may briefly see more than the cap, because an
// abandoned request lingers on its side until it notices the disconnect;
// that is logged, not asserted.
func TestHangingEndpointDoesNotStallOthers(t *testing.T) {
	runIsolation(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(1500 * time.Millisecond):
		}
	})
}

// After BreakerThreshold consecutive failures the endpoint is paused: no
// requests are sent and pending deliveries keep their remaining attempts.
// Once it recovers, the backlog is delivered and the breaker resets.
func TestCircuitBreaker(t *testing.T) {
	pool := testDB(t)
	st := store.New(pool, testBox)
	srv := httptest.NewServer(newRouter(st))
	defer srv.Close()

	var healthy atomic.Bool
	var hits atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if !healthy.Load() {
			w.WriteHeader(http.StatusBadGateway)
		}
	}))
	defer receiver.Close()
	epID, _ := registerFor(t, srv, receiver.URL, "order.created")

	for i := range 20 {
		apiCall(t, srv, "POST", "/events", fmt.Sprintf(`{"id":"cb_%d","type":"order.created","payload":{}}`, i))
	}
	startPool(t, st, func(c *worker.Config) {
		c.Workers, c.EndpointMaxInflight, c.MaxAttempts = 4, 1, 3
		c.Breaker = store.BreakerConfig{Threshold: 3, Cooldown: 700 * time.Millisecond, MaxCooldown: 700 * time.Millisecond}
	})

	waitFor(t, 5*time.Second, func() bool {
		_, ep := apiCall(t, srv, "GET", "/endpoints/"+epID, "")
		return ep["paused_until"] != nil
	})
	hitsWhenOpened := hits.Load()
	time.Sleep(400 * time.Millisecond) // well inside the 700ms pause
	if got := hits.Load(); got != hitsWhenOpened {
		t.Errorf("requests were sent while the breaker was open: %d -> %d", hitsWhenOpened, got)
	}
	if n := countWhere(t, pool, "status = 'dead'"); n > 1 {
		t.Errorf("%d deliveries died; a paused endpoint should not burn through attempts", n)
	}
	t.Logf("breaker opened after %d requests; %d deliveries still pending", hitsWhenOpened, countWhere(t, pool, "status = 'pending'"))

	healthy.Store(true)
	waitFor(t, 10*time.Second, func() bool { return countWhere(t, pool, "status = 'pending'") == 0 })
	if n := countWhere(t, pool, "status = 'delivered'"); n < 19 {
		t.Errorf("only %d of 20 delivered after recovery", n)
	}
	_, ep := apiCall(t, srv, "GET", "/endpoints/"+epID, "")
	if ep["consecutive_failures"].(float64) != 0 || ep["paused_until"] != nil {
		t.Errorf("breaker not reset after recovery: %v", ep)
	}
}

func TestEndpointManagementAndRetry(t *testing.T) {
	pool := testDB(t)
	st := store.New(pool, testBox)
	srv := httptest.NewServer(newRouter(st))
	defer srv.Close()

	var mu sync.Mutex
	secret := ""
	var accept atomic.Bool
	var verified, rejected atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		s := secret
		mu.Unlock()
		if signing.Verify(s, r.Header.Get(signing.TimestampHeader), r.Header.Get(signing.SignatureHeader), body, time.Minute, time.Now()) != nil {
			rejected.Add(1)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		verified.Add(1)
		if !accept.Load() {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer receiver.Close()

	id, s := registerFor(t, srv, receiver.URL, "order.created")
	mu.Lock()
	secret = s
	mu.Unlock()

	// List and get never expose secrets.
	_, list := apiCall(t, srv, "GET", "/endpoints", "")
	if eps := list["endpoints"].([]any); len(eps) != 1 || eps[0].(map[string]any)["secret"] != nil {
		t.Fatalf("unexpected endpoint list: %v", list)
	}
	if code, _ := apiCall(t, srv, "GET", "/endpoints/not-a-uuid", ""); code != http.StatusBadRequest {
		t.Errorf("bad id: HTTP %d", code)
	}
	if code, _ := apiCall(t, srv, "GET", "/endpoints/00000000-0000-0000-0000-000000000000", ""); code != http.StatusNotFound {
		t.Errorf("unknown id: HTTP %d", code)
	}

	// Disabled: no deliveries are created for new events.
	if code, ep := apiCall(t, srv, "POST", "/endpoints/"+id+"/disable", ""); code != http.StatusOK || ep["disabled_at"] == nil {
		t.Fatalf("disable: HTTP %d %v", code, ep)
	}
	if _, out := apiCall(t, srv, "POST", "/events", `{"id":"while_disabled","type":"order.created","payload":{}}`); out["deliveries"].(float64) != 0 {
		t.Errorf("disabled endpoint got a delivery: %v", out)
	}
	apiCall(t, srv, "POST", "/endpoints/"+id+"/enable", "")

	// Fail until dead, then retry it after the receiver recovers.
	startWorkers(t, st, 2, 2)
	apiCall(t, srv, "POST", "/events", `{"id":"to_retry","type":"order.created","payload":{}}`)
	waitFor(t, 5*time.Second, func() bool { return countWhere(t, pool, "status = 'dead'") == 1 })
	var deadID int64
	pool.QueryRow(context.Background(), "SELECT id FROM deliveries WHERE status = 'dead'").Scan(&deadID)

	if code, _ := apiCall(t, srv, "POST", fmt.Sprintf("/deliveries/%d/retry", deadID+1000), ""); code != http.StatusNotFound {
		t.Errorf("retry unknown delivery: HTTP %d", code)
	}

	// Rotate the secret; the receiver now only accepts the new one.
	code, rotated := apiCall(t, srv, "POST", "/endpoints/"+id+"/rotate-secret", "")
	if code != http.StatusOK || rotated["secret"] == nil || rotated["secret"] == s {
		t.Fatalf("rotate: HTTP %d %v", code, rotated)
	}
	mu.Lock()
	secret = rotated["secret"].(string)
	mu.Unlock()
	accept.Store(true)
	rejected.Store(0)

	code, d := apiCall(t, srv, "POST", fmt.Sprintf("/deliveries/%d/retry", deadID), "")
	if code != http.StatusOK || d["status"] != "pending" || d["attempts"].(float64) != 0 {
		t.Fatalf("retry: HTTP %d %v", code, d)
	}
	waitFor(t, 5*time.Second, func() bool { return countWhere(t, pool, "status = 'delivered'") == 1 })
	if rejected.Load() != 0 {
		t.Errorf("%d requests after rotation were signed with the old secret", rejected.Load())
	}
	if code, _ := apiCall(t, srv, "POST", fmt.Sprintf("/deliveries/%d/retry", deadID), ""); code != http.StatusConflict {
		t.Errorf("retrying a delivered delivery: HTTP %d, want 409", code)
	}
}

// Retention deletes old finished deliveries and then events with no
// deliveries left, never pending deliveries or recent rows.
func TestRetention(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	st := store.New(pool, testBox)
	ep, err := st.CreateEndpoint(ctx, "https://example.com/hook", []string{"t"}, "whsec_x")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"old_done", "old_dead", "old_pending", "new_done"} {
		if _, _, err := st.CreateEvent(ctx, store.Event{ID: id, Type: "t", Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	exec := func(q string) {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	exec(`UPDATE events SET created_at = now() - interval '40 days' WHERE id LIKE 'old_%'`)
	exec(`UPDATE deliveries SET status = 'delivered', updated_at = now() - interval '40 days' WHERE event_id = 'old_done'`)
	exec(`UPDATE deliveries SET status = 'dead', updated_at = now() - interval '40 days' WHERE event_id = 'old_dead'`)
	exec(`UPDATE deliveries SET updated_at = now() - interval '40 days' WHERE event_id = 'old_pending'`)
	exec(`UPDATE deliveries SET status = 'delivered' WHERE event_id = 'new_done'`)

	m := metrics.New()
	d, e := retention.RunOnce(ctx, st, 30*24*time.Hour, m)
	if d != 2 || e != 2 {
		t.Fatalf("deleted %d deliveries and %d events, want 2 and 2", d, e)
	}
	var remaining []string
	rows, _ := pool.Query(ctx, `SELECT id FROM events ORDER BY id`)
	for rows.Next() {
		var id string
		rows.Scan(&id)
		remaining = append(remaining, id)
	}
	rows.Close()
	if strings.Join(remaining, ",") != "new_done,old_pending" {
		t.Errorf("remaining events: %v", remaining)
	}
	if countWhere(t, pool, "event_id = 'old_pending' AND status = 'pending'") != 1 {
		t.Error("pending delivery was deleted")
	}
	// A deleted event ID can be accepted again.
	if created, n, _ := st.CreateEvent(ctx, store.Event{ID: "old_done", Type: "t", Payload: json.RawMessage(`{}`)}); !created || n != 1 {
		t.Errorf("re-sending a deleted event: created=%v deliveries=%d", created, n)
	}
	if m.RetentionDeliveries.Load() != 2 || m.RetentionEvents.Load() != 2 {
		t.Error("retention metrics not updated")
	}
	_ = ep
}

func TestMetricsAndReadiness(t *testing.T) {
	pool := testDB(t)
	st := store.New(pool, testBox)
	m := metrics.New()
	srv := httptest.NewServer(api.NewRouter(st, api.Options{APIKey: apiKey, AllowPrivateDestinations: true, Metrics: m}))
	defer srv.Close()

	resp, _ := http.Get(srv.URL + "/readyz")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/readyz: HTTP %d", resp.StatusCode)
	}
	resp, _ = http.Get(srv.URL + "/metrics")
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("/metrics without key: HTTP %d, want 401", resp.StatusCode)
	}

	var n atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1)%2 == 1 { // fail every other request
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer receiver.Close()
	registerEndpoint(t, srv, receiver.URL)
	for i := range 5 {
		apiCall(t, srv, "POST", "/events", fmt.Sprintf(`{"id":"m_%d","type":"order.created","payload":{}}`, i))
	}
	apiCall(t, srv, "POST", "/events", `{"id":"m_0","type":"order.created","payload":{}}`)
	startPool(t, st, func(c *worker.Config) { c.Workers, c.Metrics = 1, m })
	waitFor(t, 5*time.Second, func() bool { return countWhere(t, pool, "status = 'delivered'") == 5 })

	req, _ := http.NewRequest("GET", srv.URL+"/metrics", nil)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{
		`webhook_delivery_attempts_total{result="success"} 5`,
		`webhook_delivery_attempts_total{result="failure"} 5`,
		`webhook_deliveries_finished_total{status="delivered"} 5`,
		`webhook_events_total{result="accepted"} 5`,
		`webhook_events_total{result="duplicate"} 1`,
		"webhook_queue_stats_up 1",
		"webhook_deliveries_pending 0",
		"webhook_delivery_attempt_duration_seconds_count 10",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

// Regression test for the claiming bug fixed in migration 005: when the
// endpoint with the oldest due work is already at its in-flight cap, a claim
// must hand out another endpoint's delivery instead of coming back empty.
func TestClaimSkipsEndpointsAtCap(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	st := store.New(pool, testBox)
	full, err := st.CreateEndpoint(ctx, "https://example.com/full", []string{"a"}, "whsec_a")
	if err != nil {
		t.Fatal(err)
	}
	free, err := st.CreateEndpoint(ctx, "https://example.com/free", []string{"b"}, "whsec_b")
	if err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		st.CreateEvent(ctx, store.Event{ID: fmt.Sprintf("a_%d", i), Type: "a", Payload: json.RawMessage(`{}`)})
	}
	st.CreateEvent(ctx, store.Event{ID: "b_0", Type: "b", Payload: json.RawMessage(`{}`)})
	// The full endpoint's work is the oldest...
	if _, err := pool.Exec(ctx, `UPDATE deliveries SET next_attempt_at = now() - interval '1 hour' WHERE endpoint_id = $1`, full.ID); err != nil {
		t.Fatal(err)
	}
	// ...and 2 of its deliveries are in flight, filling a cap of 2.
	if _, err := pool.Exec(ctx, `UPDATE deliveries SET lease_expires_at = now() + interval '1 minute', next_attempt_at = now() + interval '1 minute'
		WHERE id IN (SELECT id FROM deliveries WHERE endpoint_id = $1 ORDER BY id LIMIT 2)`, full.ID); err != nil {
		t.Fatal(err)
	}

	jobs, err := st.ClaimDue(ctx, 1, 10*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].EndpointID != free.ID {
		t.Fatalf("claim returned %d jobs (want 1 for the endpoint with free capacity)", len(jobs))
	}
}
