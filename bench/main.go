// Command bench fires events at a running server and measures ingest latency,
// delivery throughput, success rate and attempts. Use bench/run.sh to start
// the server and receiver for each configuration.
//
// WARNING: truncates the endpoints/events/deliveries tables.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	apiURL := flag.String("api", "http://127.0.0.1:8080", "server base URL")
	dbURL := flag.String("db", os.Getenv("DATABASE_URL"), "Postgres URL (reads results directly)")
	target := flag.String("receiver", "http://127.0.0.1:9090/hook", "webhook URL to register")
	events := flag.Int("events", 10000, "number of events to send")
	concurrency := flag.Int("concurrency", 32, "concurrent POST /events clients")
	endpoints := flag.Int("endpoints", 1, "number of endpoints; events are spread evenly across them")
	timeout := flag.Duration("timeout", 10*time.Minute, "max time to wait for deliveries to finish")
	label := flag.String("label", "", "label printed with the results")
	allowRemote := flag.Bool("allow-remote-db", false, "allow a non-localhost database (its tables will be wiped)")
	flag.Parse()
	apiKey := os.Getenv("API_KEY")
	if apiKey == "" || *dbURL == "" {
		log.Fatal("API_KEY and DATABASE_URL (or -db) must be set")
	}

	ctx := context.Background()
	poolCfg, err := pgxpool.ParseConfig(*dbURL)
	if err != nil {
		log.Fatal(err)
	}
	// The bench wipes tables, so refuse anything but a local database unless
	// explicitly allowed.
	if h := poolCfg.ConnConfig.Host; !*allowRemote && h != "localhost" && h != "127.0.0.1" && h != "::1" && !strings.HasPrefix(h, "/") {
		log.Fatalf("refusing to wipe tables on non-local database host %q (use -allow-remote-db)", h)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	must(pool.Exec(ctx, "TRUNCATE deliveries, events, endpoints"))

	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *concurrency},
	}
	post := func(path, body string) (int, error) {
		req, _ := http.NewRequest(http.MethodPost, *apiURL+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		resp.Body.Close()
		return resp.StatusCode, nil
	}
	// Endpoint k subscribes to "bench.event.k"; event i has type i % endpoints,
	// so every event produces exactly one delivery.
	for k := range *endpoints {
		body := fmt.Sprintf(`{"url":"%s?ep=%d","event_types":["bench.event.%d"]}`, *target, k, k)
		if code, err := post("/endpoints", body); err != nil || code != http.StatusCreated {
			log.Fatalf("register endpoint: HTTP %d %v", code, err)
		}
	}

	// Phase 1: send events and time each POST.
	latencies := make([]time.Duration, *events)
	var next, non202 atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for range *concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := next.Add(1) - 1
				if i >= int64(*events) {
					return
				}
				body := fmt.Sprintf(`{"id":"bench_%d","type":"bench.event.%d","payload":{"n":%d,"note":"benchmark payload"}}`,
					i, i%int64(*endpoints), i)
				t0 := time.Now()
				code, err := post("/events", body)
				latencies[i] = time.Since(t0)
				if err != nil || code != http.StatusAccepted {
					non202.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	ingest := time.Since(start)

	// Phase 2: wait until no delivery is pending.
	deadline := time.Now().Add(*timeout)
	var pending int
	for {
		must1(pool.QueryRow(ctx, "SELECT count(*) FROM deliveries WHERE status = 'pending'").Scan(&pending))
		if pending == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	drain := time.Since(start)

	var total, delivered, dead, attempts int
	var avgAttempts float64
	var deliverySpan time.Duration
	must1(pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE status = 'delivered'),
		       count(*) FILTER (WHERE status = 'dead'),
		       coalesce(sum(attempts), 0),
		       coalesce(avg(attempts), 0)::float8,
		       coalesce(max(delivered_at) - (SELECT min(created_at) FROM events), '0'::interval)
		FROM deliveries`).Scan(&total, &delivered, &dead, &attempts, &avgAttempts, &deliverySpan))

	slices.Sort(latencies)
	pct := func(p float64) time.Duration { return latencies[int(float64(len(latencies)-1)*p)] }

	fmt.Printf("\n=== %s\n", *label)
	fmt.Printf("events sent:          %d (%d concurrent clients) to %d endpoint(s), non-202 responses: %d\n",
		*events, *concurrency, *endpoints, non202.Load())
	fmt.Printf("ingest:               %.0f events/s over %v\n", float64(*events)/ingest.Seconds(), ingest.Round(time.Millisecond))
	fmt.Printf("POST /events latency: p50 %v  p95 %v  p99 %v  max %v\n",
		pct(0.50).Round(10*time.Microsecond), pct(0.95).Round(10*time.Microsecond),
		pct(0.99).Round(10*time.Microsecond), latencies[len(latencies)-1].Round(10*time.Microsecond))
	fmt.Printf("deliveries:           %d total, %d delivered, %d dead, %d still pending\n", total, delivered, dead, pending)
	fmt.Printf("success rate:         %.2f%%\n", 100*float64(delivered)/float64(max(total, 1)))
	fmt.Printf("attempts:             %d HTTP attempts, %.3f avg per delivery\n", attempts, avgAttempts)
	fmt.Printf("delivery throughput:  %.0f deliveries/s (first event created -> last delivery, %v)\n",
		float64(delivered)/deliverySpan.Seconds(), deliverySpan.Round(time.Millisecond))
	fmt.Printf("wall time to drain:   %v\n", drain.Round(time.Millisecond))
}

func must[T any](_ T, err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func must1(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
