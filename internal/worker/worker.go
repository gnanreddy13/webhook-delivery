// Package worker claims due deliveries from Postgres and sends them.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gnanreddy13/webhook-delivery/internal/netguard"
	"github.com/gnanreddy13/webhook-delivery/internal/signing"
	"github.com/gnanreddy13/webhook-delivery/internal/store"
)

type Config struct {
	Workers      int
	BatchSize    int
	MaxAttempts  int
	BackoffBase  time.Duration
	BackoffMax   time.Duration
	Timeout      time.Duration // per HTTP attempt
	Lease        time.Duration
	PollInterval time.Duration
	// AllowPrivateDestinations disables the SSRF dial check (local dev only).
	AllowPrivateDestinations bool
}

type Pool struct {
	cfg    Config
	store  *store.Store
	client *http.Client
}

func New(cfg Config, s *store.Store) *Pool {
	dialer := &net.Dialer{Timeout: cfg.Timeout, KeepAlive: 30 * time.Second}
	if !cfg.AllowPrivateDestinations {
		// Checked on the resolved IP of every connection, so DNS tricks
		// can't reach internal services.
		dialer.Control = netguard.Control
	}
	return &Pool{
		cfg:   cfg,
		store: s,
		client: &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				// No proxy: an HTTP(S)_PROXY would make the dial check see
				// the proxy's address instead of the webhook destination.
				Proxy:               nil,
				DialContext:         dialer.DialContext,
				TLSHandshakeTimeout: cfg.Timeout,
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: max(cfg.Workers, 2),
				IdleConnTimeout:     90 * time.Second,
			},
			// A redirect is treated as a non-2xx response, not followed.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

// Run starts the workers and blocks until ctx is cancelled and every worker
// has finished the request it was sending.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := range p.cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.loop(ctx, i)
		}()
	}
	wg.Wait()
}

func (p *Pool) loop(ctx context.Context, id int) {
	log := slog.With("worker", id)
	for ctx.Err() == nil {
		leaseEnd := time.Now().Add(p.cfg.Lease)
		jobs, err := p.store.ClaimDue(ctx, p.cfg.BatchSize, p.cfg.Lease)
		if err != nil {
			if ctx.Err() == nil {
				log.Error("claim failed", "err", err)
			}
			sleep(ctx, p.cfg.PollInterval)
			continue
		}
		if len(jobs) == 0 {
			sleep(ctx, p.cfg.PollInterval)
			continue
		}
		for i, j := range jobs {
			// Hand the unsent rest back if we are shutting down, or if the
			// lease could expire mid-attempt (slow receivers with batches
			// > 1), which would let another worker send the same row.
			if ctx.Err() != nil || time.Until(leaseEnd) < p.cfg.Timeout+time.Second {
				if err := p.store.Release(context.Background(), jobs[i:]); err != nil {
					log.Error("release failed", "err", err)
				}
				break
			}
			p.process(log, j)
		}
	}
}

// process sends one delivery and records the outcome. It deliberately does
// not use the shutdown context, so an in-flight attempt is finished and
// recorded during graceful shutdown.
func (p *Pool) process(log *slog.Logger, j store.Job) {
	code, sendErr := p.send(j)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var err error
	if sendErr == nil {
		err = p.store.MarkDelivered(ctx, j, code)
	} else {
		attempts := j.Attempts + 1
		dead := attempts >= p.cfg.MaxAttempts
		next := time.Now().Add(Backoff(p.cfg.BackoffBase, p.cfg.BackoffMax, attempts))
		var codePtr *int
		if code != 0 {
			codePtr = &code
		}
		err = p.store.MarkFailed(ctx, j, codePtr, truncate(sendErr.Error(), 500), dead, next)
		if dead && err == nil {
			log.Warn("delivery dead", "delivery", j.DeliveryID, "attempts", attempts, "last_error", sendErr)
		}
	}
	if errors.Is(err, store.ErrLeaseLost) {
		log.Warn("lease expired before result was recorded", "delivery", j.DeliveryID)
	} else if err != nil {
		log.Error("recording result failed", "delivery", j.DeliveryID, "err", err)
	}
}

// send makes one signed POST. It returns the HTTP status code (0 if no
// response) and a non-nil error unless the response was 2xx.
func (p *Pool) send(j store.Job) (int, error) {
	if j.SecretErr != nil {
		// Never send unsigned; record it as a failed attempt instead.
		return 0, fmt.Errorf("signing secret unavailable: %w", j.SecretErr)
	}
	body, err := json.Marshal(map[string]any{
		"id":         j.EventID,
		"type":       j.EventType,
		"created_at": j.EventCreatedAt.UTC(),
		"payload":    j.Payload,
	})
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequest(http.MethodPost, j.URL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	ts := time.Now().Unix()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "webhook-delivery/0.1")
	req.Header.Set("Webhook-Id", j.EventID)
	req.Header.Set("Webhook-Delivery-Id", strconv.FormatInt(j.DeliveryID, 10))
	req.Header.Set(signing.TimestampHeader, strconv.FormatInt(ts, 10))
	req.Header.Set(signing.SignatureHeader, signing.Sign(j.Secret, ts, body))

	resp, err := p.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	// Drain a little so the connection can be reused.
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return resp.StatusCode, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return resp.StatusCode, nil
}

// Backoff returns the delay before retry number attempt (1-based):
// base * 2^(attempt-1), capped at max, with "equal jitter" so the result is
// uniformly spread over [d/2, d). Jitter keeps retries of many failed
// deliveries from arriving at a recovering receiver all at once.
func Backoff(base, maxDelay time.Duration, attempt int) time.Duration {
	d := base
	for i := 1; i < attempt && d < maxDelay; i++ {
		d *= 2
	}
	d = min(d, maxDelay)
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(half)
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
