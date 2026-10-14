// Package store holds all SQL used by the API and the delivery workers.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gnanreddy13/webhook-delivery/internal/secretbox"
)

var ErrNotFound = errors.New("not found")

type Store struct {
	pool *pgxpool.Pool
	box  *secretbox.Box // encrypts endpoint secrets at rest
}

func New(pool *pgxpool.Pool, box *secretbox.Box) *Store {
	return &Store{pool: pool, box: box}
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

type Endpoint struct {
	ID                  string     `json:"id"`
	URL                 string     `json:"url"`
	EventTypes          []string   `json:"event_types"`
	Secret              string     `json:"secret,omitempty"` // only set when created or rotated
	CreatedAt           time.Time  `json:"created_at"`
	DisabledAt          *time.Time `json:"disabled_at"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	PausedUntil         *time.Time `json:"paused_until"` // circuit breaker open until then
}

type Event struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type Delivery struct {
	ID             int64      `json:"id"`
	EventID        string     `json:"event_id"`
	EndpointID     string     `json:"endpoint_id"`
	Status         string     `json:"status"`
	Attempts       int        `json:"attempts"`
	NextAttemptAt  *time.Time `json:"next_attempt_at"` // null unless pending
	LastStatusCode *int       `json:"last_status_code"`
	LastError      *string    `json:"last_error"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	DeliveredAt    *time.Time `json:"delivered_at"`
}

// CreateEndpoint stores the endpoint with its secret encrypted. The returned
// Endpoint carries the plaintext secret so it can be shown once.
func (s *Store) CreateEndpoint(ctx context.Context, url string, eventTypes []string, secret string) (Endpoint, error) {
	e := Endpoint{URL: url, EventTypes: eventTypes, Secret: secret}
	sealed, err := s.box.Seal(secret)
	if err != nil {
		return e, err
	}
	err = s.pool.QueryRow(ctx,
		`INSERT INTO endpoints (url, event_types, secret) VALUES ($1, $2, $3)
		 RETURNING id, created_at`,
		url, eventTypes, sealed,
	).Scan(&e.ID, &e.CreatedAt)
	return e, err
}

// ReencryptSecrets encrypts any plaintext secrets (rows written before
// encryption existed) and re-seals secrets that only the previous key can
// open, so SECRET_ENCRYPTION_KEY_PREVIOUS can be dropped afterwards.
func (s *Store) ReencryptSecrets(ctx context.Context) (updated int, err error) {
	rows, err := s.pool.Query(ctx, `SELECT id, secret FROM endpoints`)
	if err != nil {
		return 0, err
	}
	type row struct{ id, secret string }
	var all []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.secret); err != nil {
			return 0, err
		}
		all = append(all, r)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, r := range all {
		plain := r.secret
		if secretbox.IsSealed(r.secret) {
			p, usedPrevious, err := s.box.Open(r.secret)
			if err != nil {
				return updated, fmt.Errorf("endpoint %s: %w", r.id, err)
			}
			if !usedPrevious {
				continue
			}
			plain = p
		}
		sealed, err := s.box.Seal(plain)
		if err != nil {
			return updated, err
		}
		if _, err := s.pool.Exec(ctx, `UPDATE endpoints SET secret = $2 WHERE id = $1 AND secret = $3`,
			r.id, sealed, r.secret); err != nil {
			return updated, err
		}
		updated++
	}
	return updated, nil
}

// CreateEvent stores the event and fans it out to one pending delivery per
// subscribed endpoint, all in a single transaction (transactional outbox).
// If an event with the same ID already exists, nothing is written and
// created is false.
func (s *Store) CreateEvent(ctx context.Context, ev Event) (created bool, deliveries int64, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, 0, err
	}
	defer tx.Rollback(ctx) // no-op after Commit

	tag, err := tx.Exec(ctx,
		`INSERT INTO events (id, type, payload) VALUES ($1, $2, $3)
		 ON CONFLICT (id) DO NOTHING`,
		ev.ID, ev.Type, string(ev.Payload))
	if err != nil {
		return false, 0, err
	}
	if tag.RowsAffected() == 0 {
		return false, 0, tx.Commit(ctx)
	}

	tag, err = tx.Exec(ctx,
		`INSERT INTO deliveries (event_id, endpoint_id)
		 SELECT $1, id FROM endpoints WHERE $2 = ANY (event_types) AND disabled_at IS NULL
		 ON CONFLICT (event_id, endpoint_id) DO NOTHING`,
		ev.ID, ev.Type)
	if err != nil {
		return false, 0, err
	}
	return true, tag.RowsAffected(), tx.Commit(ctx)
}

const deliveryColumns = `id, event_id, endpoint_id, status, attempts, next_attempt_at,
	last_status_code, last_error, created_at, updated_at, delivered_at`

func scanDelivery(row pgx.Row) (Delivery, error) {
	var d Delivery
	var next time.Time
	err := row.Scan(&d.ID, &d.EventID, &d.EndpointID, &d.Status, &d.Attempts, &next,
		&d.LastStatusCode, &d.LastError, &d.CreatedAt, &d.UpdatedAt, &d.DeliveredAt)
	// For delivered and dead rows the column only holds a stale lease or
	// retry time, so don't expose it.
	if d.Status == "pending" {
		d.NextAttemptAt = &next
	}
	return d, err
}

func (s *Store) GetDelivery(ctx context.Context, id int64) (Delivery, error) {
	d, err := scanDelivery(s.pool.QueryRow(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return d, ErrNotFound
	}
	return d, err
}

// ListDeliveries returns the newest deliveries first. An empty status means
// any status.
func (s *Store) ListDeliveries(ctx context.Context, status string, limit int) ([]Delivery, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries
		 WHERE $1 = '' OR status = $1
		 ORDER BY id DESC LIMIT $2`, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []Delivery{}
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// Job is a claimed delivery plus everything needed to send it.
type Job struct {
	DeliveryID     int64
	Attempts       int
	LeaseUntil     time.Time // next_attempt_at written by the claim; acts as a lease token
	EventID        string
	EventType      string
	Payload        json.RawMessage
	EventCreatedAt time.Time
	EndpointID     string
	URL            string
	Secret         string // decrypted
	SecretErr      error  // set if the stored secret could not be decrypted
}

// ClaimDue leases up to limit due deliveries, at most one per endpoint per
// call, skipping endpoints that are disabled, paused by the circuit breaker,
// or already at maxInflight leased deliveries. See claim_deliveries in
// migrations/003_claim_function.sql for how the cap is kept exact under
// concurrency.
//
// The lease pushes next_attempt_at forward (hiding the row from other
// workers, and serving as the fencing token for recording the result) and
// sets lease_expires_at (counting it as in flight). If the worker dies, both
// simply expire and the row becomes due again.
func (s *Store) ClaimDue(ctx context.Context, limit int, lease time.Duration, maxInflight int) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `SELECT * FROM claim_deliveries($1, $2, $3)`,
		limit, lease.Milliseconds(), maxInflight)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.DeliveryID, &j.Attempts, &j.LeaseUntil, &j.EventID, &j.EventType,
			&j.Payload, &j.EventCreatedAt, &j.EndpointID, &j.URL, &j.Secret); err != nil {
			return nil, err
		}
		j.Secret, _, j.SecretErr = s.box.Open(j.Secret)
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// ErrLeaseLost means the lease expired and another worker may own the row.
var ErrLeaseLost = errors.New("lease lost")

// MarkDelivered records a successful attempt.
func (s *Store) MarkDelivered(ctx context.Context, j Job, statusCode int) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE deliveries
		 SET status = 'delivered', attempts = attempts + 1, last_status_code = $2,
		     last_error = NULL, delivered_at = now(), updated_at = now(), lease_expires_at = NULL
		 WHERE id = $1 AND status = 'pending' AND next_attempt_at = $3`,
		j.DeliveryID, statusCode, j.LeaseUntil)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return err
}

// MarkFailed records a failed attempt. If dead is true the delivery is
// given up on; otherwise it is retried at nextAttempt.
func (s *Store) MarkFailed(ctx context.Context, j Job, statusCode *int, errMsg string, dead bool, nextAttempt time.Time) error {
	status := "pending"
	if dead {
		status = "dead"
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE deliveries
		 SET status = $2, attempts = attempts + 1, last_status_code = $3, last_error = $4,
		     next_attempt_at = $5, updated_at = now(), lease_expires_at = NULL
		 WHERE id = $1 AND status = 'pending' AND next_attempt_at = $6`,
		j.DeliveryID, status, statusCode, errMsg, nextAttempt, j.LeaseUntil)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return err
}

// Release makes claimed-but-unsent jobs due immediately (used on shutdown).
func (s *Store) Release(ctx context.Context, jobs []Job) error {
	ids := make([]int64, len(jobs))
	for i, j := range jobs {
		ids[i] = j.DeliveryID
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE deliveries SET next_attempt_at = now(), updated_at = now(), lease_expires_at = NULL
		 WHERE id = ANY ($1) AND status = 'pending'`, ids)
	return err
}

// BreakerConfig controls the per-endpoint circuit breaker. Threshold 0
// disables it.
type BreakerConfig struct {
	Threshold   int           // consecutive failures that open the breaker
	Cooldown    time.Duration // first pause; doubles on each further failure
	MaxCooldown time.Duration
}

// RecordEndpointResult updates the endpoint's circuit breaker after an
// attempt. Success closes it. Each failure at or past the threshold pauses
// the endpoint for an exponentially growing cooldown; while paused its
// deliveries are not claimed (and don't use up attempts). It returns the
// pause end when this failure opened or extended the pause.
func (s *Store) RecordEndpointResult(ctx context.Context, endpointID string, ok bool, b BreakerConfig) (*time.Time, error) {
	if ok {
		// Only write when there is something to reset, to keep the hot
		// success path to a single delivery update.
		_, err := s.pool.Exec(ctx,
			`UPDATE endpoints SET consecutive_failures = 0, paused_until = NULL
			 WHERE id = $1 AND (consecutive_failures <> 0 OR paused_until IS NOT NULL)`, endpointID)
		return nil, err
	}
	var pausedUntil *time.Time
	err := s.pool.QueryRow(ctx,
		`UPDATE endpoints SET
		   consecutive_failures = consecutive_failures + 1,
		   paused_until = CASE
		     WHEN $2 > 0 AND consecutive_failures + 1 >= $2 THEN
		       now() + least($3::float8 * power(2, least(consecutive_failures + 1 - $2, 30)), $4::float8)
		               * interval '1 millisecond'
		     ELSE paused_until END
		 WHERE id = $1
		 RETURNING CASE WHEN $2 > 0 AND consecutive_failures >= $2 THEN paused_until END`,
		endpointID, b.Threshold, b.Cooldown.Milliseconds(), b.MaxCooldown.Milliseconds()).Scan(&pausedUntil)
	return pausedUntil, err
}

const endpointColumns = `id, url, event_types, created_at, disabled_at, consecutive_failures, paused_until`

func scanEndpoint(row pgx.Row) (Endpoint, error) {
	var e Endpoint
	err := row.Scan(&e.ID, &e.URL, &e.EventTypes, &e.CreatedAt, &e.DisabledAt, &e.ConsecutiveFailures, &e.PausedUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return e, ErrNotFound
	}
	return e, err
}

func (s *Store) GetEndpoint(ctx context.Context, id string) (Endpoint, error) {
	return scanEndpoint(s.pool.QueryRow(ctx, `SELECT `+endpointColumns+` FROM endpoints WHERE id = $1`, id))
}

func (s *Store) ListEndpoints(ctx context.Context) ([]Endpoint, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+endpointColumns+` FROM endpoints ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Endpoint{}
	for rows.Next() {
		e, err := scanEndpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// SetEndpointDisabled disables or re-enables an endpoint. Re-enabling also
// resets its circuit breaker. A disabled endpoint gets no new deliveries
// and its pending ones wait (not attempted) until it is enabled again.
func (s *Store) SetEndpointDisabled(ctx context.Context, id string, disabled bool) (Endpoint, error) {
	q := `UPDATE endpoints SET disabled_at = coalesce(disabled_at, now()) WHERE id = $1 RETURNING ` + endpointColumns
	if !disabled {
		q = `UPDATE endpoints SET disabled_at = NULL, consecutive_failures = 0, paused_until = NULL
		     WHERE id = $1 RETURNING ` + endpointColumns
	}
	return scanEndpoint(s.pool.QueryRow(ctx, q, id))
}

// RotateEndpointSecret replaces the signing secret. The old secret stops
// being used immediately.
func (s *Store) RotateEndpointSecret(ctx context.Context, id, secret string) (Endpoint, error) {
	sealed, err := s.box.Seal(secret)
	if err != nil {
		return Endpoint{}, err
	}
	e, err := scanEndpoint(s.pool.QueryRow(ctx,
		`UPDATE endpoints SET secret = $2 WHERE id = $1 RETURNING `+endpointColumns, id, sealed))
	e.Secret = secret
	return e, err
}

// ErrNotDead is returned when retrying a delivery that isn't dead.
var ErrNotDead = errors.New("delivery is not dead")

// RetryDelivery puts a dead delivery back in the queue with a fresh set of
// attempts.
func (s *Store) RetryDelivery(ctx context.Context, id int64) (Delivery, error) {
	d, err := scanDelivery(s.pool.QueryRow(ctx,
		`UPDATE deliveries SET status = 'pending', attempts = 0, next_attempt_at = now(), updated_at = now()
		 WHERE id = $1 AND status = 'dead'
		 RETURNING `+deliveryColumns, id))
	if errors.Is(err, pgx.ErrNoRows) {
		if _, getErr := s.GetDelivery(ctx, id); getErr != nil {
			return d, getErr
		}
		return d, ErrNotDead
	}
	return d, err
}
