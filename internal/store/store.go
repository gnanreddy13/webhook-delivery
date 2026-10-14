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
	ID         string    `json:"id"`
	URL        string    `json:"url"`
	EventTypes []string  `json:"event_types"`
	Secret     string    `json:"secret,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
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
		 SELECT $1, id FROM endpoints WHERE $2 = ANY (event_types)
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
	URL            string
	Secret         string // decrypted
	SecretErr      error  // set if the stored secret could not be decrypted
}

// ClaimDue locks up to limit due pending deliveries with FOR UPDATE SKIP
// LOCKED and leases them by pushing next_attempt_at forward by lease. If the
// worker dies before recording a result, the rows simply become due again
// when the lease expires.
func (s *Store) ClaimDue(ctx context.Context, limit int, lease time.Duration) ([]Job, error) {
	rows, err := s.pool.Query(ctx,
		`WITH due AS (
			SELECT id FROM deliveries
			WHERE status = 'pending' AND next_attempt_at <= now()
			ORDER BY next_attempt_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE deliveries d
		SET next_attempt_at = now() + $2 * interval '1 millisecond', updated_at = now()
		FROM due, events e, endpoints ep
		WHERE d.id = due.id AND e.id = d.event_id AND ep.id = d.endpoint_id
		RETURNING d.id, d.attempts, d.next_attempt_at, e.id, e.type, e.payload, e.created_at, ep.url, ep.secret`,
		limit, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.DeliveryID, &j.Attempts, &j.LeaseUntil, &j.EventID, &j.EventType,
			&j.Payload, &j.EventCreatedAt, &j.URL, &j.Secret); err != nil {
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
		     last_error = NULL, delivered_at = now(), updated_at = now()
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
		     next_attempt_at = $5, updated_at = now()
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
		`UPDATE deliveries SET next_attempt_at = now(), updated_at = now()
		 WHERE id = ANY ($1) AND status = 'pending'`, ids)
	return err
}
