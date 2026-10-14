// Package store holds all SQL used by the API and the delivery workers.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
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
	NextAttemptAt  time.Time  `json:"next_attempt_at"`
	LastStatusCode *int       `json:"last_status_code"`
	LastError      *string    `json:"last_error"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	DeliveredAt    *time.Time `json:"delivered_at"`
}

func (s *Store) CreateEndpoint(ctx context.Context, url string, eventTypes []string, secret string) (Endpoint, error) {
	e := Endpoint{URL: url, EventTypes: eventTypes, Secret: secret}
	err := s.pool.QueryRow(ctx,
		`INSERT INTO endpoints (url, event_types, secret) VALUES ($1, $2, $3)
		 RETURNING id, created_at`,
		url, eventTypes, secret,
	).Scan(&e.ID, &e.CreatedAt)
	return e, err
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
	err := row.Scan(&d.ID, &d.EventID, &d.EndpointID, &d.Status, &d.Attempts, &d.NextAttemptAt,
		&d.LastStatusCode, &d.LastError, &d.CreatedAt, &d.UpdatedAt, &d.DeliveredAt)
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
