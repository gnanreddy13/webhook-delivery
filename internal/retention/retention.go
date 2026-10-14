// Package retention periodically deletes finished deliveries and events
// older than the retention period, so tables don't grow forever.
package retention

import (
	"context"
	"log/slog"
	"time"

	"github.com/gnanreddy13/webhook-delivery/internal/metrics"
	"github.com/gnanreddy13/webhook-delivery/internal/store"
)

const batchSize = 5000

// Run deletes old rows every interval until ctx is cancelled. Pending
// deliveries are never deleted, and an event is only deleted once none of
// its deliveries remain.
func Run(ctx context.Context, st *store.Store, period, interval time.Duration, m *metrics.Metrics) {
	for {
		RunOnce(ctx, st, period, m)
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// RunOnce deletes in batches until nothing older than period is left.
func RunOnce(ctx context.Context, st *store.Store, period time.Duration, m *metrics.Metrics) (deliveries, events int64) {
	cutoff := time.Now().Add(-period)
	deliveries = drain(ctx, "deliveries", func() (int64, error) { return st.DeleteFinishedDeliveries(ctx, cutoff, batchSize) })
	events = drain(ctx, "events", func() (int64, error) { return st.DeleteOldEvents(ctx, cutoff, batchSize) })
	m.RetentionDeliveries.Add(deliveries)
	m.RetentionEvents.Add(events)
	if deliveries+events > 0 {
		slog.Info("retention cleanup", "deliveries_deleted", deliveries, "events_deleted", events, "older_than", cutoff.Format(time.RFC3339))
	}
	return deliveries, events
}

func drain(ctx context.Context, table string, del func() (int64, error)) int64 {
	var total int64
	for ctx.Err() == nil {
		n, err := del()
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("retention cleanup failed", "table", table, "err", err)
			}
			break
		}
		total += n
		if n < batchSize {
			break
		}
	}
	return total
}
