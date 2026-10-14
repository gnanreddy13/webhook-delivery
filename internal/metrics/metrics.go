// Package metrics keeps a few counters and renders them, plus queue gauges,
// in the Prometheus text exposition format.
package metrics

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/gnanreddy13/webhook-delivery/internal/store"
)

type Metrics struct {
	AttemptsSucceeded   atomic.Int64
	AttemptsFailed      atomic.Int64
	attemptNanos        atomic.Int64
	Delivered           atomic.Int64
	Dead                atomic.Int64
	EndpointPauses      atomic.Int64
	EventsAccepted      atomic.Int64
	EventsDuplicate     atomic.Int64
	RetentionDeliveries atomic.Int64
	RetentionEvents     atomic.Int64
}

func New() *Metrics { return &Metrics{} }

// ObserveAttempt records one HTTP attempt and how long it took.
func (m *Metrics) ObserveAttempt(ok bool, d time.Duration) {
	if ok {
		m.AttemptsSucceeded.Add(1)
	} else {
		m.AttemptsFailed.Add(1)
	}
	m.attemptNanos.Add(int64(d))
}

// Write renders counters and the given queue gauges. If statsErr is not
// nil the gauges are omitted and webhook_queue_stats_up is 0.
func (m *Metrics) Write(w io.Writer, q store.QueueStats, statsErr error) {
	counter := func(name, help string, series ...string) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
		for _, s := range series {
			fmt.Fprintln(w, s)
		}
	}
	gauge := func(name, help string, v any) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %v\n", name, help, name, name, v)
	}

	attempts := m.AttemptsSucceeded.Load() + m.AttemptsFailed.Load()
	counter("webhook_delivery_attempts_total", "HTTP delivery attempts by result.",
		fmt.Sprintf(`webhook_delivery_attempts_total{result="success"} %d`, m.AttemptsSucceeded.Load()),
		fmt.Sprintf(`webhook_delivery_attempts_total{result="failure"} %d`, m.AttemptsFailed.Load()))
	fmt.Fprintf(w, "# HELP webhook_delivery_attempt_duration_seconds Time spent on delivery attempts.\n"+
		"# TYPE webhook_delivery_attempt_duration_seconds summary\n"+
		"webhook_delivery_attempt_duration_seconds_sum %g\nwebhook_delivery_attempt_duration_seconds_count %d\n",
		time.Duration(m.attemptNanos.Load()).Seconds(), attempts)
	counter("webhook_deliveries_finished_total", "Deliveries that reached a final state.",
		fmt.Sprintf(`webhook_deliveries_finished_total{status="delivered"} %d`, m.Delivered.Load()),
		fmt.Sprintf(`webhook_deliveries_finished_total{status="dead"} %d`, m.Dead.Load()))
	counter("webhook_endpoint_pauses_total", "Times the circuit breaker paused an endpoint.",
		fmt.Sprintf("webhook_endpoint_pauses_total %d", m.EndpointPauses.Load()))
	counter("webhook_events_total", "POST /events requests by outcome.",
		fmt.Sprintf(`webhook_events_total{result="accepted"} %d`, m.EventsAccepted.Load()),
		fmt.Sprintf(`webhook_events_total{result="duplicate"} %d`, m.EventsDuplicate.Load()))
	counter("webhook_retention_deleted_total", "Rows removed by the retention job.",
		fmt.Sprintf(`webhook_retention_deleted_total{table="deliveries"} %d`, m.RetentionDeliveries.Load()),
		fmt.Sprintf(`webhook_retention_deleted_total{table="events"} %d`, m.RetentionEvents.Load()))

	if statsErr != nil {
		gauge("webhook_queue_stats_up", "1 if the queue gauges below could be read from the database.", 0)
		return
	}
	gauge("webhook_queue_stats_up", "1 if the queue gauges below could be read from the database.", 1)
	gauge("webhook_deliveries_pending", "Deliveries not yet delivered or dead.", q.Pending)
	gauge("webhook_deliveries_due", "Pending deliveries due now and waiting for a worker.", q.Due)
	gauge("webhook_oldest_due_delivery_seconds", "Age of the oldest due delivery (delivery lag).", q.OldestDueSeconds)
	gauge("webhook_endpoints_paused", "Endpoints currently paused by the circuit breaker.", q.EndpointsPaused)
	gauge("webhook_endpoints_disabled", "Endpoints disabled through the API.", q.EndpointsDisabled)
}
