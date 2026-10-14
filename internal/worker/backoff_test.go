package worker

import (
	"testing"
	"time"
)

func TestBackoffGrowsWithJitterAndCap(t *testing.T) {
	base, maxDelay := time.Second, 10*time.Second
	for attempt := 1; attempt <= 40; attempt++ {
		want := min(base<<min(attempt-1, 10), maxDelay)
		for range 100 {
			got := Backoff(base, maxDelay, attempt)
			if got < want/2 || got >= want {
				t.Fatalf("attempt %d: got %v, want in [%v, %v)", attempt, got, want/2, want)
			}
		}
	}
}
