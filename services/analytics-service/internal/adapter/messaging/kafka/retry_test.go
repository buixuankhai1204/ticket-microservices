package kafka_test

import (
	"testing"
	"time"

	kafka "github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/messaging/kafka"
)

func TestBackoffDoublesFromTheFirstWaitAndStopsAtTheCap(t *testing.T) {
	policy := kafka.RetryPolicy{MaxAttempts: 10, FirstBackoff: 100 * time.Millisecond, MaxBackoff: 500 * time.Millisecond}

	var waits []time.Duration
	for attempt := 1; attempt <= 5; attempt++ {
		waits = append(waits, policy.Backoff(attempt))
	}

	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 500 * time.Millisecond, 500 * time.Millisecond}
	for i := range want {
		if waits[i] != want[i] {
			t.Fatalf("waits = %v, want %v", waits, want)
		}
	}
}

func TestBackoffNeverOverflowsForAbsurdAttemptCounts(t *testing.T) {
	policy := kafka.DefaultRetryPolicy(5)

	for _, attempt := range []int{0, -3, 64, 1 << 30} {
		if got := policy.Backoff(attempt); got <= 0 || got > policy.MaxBackoff {
			t.Fatalf("Backoff(%d) = %v, want within (0, %v]", attempt, got, policy.MaxBackoff)
		}
	}
}

func TestDefaultRetryPolicyFallsBackWhenAttemptsAreMisconfigured(t *testing.T) {
	if got := kafka.DefaultRetryPolicy(0).MaxAttempts; got != 5 {
		t.Fatalf("MaxAttempts = %d, want 5", got)
	}
	if got := kafka.DefaultRetryPolicy(7).MaxAttempts; got != 7 {
		t.Fatalf("MaxAttempts = %d, want 7", got)
	}
}
