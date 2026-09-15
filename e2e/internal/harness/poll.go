//go:build e2e

package harness

import (
	"testing"
	"time"
)

// PollUntil calls fn repeatedly (sleeping interval between attempts) until it
// returns true, failing the test via t.Fatalf if timeout elapses first. Every
// saga hop in this repo is sub-second in normal operation (CLAUDE.md), so a
// generous bounded timeout here is a real assertion -- a saga that never
// reaches a terminal state is a genuine defect, not a flaky test.
func PollUntil(t *testing.T, timeout, interval time.Duration, description string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if fn() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for: %s", timeout, description)
		}
		time.Sleep(interval)
	}
}
