//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/buixuankhai1204/ticket-microservice-golang/e2e/internal/harness"
)

// harnessPools is opened once in TestMain and shared read-only by every test
// in this package -- pgxpool is safe for concurrent use, and none of these
// tests mutate participant schemas, only saga-produced rows.
var (
	harnessPools          *harness.Pools
	stackReachable        bool
	stackSkipReason       string
	kafkaReachableAtStart bool
)

// TestMain preflights the stack (every participant service's /healthz, the
// Kafka Connect REST API, and the gateway) before any test runs. Per this
// package's ground rules it never builds or starts anything itself: a dev who
// runs `go test -tags=e2e ./e2e/...` against a cold stack sees every test
// report SKIP with a one-line "bring the stack up" reason, never a wall of
// red.
func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := harness.Preflight(ctx); err != nil {
		stackSkipReason = fmt.Sprintf("stack not reachable (%v) -- bring it up with `docker compose up -d` and retry", err)
		os.Exit(m.Run())
	}

	pools, err := harness.OpenPools(ctx)
	if err != nil {
		stackSkipReason = fmt.Sprintf("could not open participant DB pools (%v) -- bring the stack up with `docker compose up -d` and retry", err)
		os.Exit(m.Run())
	}
	if err := pools.Ping(ctx); err != nil {
		stackSkipReason = fmt.Sprintf("a participant DB is unreachable (%v) -- bring the stack up with `docker compose up -d` and retry", err)
		pools.Close()
		os.Exit(m.Run())
	}

	harnessPools = pools
	stackReachable = true
	kafkaReachableAtStart = harness.KafkaReachable(ctx)

	code := m.Run()
	pools.Close()
	os.Exit(code)
}

// requireStack skips the calling test cleanly when the stack preflight
// failed. Call it first in every test.
func requireStack(t *testing.T) {
	t.Helper()
	if !stackReachable {
		t.Skip(stackSkipReason)
	}
}

// requireKafka additionally skips a test that needs a direct, host-reachable
// Kafka broker (the idempotency and poison-message tests). This repo's
// docker-compose.yml advertises Kafka in-network only (see
// harness.KafkaReachable), so on an unmodified stack these tests report SKIP
// with this reason rather than a false failure.
func requireKafka(t *testing.T) {
	t.Helper()
	requireStack(t)
	if !kafkaReachableAtStart {
		t.Skip("kafka broker not reachable from the host at " + strings.Join(harness.KafkaBrokers(), ",") +
			" -- docker-compose.yml's kafka service only publishes an in-network listener (kafka:9092)," +
			" by design (see its comment: \"use `docker compose exec kafka ...` to poke it from the host\")." +
			" Add a host-mapped listener to run this test from outside the compose network.")
	}
}
