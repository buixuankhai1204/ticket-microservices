//go:build integration

// Package integration holds event-service's real-Postgres integration tests: the
// HTTP handler -> usecase -> repository -> Postgres path driven end to end, no
// mocks and no in-memory fakes. Every test here needs the container to mean
// anything — it covers the seams a unit test structurally cannot reach: the
// layout-expansion write actually landing in `seats`, the create transaction
// rolling back as a unit on a constraint violation, concurrent creates not
// losing or cross-contaminating rows, and the list/get queries' real ORDER BY /
// LIMIT / OFFSET behaviour.
//
// Run with:
//
//	go test -tags=integration ./services/event-service/integration/...
//
// Requires a running Docker daemon (testcontainers-go spins up one
// postgres:16-alpine container for the whole package in TestMain).
package integration

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	appdb "github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/db"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/logger"
)

// testPool is the shared connection pool against the throwaway container. It is
// created once in TestMain and closed when the package's tests finish.
var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("event_test"),
		tcpostgres.WithUsername("event"),
		tcpostgres.WithPassword("event"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration tests need Docker running: could not start postgres container: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection string: %v\n", err)
		os.Exit(1)
	}

	testPool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open pool: %v\n", err)
		os.Exit(1)
	}
	defer testPool.Close()

	// Run the service's real migration runner: every embedded .sql under
	// migrations/ applied in filename order, exactly as cmd/main.go does at boot.
	if err := appdb.Migrate(ctx, testPool); err != nil {
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

// truncateAll wipes the catalogue so each test starts from a known-empty schema.
// seats is listed before events for readability, but TRUNCATE handles both in one
// statement so the seats -> events FK (seats.event_id REFERENCES events.id) is
// never a problem. schema_migrations is left intact.
func truncateAll(t *testing.T) {
	t.Helper()
	_, err := testPool.Exec(context.Background(), `TRUNCATE seats, events`)
	if err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
}

// noopLogger satisfies logger.Logger without writing anything, keeping test
// output readable. The middleware chain still runs against it for real.
type noopLogger struct{}

func (noopLogger) Info(string, ...any)         {}
func (noopLogger) Error(string, ...any)        {}
func (n noopLogger) With(...any) logger.Logger { return n }
