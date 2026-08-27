//go:build integration

// Package integration holds analytics-service's real-Postgres integration tests:
// the HTTP handler -> usecase -> repository -> Postgres path driven end to end,
// plus the Kafka consumer's use-case surface (RecordUserRegistrationUseCase)
// exercised against a live DB. No mocks, no in-memory fakes — every test here
// needs the container to mean anything.
//
// Run with:
//
//	go test -tags=integration ./services/analytics-service/...
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

	appdb "github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/db"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/logger"
)

// testPool is the shared connection pool against the throwaway container. It is
// created once in TestMain and closed when the package's tests finish.
var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("analytics_test"),
		tcpostgres.WithUsername("analytics"),
		tcpostgres.WithPassword("analytics"),
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

// truncateAll wipes every read-model / ledger table so each test starts from a
// known-empty schema. schema_migrations is left intact.
func truncateAll(t *testing.T) {
	t.Helper()
	_, err := testPool.Exec(context.Background(),
		`TRUNCATE booking_outcomes, user_registrations, processed_events`)
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
