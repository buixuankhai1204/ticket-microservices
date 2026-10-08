package testsupport

import (
	"context"
	"net/url"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/db"
)

var (
	pgOnce sync.Once
	pgURL  string
	pgErr  error
)

func adminURL(t testing.TB) string {
	t.Helper()
	pgOnce.Do(func() {
		if v := os.Getenv("TEST_DATABASE_URL"); v != "" {
			pgURL = v
			return
		}
		ctx := context.Background()
		container, err := postgres.Run(ctx, "postgres:16-alpine",
			postgres.WithDatabase("postgres"),
			postgres.WithUsername("postgres"),
			postgres.WithPassword("postgres"),
			postgres.BasicWaitStrategies(),
		)
		if err != nil {
			pgErr = err
			return
		}
		onShutdown(func(ctx context.Context) { _ = container.Terminate(ctx) })
		pgURL, pgErr = container.ConnectionString(ctx, "sslmode=disable")
	})
	if pgErr != nil {
		t.Fatalf("start Postgres (is Docker running? or set TEST_DATABASE_URL): %v", pgErr)
	}
	return pgURL
}

func EmptyDatabaseURL(t testing.TB) string {
	t.Helper()
	ctx := context.Background()
	admin := adminURL(t)

	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatalf("connect to Postgres: %v", err)
	}
	name := "t_" + uuid.NewString()[:8]
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	_ = conn.Close(ctx)

	u, err := url.Parse(admin)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name

	t.Cleanup(func() {
		cleanup, err := pgx.Connect(context.Background(), admin)
		if err != nil {
			return
		}
		defer cleanup.Close(context.Background())
		_, _ = cleanup.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	return u.String()
}

func NewDatabase(t testing.TB) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	pool, err := db.NewPool(ctx, EmptyDatabaseURL(t), 8)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}
