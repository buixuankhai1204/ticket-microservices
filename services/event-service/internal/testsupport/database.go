package testsupport

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/db"
)

func adminURL() string {
	if v := os.Getenv("COMPONENT_PG_URL"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@localhost:5440/postgres?sslmode=disable"
}

func NewDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	admin, err := pgx.Connect(ctx, adminURL())
	if err != nil {
		t.Fatalf("connect to the component-test postgres (docker compose --profile component-test up -d postgres-test): %v", err)
	}
	name := "comp_" + uuid.NewString()[:8]
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	_ = admin.Close(ctx)

	u, err := url.Parse(adminURL())
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	pool, err := db.NewPool(ctx, u.String(), 8)
	if err != nil {
		t.Fatalf("open pool on %s: %v", name, err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate %s: %v", name, err)
	}

	t.Cleanup(func() {
		pool.Close()
		cleanup, err := pgx.Connect(context.Background(), adminURL())
		if err != nil {
			return
		}
		defer cleanup.Close(context.Background())
		_, _ = cleanup.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	return pool
}
