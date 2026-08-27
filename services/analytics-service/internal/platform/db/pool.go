// Package db owns Postgres connection-pool construction. It is infrastructure
// with no business meaning and is used only from cmd/main.go; the postgres
// adapter receives the *pgxpool.Pool it builds.
package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool builds the single shared connection pool with an explicit, bounded
// MaxConns — never the driver's unbounded default. It pings once so a bad
// DATABASE_URL fails fast at startup.
func NewPool(ctx context.Context, databaseURL string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return pool, nil
}
