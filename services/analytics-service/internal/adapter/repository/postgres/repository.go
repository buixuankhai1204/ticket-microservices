package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
)

// Repository implements domain.Repository against Postgres. This is the only
// package in the service allowed to import pgx / pgxpool — the DB driver is a
// detail, not something usecase or domain should know exists.
type Repository struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// GetEventBookingStats runs inside a read-only transaction so every query it
// issues sees one consistent snapshot and can never accidentally write
// (CLAUDE.md: every endpoint's DB access runs in one transaction; reads use a
// read-only tx).
func (r *Repository) GetEventBookingStats(ctx context.Context, eventID uuid.UUID) (domain.EventBookingStats, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.EventBookingStats{}, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx) // no-op after Commit; rolls back on any early return

	const query = `
		SELECT
			COUNT(*) FILTER (WHERE status = 'confirmed') AS confirmed,
			COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled
		FROM booking_outcomes
		WHERE event_id = $1`

	stats := domain.EventBookingStats{EventID: eventID}
	if err := tx.QueryRow(ctx, query, eventID).Scan(&stats.Confirmed, &stats.Cancelled); err != nil {
		return domain.EventBookingStats{}, &domain.RepositoryError{Err: fmt.Errorf("query event stats: %w", err)}
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.EventBookingStats{}, &domain.RepositoryError{Err: err}
	}
	return stats, nil
}
