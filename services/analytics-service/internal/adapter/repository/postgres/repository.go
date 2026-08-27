package postgres

import (
	"context"
	"errors"
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

// RecordUserRegistration writes the processed_events marker and the
// user_registrations row in one read-write transaction, so a redelivered event
// can never be half-applied. The processed_events insert is the idempotency
// gate: if the event_id is already there (0 rows affected), we commit and report
// alreadyProcessed without touching the read model.
func (r *Repository) RecordUserRegistration(ctx context.Context, eventID uuid.UUID, reg domain.UserRegistration) (bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx) // no-op after Commit; rolls back on any early return

	tag, err := tx.Exec(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		eventID,
	)
	if err != nil {
		return false, &domain.RepositoryError{Err: fmt.Errorf("mark processed_events: %w", err)}
	}
	if tag.RowsAffected() == 0 {
		// Already applied on an earlier delivery — commit the empty tx so the
		// caller can safely commit the Kafka offset.
		if err := tx.Commit(ctx); err != nil {
			return false, &domain.RepositoryError{Err: err}
		}
		return true, nil
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO user_registrations (user_id, email, registered_at, recorded_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (user_id) DO NOTHING`,
		reg.UserID, reg.Email, reg.RegisteredAt, reg.RecordedAt,
	); err != nil {
		return false, &domain.RepositoryError{Err: fmt.Errorf("insert user_registration: %w", err)}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, &domain.RepositoryError{Err: err}
	}
	return false, nil
}

// GetUserRegistration runs inside a read-only transaction (CLAUDE.md: every
// endpoint's DB access runs in one transaction; reads use a read-only tx).
func (r *Repository) GetUserRegistration(ctx context.Context, userID uuid.UUID) (domain.UserRegistration, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.UserRegistration{}, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx)

	const query = `
		SELECT user_id, email, registered_at, recorded_at
		FROM user_registrations
		WHERE user_id = $1`

	var reg domain.UserRegistration
	err = tx.QueryRow(ctx, query, userID).
		Scan(&reg.UserID, &reg.Email, &reg.RegisteredAt, &reg.RecordedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.UserRegistration{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.UserRegistration{}, &domain.RepositoryError{Err: fmt.Errorf("query user_registration: %w", err)}
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.UserRegistration{}, &domain.RepositoryError{Err: err}
	}
	return reg, nil
}
