package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
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

// ListEvents runs the COUNT and the page SELECT inside one read-only
// transaction so the two can never disagree because of a concurrent write
// between them (CLAUDE.md: every list endpoint's count + page share one
// read-only tx).
func (r *Repository) ListEvents(ctx context.Context, f domain.EventFilter, p domain.Pagination) ([]domain.Event, int, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, 0, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx) // no-op after Commit; rolls back on any early return

	var total int
	if err := tx.QueryRow(ctx,
		`SELECT COUNT(*) FROM events WHERE (NOT $1::bool OR ends_at > now())`,
		f.UpcomingOnly,
	).Scan(&total); err != nil {
		return nil, 0, &domain.RepositoryError{Err: fmt.Errorf("count events: %w", err)}
	}

	const query = `
		SELECT id, name, description, venue, starts_at, ends_at, created_at
		FROM events
		WHERE (NOT $1::bool OR ends_at > now())
		ORDER BY created_at DESC, id DESC
		LIMIT $2 OFFSET $3`

	rows, err := tx.Query(ctx, query, f.UpcomingOnly, p.Limit, p.Offset)
	if err != nil {
		return nil, 0, &domain.RepositoryError{Err: fmt.Errorf("query events: %w", err)}
	}
	defer rows.Close()

	var events []domain.Event
	for rows.Next() {
		var e domain.Event
		if err := rows.Scan(&e.ID, &e.Name, &e.Description, &e.Venue, &e.StartsAt, &e.EndsAt, &e.CreatedAt); err != nil {
			return nil, 0, &domain.RepositoryError{Err: fmt.Errorf("scan event: %w", err)}
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, &domain.RepositoryError{Err: fmt.Errorf("iterate events: %w", err)}
	}
	rows.Close()

	if err := tx.Commit(ctx); err != nil {
		return nil, 0, &domain.RepositoryError{Err: err}
	}
	return events, total, nil
}

// GetEvent runs inside a read-only transaction (CLAUDE.md: every endpoint's DB
// access runs in one transaction; reads use a read-only tx).
func (r *Repository) GetEvent(ctx context.Context, id uuid.UUID) (domain.Event, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.Event{}, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx)

	const query = `
		SELECT id, name, description, venue, starts_at, ends_at, created_at
		FROM events
		WHERE id = $1`

	var e domain.Event
	err = tx.QueryRow(ctx, query, id).
		Scan(&e.ID, &e.Name, &e.Description, &e.Venue, &e.StartsAt, &e.EndsAt, &e.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Event{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Event{}, &domain.RepositoryError{Err: fmt.Errorf("query event: %w", err)}
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.Event{}, &domain.RepositoryError{Err: err}
	}
	return e, nil
}

// CreateEventWithSeats inserts the event row and every seat row in one
// read-write transaction (CLAUDE.md: every write handler starts a transaction).
// The seats go in via COPY rather than N INSERTs so creating an event with a
// large seat map stays one round trip's worth of work.
func (r *Repository) CreateEventWithSeats(ctx context.Context, e domain.Event, seats []domain.Seat) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx) // no-op after Commit; rolls back on any early return

	if _, err := tx.Exec(ctx,
		`INSERT INTO events (id, name, description, venue, starts_at, ends_at, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		e.ID, e.Name, e.Description, e.Venue, e.StartsAt, e.EndsAt, e.CreatedAt,
	); err != nil {
		return &domain.RepositoryError{Err: fmt.Errorf("insert event: %w", err)}
	}

	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{"seats"},
		[]string{"id", "event_id", "section", "row", "number", "status", "price_minor"},
		pgx.CopyFromSlice(len(seats), func(i int) ([]any, error) {
			s := seats[i]
			return []any{s.ID, s.EventID, s.Section, s.Row, s.Number, s.Status, s.PriceMinor}, nil
		}),
	); err != nil {
		return &domain.RepositoryError{Err: fmt.Errorf("bulk insert seats: %w", err)}
	}

	if err := tx.Commit(ctx); err != nil {
		return &domain.RepositoryError{Err: err}
	}
	return nil
}

// ListSeatsForEvent runs inside one read-only transaction that first confirms
// the event exists (so the caller can tell "no seats" from "no such event"),
// then reads the COUNT and the seat page from the same snapshot.
func (r *Repository) ListSeatsForEvent(ctx context.Context, eventID uuid.UUID, p domain.Pagination) ([]domain.Seat, int, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, 0, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx)

	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM events WHERE id = $1)`, eventID,
	).Scan(&exists); err != nil {
		return nil, 0, &domain.RepositoryError{Err: fmt.Errorf("check event exists: %w", err)}
	}
	if !exists {
		return nil, 0, domain.ErrNotFound
	}

	var total int
	if err := tx.QueryRow(ctx,
		`SELECT COUNT(*) FROM seats WHERE event_id = $1`, eventID,
	).Scan(&total); err != nil {
		return nil, 0, &domain.RepositoryError{Err: fmt.Errorf("count seats: %w", err)}
	}

	const query = `
		SELECT id, event_id, section, row, number, status, price_minor
		FROM seats
		WHERE event_id = $1
		ORDER BY section, row, number, id
		LIMIT $2 OFFSET $3`

	rows, err := tx.Query(ctx, query, eventID, p.Limit, p.Offset)
	if err != nil {
		return nil, 0, &domain.RepositoryError{Err: fmt.Errorf("query seats: %w", err)}
	}
	defer rows.Close()

	var seats []domain.Seat
	for rows.Next() {
		var s domain.Seat
		if err := rows.Scan(&s.ID, &s.EventID, &s.Section, &s.Row, &s.Number, &s.Status, &s.PriceMinor); err != nil {
			return nil, 0, &domain.RepositoryError{Err: fmt.Errorf("scan seat: %w", err)}
		}
		seats = append(seats, s)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, &domain.RepositoryError{Err: fmt.Errorf("iterate seats: %w", err)}
	}
	rows.Close()

	if err := tx.Commit(ctx); err != nil {
		return nil, 0, &domain.RepositoryError{Err: err}
	}
	return seats, total, nil
}
