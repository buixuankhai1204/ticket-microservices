package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/port"
)

type Repository struct{}

func New() *Repository {
	return &Repository{}
}

var _ port.Repository = (*Repository)(nil)

func (r *Repository) ListEvents(ctx context.Context, tx pgx.Tx, f domain.EventFilter, p domain.Pagination) ([]domain.Event, int, error) {
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

	return events, total, nil
}

func (r *Repository) GetEvent(ctx context.Context, tx pgx.Tx, id uuid.UUID) (domain.Event, error) {
	const query = `
		SELECT id, name, description, venue, starts_at, ends_at, created_at
		FROM events
		WHERE id = $1`

	var e domain.Event
	err := tx.QueryRow(ctx, query, id).
		Scan(&e.ID, &e.Name, &e.Description, &e.Venue, &e.StartsAt, &e.EndsAt, &e.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Event{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Event{}, &domain.RepositoryError{Err: fmt.Errorf("query event: %w", err)}
	}

	return e, nil
}

func (r *Repository) CreateEventWithSeats(ctx context.Context, tx pgx.Tx, e domain.Event, seats []domain.Seat) error {
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

	return nil
}

func (r *Repository) ListSeatsForEvent(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, p domain.Pagination) ([]domain.Seat, int, error) {
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

	return seats, total, nil
}
