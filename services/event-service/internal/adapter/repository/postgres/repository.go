package postgres

import (
	"context"
	"encoding/json"
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

type rowScanner interface {
	Scan(dest ...any) error
}

func scanSeat(row rowScanner) (domain.Seat, error) {
	var s domain.Seat
	err := row.Scan(&s.ID, &s.EventID, &s.Section, &s.Row, &s.Number, &s.Status, &s.PriceMinor)
	return s, err
}

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
		s, err := scanSeat(rows)
		if err != nil {
			return nil, 0, &domain.RepositoryError{Err: fmt.Errorf("scan seat: %w", err)}
		}
		seats = append(seats, s)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, &domain.RepositoryError{Err: fmt.Errorf("iterate seats: %w", err)}
	}

	return seats, total, nil
}

func (r *Repository) LockSeatsForReservation(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, seatIDs []uuid.UUID) ([]domain.Seat, error) {
	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM events WHERE id = $1)`, eventID,
	).Scan(&exists); err != nil {
		return nil, &domain.RepositoryError{Err: fmt.Errorf("check event exists: %w", err)}
	}
	if !exists {
		return nil, domain.ErrNotFound
	}

	const query = `
		SELECT id, event_id, section, row, number, status, price_minor
		FROM seats
		WHERE event_id = $1 AND id = ANY($2)
		ORDER BY id
		FOR UPDATE`

	rows, err := tx.Query(ctx, query, eventID, seatIDs)
	if err != nil {
		return nil, &domain.RepositoryError{Err: fmt.Errorf("lock seats: %w", err)}
	}
	defer rows.Close()

	var seats []domain.Seat
	for rows.Next() {
		s, err := scanSeat(rows)
		if err != nil {
			return nil, &domain.RepositoryError{Err: fmt.Errorf("scan seat: %w", err)}
		}
		seats = append(seats, s)
	}
	if err := rows.Err(); err != nil {
		return nil, &domain.RepositoryError{Err: fmt.Errorf("iterate seats: %w", err)}
	}

	return seats, nil
}

func (r *Repository) UpdateSeatsStatus(ctx context.Context, tx pgx.Tx, seatIDs []uuid.UUID, status string) error {
	if _, err := tx.Exec(ctx,
		`UPDATE seats SET status = $1 WHERE id = ANY($2)`,
		status, seatIDs,
	); err != nil {
		return &domain.RepositoryError{Err: fmt.Errorf("update seats status: %w", err)}
	}
	return nil
}

func (r *Repository) CreateSeatReservation(ctx context.Context, tx pgx.Tx, bookingID, eventID uuid.UUID, seatIDs []uuid.UUID) error {
	if _, err := tx.Exec(ctx,
		`INSERT INTO seat_reservations (booking_id, event_id, seat_ids) VALUES ($1, $2, $3)`,
		bookingID, eventID, seatIDs,
	); err != nil {
		return &domain.RepositoryError{Err: fmt.Errorf("insert seat_reservation: %w", err)}
	}
	return nil
}

func (r *Repository) WriteOutbox(ctx context.Context, tx pgx.Tx, ev domain.OutboxEvent) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return &domain.RepositoryError{Err: fmt.Errorf("marshal outbox payload: %w", err)}
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO outbox_events (id, aggregate_id, aggregate_type, event_type, payload) VALUES ($1, $2, $3, $4, $5)`,
		ev.EventID(), ev.AggregateID(), ev.AggregateType(), ev.EventType(), payload,
	); err != nil {
		return &domain.RepositoryError{Err: fmt.Errorf("insert outbox event: %w", err)}
	}

	if _, err := tx.Exec(ctx, `DELETE FROM outbox_events WHERE id = $1`, ev.EventID()); err != nil {
		return &domain.RepositoryError{Err: fmt.Errorf("delete outbox event: %w", err)}
	}

	return nil
}

func (r *Repository) MarkEventProcessed(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) (bool, error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		eventID,
	)
	if err != nil {
		return false, &domain.RepositoryError{Err: fmt.Errorf("mark processed_events: %w", err)}
	}
	return tag.RowsAffected() == 0, nil
}
