package port

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
)

type Repository interface {
	ListEvents(ctx context.Context, tx pgx.Tx, f domain.EventFilter, p domain.Pagination) (events []domain.Event, total int, err error)

	GetEvent(ctx context.Context, tx pgx.Tx, id uuid.UUID) (domain.Event, error)

	CreateEventWithSeats(ctx context.Context, tx pgx.Tx, e domain.Event, seats []domain.Seat) error

	ListSeatsForEvent(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, p domain.Pagination) (seats []domain.Seat, total int, err error)

	LockSeatsForReservation(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, seatIDs []uuid.UUID) (seats []domain.Seat, err error)

	UpdateSeatsStatus(ctx context.Context, tx pgx.Tx, seatIDs []uuid.UUID, status string) error

	ReleaseReservedSeats(ctx context.Context, tx pgx.Tx, seatIDs []uuid.UUID) error

	CreateSeatReservation(ctx context.Context, tx pgx.Tx, bookingID, eventID uuid.UUID, seatIDs []uuid.UUID) error

	LockSeatReservation(ctx context.Context, tx pgx.Tx, bookingID uuid.UUID) (domain.SeatReservation, error)

	ListStaleHeldReservations(ctx context.Context, tx pgx.Tx, olderThanSecs int) ([]domain.SeatReservation, error)

	UpdateSeatReservationStatus(ctx context.Context, tx pgx.Tx, bookingID uuid.UUID, status string) error

	// UpdateSeatReservationStatusBatch sets the same status on every reservation
	// in bookingIDs in one statement — for a reaper/sweep usecase acting on many
	// rows per tick, where a per-row UpdateSeatReservationStatus call would be
	// an N+1 query loop.
	UpdateSeatReservationStatusBatch(ctx context.Context, tx pgx.Tx, bookingIDs []uuid.UUID, status string) error

	WriteOutbox(ctx context.Context, tx pgx.Tx, ev domain.OutboxEvent) error

	MarkEventProcessed(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) (alreadyProcessed bool, err error)
}
