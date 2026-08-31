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
}
