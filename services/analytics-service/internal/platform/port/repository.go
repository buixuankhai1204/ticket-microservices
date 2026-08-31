package port

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
)

type Repository interface {
	GetEventBookingStats(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) (domain.EventBookingStats, error)

	RecordUserRegistration(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, reg domain.UserRegistration) (alreadyProcessed bool, err error)

	GetUserRegistration(ctx context.Context, tx pgx.Tx, userID uuid.UUID) (domain.UserRegistration, error)
}
