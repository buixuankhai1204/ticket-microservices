package domain

import (
	"context"

	"github.com/google/uuid"
)

// Repository is the read-model store analytics-service queries and projects into.
// It is implemented in adapter/repository/postgres; the domain layer knows
// nothing about the driver.
type Repository interface {
	// GetEventBookingStats returns aggregate booking counts for one event.
	GetEventBookingStats(ctx context.Context, eventID uuid.UUID) (EventBookingStats, error)

	// RecordUserRegistration writes the user_registrations row and the
	// processed_events idempotency marker for eventID in ONE transaction.
	// alreadyProcessed is true (and nothing is written) when eventID was already
	// recorded — the idempotent no-op path for a Kafka redelivery.
	RecordUserRegistration(ctx context.Context, eventID uuid.UUID, reg UserRegistration) (alreadyProcessed bool, err error)

	// GetUserRegistration returns the projected registration for a user, or
	// ErrNotFound.
	GetUserRegistration(ctx context.Context, userID uuid.UUID) (UserRegistration, error)
}
