package domain

import (
	"context"

	"github.com/google/uuid"
)

// Repository is the read-model store analytics-service queries. It is implemented
// in adapter/repository/postgres; the domain layer knows nothing about the
// driver. The write side (recording saga outcomes consumed from Kafka) is added
// to this interface later by /add-go-saga-step.
type Repository interface {
	// GetEventBookingStats returns aggregate booking counts for one event.
	GetEventBookingStats(ctx context.Context, eventID uuid.UUID) (EventBookingStats, error)
}
