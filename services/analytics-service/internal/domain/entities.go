package domain

import (
	"time"

	"github.com/google/uuid"
)

// Allowed terminal statuses for a booking saga, as observed by analytics.
const (
	OutcomeConfirmed = "confirmed"
	OutcomeCancelled = "cancelled"
)

// BookingOutcome is the final, immutable result of one booking saga. It is a
// read-model record: analytics-service never originates these — it only records
// what booking-service / event-service already decided (see CLAUDE.md: analytics
// "consumes the final outcome as a read model only"). The write path is wired
// later with /add-go-saga-step; the HTTP surface stays read-only.
type BookingOutcome struct {
	ID         uuid.UUID
	BookingID  uuid.UUID
	EventID    uuid.UUID
	Status     string
	OccurredAt time.Time
	RecordedAt time.Time
}

// NewBookingOutcome constructs a BookingOutcome, enforcing the status invariant
// at the point of creation rather than leaving callers to remember to validate.
// The ID is generated here (UUID, never a sequential int — same type as a saga
// event's aggregate_id) so the record has an identity before its first insert.
func NewBookingOutcome(bookingID, eventID uuid.UUID, status string, occurredAt time.Time) (*BookingOutcome, error) {
	if status != OutcomeConfirmed && status != OutcomeCancelled {
		return nil, ErrInvalidOutcomeStatus
	}
	return &BookingOutcome{
		ID:         uuid.New(),
		BookingID:  bookingID,
		EventID:    eventID,
		Status:     status,
		OccurredAt: occurredAt,
		RecordedAt: time.Now().UTC(),
	}, nil
}

// EventBookingStats is an aggregate read model: confirmed / cancelled counts for
// one event. A pure value with no invariant of its own.
type EventBookingStats struct {
	EventID   uuid.UUID
	Confirmed int64
	Cancelled int64
}
