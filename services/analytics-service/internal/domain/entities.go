package domain

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	OutcomeConfirmed = "confirmed"
	OutcomeCancelled = "cancelled"
)

type BookingOutcome struct {
	ID         uuid.UUID
	BookingID  uuid.UUID
	EventID    uuid.UUID
	Status     string
	OccurredAt time.Time
	RecordedAt time.Time
}

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

type EventBookingStats struct {
	EventID   uuid.UUID
	Confirmed int64
	Cancelled int64
}

type UserRegistration struct {
	UserID       uuid.UUID
	Email        string
	RegisteredAt time.Time
	RecordedAt   time.Time
}

func NewUserRegistration(userID uuid.UUID, email string, registeredAt time.Time) (*UserRegistration, error) {
	if email == "" || !strings.Contains(email, "@") {
		return nil, ErrInvalidUserRegistration
	}
	return &UserRegistration{
		UserID:       userID,
		Email:        email,
		RegisteredAt: registeredAt,
		RecordedAt:   time.Now().UTC(),
	}, nil
}
