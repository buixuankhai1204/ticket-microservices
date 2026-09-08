package domain

import (
	"time"

	"github.com/google/uuid"
)

type UserCreated struct {
	EventID   uuid.UUID
	UserID    uuid.UUID
	Email     string
	CreatedAt time.Time
}

type UserLoggedIn struct {
	EventID    uuid.UUID
	UserID     uuid.UUID
	Email      string
	LoggedInAt time.Time
}

type BookingConfirmed struct {
	EventID         uuid.UUID
	BookingID       uuid.UUID
	UserID          uuid.UUID
	TicketedEventID uuid.UUID
	SeatIDs         []uuid.UUID
	OccurredAt      time.Time
}

type BookingCancelled struct {
	EventID         uuid.UUID
	BookingID       uuid.UUID
	UserID          uuid.UUID
	TicketedEventID uuid.UUID
	SeatIDs         []uuid.UUID
	Reason          string
	OccurredAt      time.Time
}
