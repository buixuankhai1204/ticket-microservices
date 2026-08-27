package domain

import (
	"context"

	"github.com/google/uuid"
)

// EventFilter narrows a ListEvents query. A zero value lists every event.
// Pagination is passed separately (see the shared Pagination type).
type EventFilter struct {
	// UpcomingOnly restricts the result to events whose EndsAt is in the future.
	UpcomingOnly bool
}

// Repository is the store event-service reads events and seats from. It is
// implemented in adapter/repository/postgres; the domain layer knows nothing
// about the driver.
type Repository interface {
	// ListEvents returns one page of events matching the filter, ordered newest
	// first, plus the total match count ignoring limit/offset (for the response
	// envelope).
	ListEvents(ctx context.Context, f EventFilter, p Pagination) (events []Event, total int, err error)

	// GetEvent returns one event by ID, or ErrNotFound.
	GetEvent(ctx context.Context, id uuid.UUID) (Event, error)

	// ListSeatsForEvent returns one page of the event's seats, ordered by
	// section, row, number, plus the total seat count for the event. It returns
	// ErrNotFound if the event itself does not exist (so the caller can tell
	// "no seats" from "no such event").
	ListSeatsForEvent(ctx context.Context, eventID uuid.UUID, p Pagination) (seats []Seat, total int, err error)
}
