package domain

import "errors"

var (
	// ErrNotFound is returned when a requested event or seat does not exist.
	ErrNotFound = errors.New("resource not found")

	// ErrInvalidEvent is returned by NewEvent for a payload that breaks an
	// event invariant (blank name/venue, or end not after start).
	ErrInvalidEvent = errors.New("invalid event")

	// ErrInvalidSeat is returned by NewSeat for a malformed seat (nil event,
	// blank number, negative price).
	ErrInvalidSeat = errors.New("invalid seat")

	// ErrSeatUnavailable is returned by Seat.Reserve / Seat.Release when the
	// seat is not in the state the transition requires.
	ErrSeatUnavailable = errors.New("seat unavailable")

	// ErrInvalidPagination is returned by NewPagination for offset < 0 or
	// limit < 1. The HTTP layer maps it to 400.
	ErrInvalidPagination = errors.New("invalid pagination")

	// ErrEventRequiresSeats is returned by NewEventWithSeats when the layout has
	// no sections, or every generated seat was removed by an exception — an
	// event is always created together with its seat map. 400.
	ErrEventRequiresSeats = errors.New("event requires at least one seat")

	// ErrInvalidLayout is returned by NewEventWithSeats for a malformed seat
	// layout: a bad section (blank name, non-positive rows/seats-per-row,
	// negative price, duplicate section name) or a bad exception (targets a
	// seat outside the grid, duplicates another exception, or neither removes
	// nor reprices). 400.
	ErrInvalidLayout = errors.New("invalid seat layout")

	// ErrLayoutTooLarge is returned when a layout would expand to more than
	// MaxSeatsPerEvent seats. 400.
	ErrLayoutTooLarge = errors.New("seat layout exceeds the per-event maximum")
)

// RepositoryError wraps an infrastructure failure from the repository layer so
// usecase can tell it apart from a domain rule violation without importing pgx.
type RepositoryError struct{ Err error }

func (e *RepositoryError) Error() string { return "repository error: " + e.Err.Error() }

func (e *RepositoryError) Unwrap() error { return e.Err }
