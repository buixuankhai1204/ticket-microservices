package domain

import "errors"

var (
	ErrNotFound = errors.New("resource not found")

	ErrInvalidEvent = errors.New("invalid event")

	ErrInvalidSeat = errors.New("invalid seat")

	ErrSeatUnavailable = errors.New("seat unavailable")

	ErrInvalidPagination = errors.New("invalid pagination")

	ErrEventRequiresSeats = errors.New("event requires at least one seat")

	ErrInvalidLayout = errors.New("invalid seat layout")

	ErrLayoutTooLarge = errors.New("seat layout exceeds the per-event maximum")
)

type RepositoryError struct{ Err error }

func (e *RepositoryError) Error() string { return "repository error: " + e.Err.Error() }

func (e *RepositoryError) Unwrap() error { return e.Err }
