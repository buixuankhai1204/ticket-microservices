package domain

import "errors"

var (
	// ErrNotFound is returned when a requested read-model record does not exist.
	ErrNotFound = errors.New("resource not found")

	// ErrInvalidOutcomeStatus is returned by NewBookingOutcome for a status
	// outside the allowed terminal set.
	ErrInvalidOutcomeStatus = errors.New("invalid booking outcome status")

	// ErrInvalidUserRegistration is returned by NewUserRegistration for a
	// malformed payload (e.g. empty / non-email address). It is a permanent
	// rejection — the Kafka consumer dead-letters such a message rather than
	// retrying it.
	ErrInvalidUserRegistration = errors.New("invalid user registration")
)

// RepositoryError wraps an infrastructure failure from the repository layer so
// usecase can tell it apart from a domain rule violation without importing pgx.
type RepositoryError struct{ Err error }

func (e *RepositoryError) Error() string { return "repository error: " + e.Err.Error() }

func (e *RepositoryError) Unwrap() error { return e.Err }
