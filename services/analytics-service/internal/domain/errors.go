package domain

import "errors"

var (
	ErrNotFound = errors.New("resource not found")

	ErrInvalidOutcomeStatus = errors.New("invalid booking outcome status")

	ErrInvalidUserRegistration = errors.New("invalid user registration")

	ErrInvalidUserLogin = errors.New("invalid user login")
)

type RepositoryError struct{ Err error }

func (e *RepositoryError) Error() string { return "repository error: " + e.Err.Error() }

func (e *RepositoryError) Unwrap() error { return e.Err }
