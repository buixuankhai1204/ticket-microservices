package kafka

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
)

func TestOnlyFailuresThatCouldSucceedLaterAreRetried(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"serialization failure", &domain.RepositoryError{Err: &pgconn.PgError{Code: "40001"}}, true},
		{"deadlock", &domain.RepositoryError{Err: &pgconn.PgError{Code: "40P01"}}, true},
		{"lock not available", &domain.RepositoryError{Err: &pgconn.PgError{Code: "55P03"}}, true},
		{"too many connections", &domain.RepositoryError{Err: &pgconn.PgError{Code: "53300"}}, true},
		{"connection failure", &domain.RepositoryError{Err: &pgconn.PgError{Code: "08006"}}, true},
		{"server shutting down", &domain.RepositoryError{Err: &pgconn.PgError{Code: "57P01"}}, true},
		{"wrapped twice", fmt.Errorf("handler: %w", &domain.RepositoryError{Err: &pgconn.PgError{Code: "40001"}}), true},
		{"no database code at all, such as a pool timeout", &domain.RepositoryError{Err: errors.New("pool exhausted")}, true},
		{"unique violation", &domain.RepositoryError{Err: &pgconn.PgError{Code: "23505"}}, false},
		{"foreign key violation", &domain.RepositoryError{Err: &pgconn.PgError{Code: "23503"}}, false},
		{"check violation", &domain.RepositoryError{Err: &pgconn.PgError{Code: "23514"}}, false},
		{"invalid text representation", &domain.RepositoryError{Err: &pgconn.PgError{Code: "22P02"}}, false},
		{"undefined table", &domain.RepositoryError{Err: &pgconn.PgError{Code: "42P01"}}, false},
		{"a rule of the domain", domain.ErrInvalidUserRegistration, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryable(tc.err); got != tc.want {
				t.Fatalf("isRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
