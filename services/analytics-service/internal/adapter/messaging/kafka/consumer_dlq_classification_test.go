//go:build integration

package kafka

import (
	"errors"
	"fmt"
	"testing"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
)

func TestParseUserCreated_RejectsMalformedTransport(t *testing.T) {
	validBody := `{"event_id":"6f9619ff-8b86-d011-b42d-00cf4fc964ff",` +
		`"user_id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8",` +
		`"email":"x@y.com","created_at":"2026-08-27T10:00:00Z"}`

	cases := []struct {
		name string
		body string
	}{
		{"poison / not JSON", "this is not json at all"},
		{"truncated JSON", `{"event_id":`},
		{"bad event_id", `{"event_id":"not-a-uuid",` +
			`"user_id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8",` +
			`"email":"x@y.com","created_at":"2026-08-27T10:00:00Z"}`},
		{"bad user_id", `{"event_id":"6f9619ff-8b86-d011-b42d-00cf4fc964ff",` +
			`"user_id":"12345","email":"x@y.com","created_at":"2026-08-27T10:00:00Z"}`},
		{"bad created_at", `{"event_id":"6f9619ff-8b86-d011-b42d-00cf4fc964ff",` +
			`"user_id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8",` +
			`"email":"x@y.com","created_at":"not-a-timestamp"}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseUserCreated([]byte(tc.body)); err == nil {
				t.Fatalf("parseUserCreated(%q) = nil error, want non-nil", tc.body)
			}
		})
	}

	if _, err := parseUserCreated([]byte(validBody)); err != nil {
		t.Fatalf("parseUserCreated(valid body) = %v, want nil", err)
	}
}

func TestIsRetryable_RepositoryErrorIsRetryable(t *testing.T) {
	if !isRetryable(&domain.RepositoryError{Err: errors.New("connection reset")}) {
		t.Errorf("isRetryable(*domain.RepositoryError) = false, want true")
	}
	wrapped := fmt.Errorf("record user registration: %w",
		&domain.RepositoryError{Err: errors.New("deadlock detected")})
	if !isRetryable(wrapped) {
		t.Errorf("isRetryable(wrapped *domain.RepositoryError) = false, want true")
	}
}

func TestIsRetryable_DomainRejectionIsNotRetryable(t *testing.T) {
	if isRetryable(domain.ErrInvalidUserRegistration) {
		t.Errorf("isRetryable(domain.ErrInvalidUserRegistration) = true, want false")
	}
	if isRetryable(fmt.Errorf("wrap: %w", domain.ErrInvalidUserRegistration)) {
		t.Errorf("isRetryable(wrapped domain.ErrInvalidUserRegistration) = true, want false")
	}
}
