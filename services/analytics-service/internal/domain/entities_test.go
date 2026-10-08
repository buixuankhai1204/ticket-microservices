package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
)

func TestBookingOutcomeAcceptsOnlyTheTwoFinalStatuses(t *testing.T) {
	bookingID, eventID := uuid.New(), uuid.New()
	occurredAt := time.Date(2026, 3, 4, 8, 30, 0, 0, time.UTC)

	tests := []struct {
		status  string
		wantErr error
	}{
		{domain.OutcomeConfirmed, nil},
		{domain.OutcomeCancelled, nil},
		{"pending", domain.ErrInvalidOutcomeStatus},
		{"CONFIRMED", domain.ErrInvalidOutcomeStatus},
		{"", domain.ErrInvalidOutcomeStatus},
	}
	for _, tc := range tests {
		t.Run("status "+tc.status, func(t *testing.T) {
			outcome, err := domain.NewBookingOutcome(bookingID, eventID, tc.status, occurredAt)

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr != nil {
				if outcome != nil {
					t.Fatalf("outcome = %+v, want nil on error", outcome)
				}
				return
			}
			if outcome.BookingID != bookingID || outcome.EventID != eventID || outcome.Status != tc.status {
				t.Fatalf("outcome = %+v, want booking %s event %s status %s", outcome, bookingID, eventID, tc.status)
			}
			if !outcome.OccurredAt.Equal(occurredAt) {
				t.Fatalf("OccurredAt = %v, want the event time %v, not the time it was recorded", outcome.OccurredAt, occurredAt)
			}
			if outcome.ID == uuid.Nil || outcome.ID.Version() != 4 {
				t.Fatalf("ID = %s, want a generated v4 UUID", outcome.ID)
			}
		})
	}
}

func TestUserRegistrationNeedsSomethingThatLooksLikeAnEmail(t *testing.T) {
	tests := []struct {
		email   string
		wantErr bool
	}{
		{"ada@example.com", false},
		{"", true},
		{"ada.example.com", true},
	}
	for _, tc := range tests {
		t.Run("email "+tc.email, func(t *testing.T) {
			reg, err := domain.NewUserRegistration(uuid.New(), tc.email, time.Now())

			if tc.wantErr {
				if !errors.Is(err, domain.ErrInvalidUserRegistration) || reg != nil {
					t.Fatalf("got (%+v, %v), want ErrInvalidUserRegistration and no registration", reg, err)
				}
				return
			}
			if err != nil || reg.Email != tc.email {
				t.Fatalf("got (%+v, %v), want a registration for %s", reg, err, tc.email)
			}
		})
	}
}

func TestUserLoginNeedsAnEmailAndALoginTime(t *testing.T) {
	loggedInAt := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)
	tests := []struct {
		name       string
		email      string
		loggedInAt time.Time
		wantErr    bool
	}{
		{"valid", "ada@example.com", loggedInAt, false},
		{"missing at sign", "ada.example.com", loggedInAt, true},
		{"empty email", "", loggedInAt, true},
		{"zero login time", "ada@example.com", time.Time{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			login, err := domain.NewUserLogin(uuid.New(), tc.email, tc.loggedInAt)

			if tc.wantErr {
				if !errors.Is(err, domain.ErrInvalidUserLogin) || login != nil {
					t.Fatalf("got (%+v, %v), want ErrInvalidUserLogin and no login", login, err)
				}
				return
			}
			if err != nil || !login.LoggedInAt.Equal(loggedInAt) {
				t.Fatalf("got (%+v, %v), want a login at %v", login, err, loggedInAt)
			}
		})
	}
}

func TestRepositoryErrorStaysMatchableThroughWrapping(t *testing.T) {
	inner := errors.New("connection refused")
	err := error(&domain.RepositoryError{Err: inner})

	var repoErr *domain.RepositoryError
	if !errors.As(err, &repoErr) || !errors.Is(err, inner) {
		t.Fatalf("a RepositoryError must be found with errors.As and unwrap to its cause: %v", err)
	}
	if got := err.Error(); got != "repository error: connection refused" {
		t.Fatalf("Error() = %q", got)
	}
}
