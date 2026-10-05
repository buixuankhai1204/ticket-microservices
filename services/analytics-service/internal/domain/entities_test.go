package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func assertV4UUID(t *testing.T, id uuid.UUID, label string) {
	t.Helper()
	if id == uuid.Nil {
		t.Fatalf("%s is the nil UUID", label)
	}
	if id.Version() != 4 {
		t.Fatalf("%s version = %d, want 4", label, id.Version())
	}
	if id.Variant() != uuid.RFC4122 {
		t.Fatalf("%s variant = %v, want RFC4122", label, id.Variant())
	}
}

func TestNewBookingOutcomeConfirmed(t *testing.T) {
	bookingID, eventID := uuid.New(), uuid.New()
	occurredAt := time.Date(2026, 3, 4, 8, 30, 0, 0, time.UTC)

	out, err := NewBookingOutcome(bookingID, eventID, OutcomeConfirmed, occurredAt)
	if err != nil {
		t.Fatalf("NewBookingOutcome returned error: %v", err)
	}
	assertV4UUID(t, out.ID, "BookingOutcome.ID")
	if out.BookingID != bookingID || out.EventID != eventID {
		t.Fatalf("ids not copied: %+v", out)
	}
	if out.Status != OutcomeConfirmed {
		t.Fatalf("Status = %q, want %q", out.Status, OutcomeConfirmed)
	}
	if !out.OccurredAt.Equal(occurredAt) {
		t.Fatalf("OccurredAt = %v, want %v", out.OccurredAt, occurredAt)
	}
	if out.RecordedAt.IsZero() {
		t.Fatalf("RecordedAt not set")
	}
}

func TestNewBookingOutcomeRejectsUnknownStatus(t *testing.T) {
	out, err := NewBookingOutcome(uuid.New(), uuid.New(), "settled", time.Now())
	if !errors.Is(err, ErrInvalidOutcomeStatus) {
		t.Fatalf("err = %v, want ErrInvalidOutcomeStatus", err)
	}
	if out != nil {
		t.Fatalf("outcome = %+v, want nil on error", out)
	}
}

func TestNewUserLoginRejectsZeroLoginTime(t *testing.T) {
	login, err := NewUserLogin(uuid.New(), "ada@example.com", time.Time{})
	if !errors.Is(err, ErrInvalidUserLogin) {
		t.Fatalf("err = %v, want ErrInvalidUserLogin", err)
	}
	if login != nil {
		t.Fatalf("login = %+v, want nil on error", login)
	}
}
