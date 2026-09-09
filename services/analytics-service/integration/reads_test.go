//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/usecase"
)

func TestGetEventStats_HappyPath_CountsByStatusScopedToEvent(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	eventID := uuid.New()
	otherEventID := uuid.New()
	for i := 0; i < 3; i++ {
		seedBookingOutcome(t, ctx, eventID, domain.OutcomeConfirmed)
	}
	for i := 0; i < 2; i++ {
		seedBookingOutcome(t, ctx, eventID, domain.OutcomeCancelled)
	}
	seedBookingOutcome(t, ctx, otherEventID, domain.OutcomeConfirmed)
	seedBookingOutcome(t, ctx, otherEventID, domain.OutcomeCancelled)

	uc := usecase.NewGetEventStatsUseCase(testPool, newRepo())

	stats, err := uc.Execute(ctx, eventID)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if stats.EventID != eventID || stats.Confirmed != 3 || stats.Cancelled != 2 {
		t.Errorf("stats = %+v, want {EventID:%s Confirmed:3 Cancelled:2}", stats, eventID)
	}

	empty, err := uc.Execute(ctx, uuid.New())
	if err != nil {
		t.Fatalf("Execute(unknown event): %v", err)
	}
	if empty.Confirmed != 0 || empty.Cancelled != 0 {
		t.Errorf("unknown event stats = %+v, want zero counts", empty)
	}
}

func TestGetUserRegistration_HappyPath_AndNotFound(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	want := domain.UserRegistration{
		UserID:       uuid.New(),
		Email:        "alice@example.com",
		RegisteredAt: time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Microsecond),
		RecordedAt:   time.Now().UTC().Add(-47 * time.Hour).Truncate(time.Microsecond),
	}
	seedUserRegistrationRow(t, ctx, want)

	uc := usecase.NewGetUserRegistrationUseCase(testPool, newRepo())

	got, err := uc.Execute(ctx, want.UserID)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got.UserID != want.UserID || got.Email != want.Email ||
		!got.RegisteredAt.Equal(want.RegisteredAt) || !got.RecordedAt.Equal(want.RecordedAt) {
		t.Errorf("got %+v, want %+v", got, want)
	}

	if _, err := uc.Execute(ctx, uuid.New()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Execute(unknown user) error = %v, want domain.ErrNotFound", err)
	}
}
