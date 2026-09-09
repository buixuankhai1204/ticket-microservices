//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/usecase"
)

func TestListEvents_HappyPath_ReturnsPageWithTotalAndUpcomingFilter(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	now := time.Now().UTC()
	oneSection := []domain.SectionSpec{{Name: "A", Rows: 1, SeatsPerRow: 2, PriceMinor: 1000}}

	upcomingA := createEventViaUseCase(t, "Upcoming A", now.Add(24*time.Hour), now.Add(27*time.Hour), oneSection)
	upcomingB := createEventViaUseCase(t, "Upcoming B", now.Add(48*time.Hour), now.Add(50*time.Hour), oneSection)
	past := createEventViaUseCase(t, "Past Show", now.Add(-5*time.Hour), now.Add(-2*time.Hour), oneSection)

	uc := usecase.NewListEventsUseCase(testPool, newRepo())

	all, total, err := uc.Execute(ctx, domain.EventFilter{}, mustPagination(t, 20, 0))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if total != 3 || len(all) != 3 {
		t.Fatalf("unfiltered list = %d rows / total %d, want 3 / 3", len(all), total)
	}
	seen := map[uuid.UUID]bool{}
	for _, e := range all {
		seen[e.ID] = true
	}
	for _, want := range []uuid.UUID{upcomingA.ID, upcomingB.ID, past.ID} {
		if !seen[want] {
			t.Errorf("event %s missing from unfiltered list", want)
		}
	}
	for i := 1; i < len(all); i++ {
		if all[i-1].CreatedAt.Before(all[i].CreatedAt) {
			t.Errorf("list not ordered created_at DESC at index %d", i)
		}
	}

	upcoming, upcomingTotal, err := uc.Execute(ctx, domain.EventFilter{UpcomingOnly: true}, mustPagination(t, 20, 0))
	if err != nil {
		t.Fatalf("Execute upcoming-only: %v", err)
	}
	if upcomingTotal != 2 || len(upcoming) != 2 {
		t.Fatalf("upcoming-only list = %d rows / total %d, want 2 / 2", len(upcoming), upcomingTotal)
	}
	for _, e := range upcoming {
		if e.ID == past.ID {
			t.Errorf("past event %s leaked into upcoming-only list", past.ID)
		}
	}
}

func TestGetEvent_HappyPath_AndNotFound(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	now := time.Now().UTC()
	created := createEventViaUseCase(t, "Fetch Me", now.Add(24*time.Hour), now.Add(27*time.Hour),
		[]domain.SectionSpec{{Name: "A", Rows: 1, SeatsPerRow: 1, PriceMinor: 500}})

	uc := usecase.NewGetEventUseCase(testPool, newRepo())

	got, err := uc.Execute(ctx, created.ID)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got.ID != created.ID || got.Name != "Fetch Me" || got.Venue != "Test Arena" {
		t.Errorf("got %+v, want id/name/venue of %+v", got, created)
	}
	if !got.StartsAt.Equal(created.StartsAt) || !got.EndsAt.Equal(created.EndsAt) {
		t.Errorf("times = %s/%s, want %s/%s", got.StartsAt, got.EndsAt, created.StartsAt, created.EndsAt)
	}

	if _, err := uc.Execute(ctx, uuid.New()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Execute(random id) error = %v, want domain.ErrNotFound", err)
	}
}

func TestListEventSeats_HappyPath_AndUnknownEvent(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	now := time.Now().UTC()
	created := createEventViaUseCase(t, "Seat Map Show", now.Add(24*time.Hour), now.Add(27*time.Hour),
		[]domain.SectionSpec{{Name: "A", Rows: 2, SeatsPerRow: 3, PriceMinor: 1000}})

	uc := usecase.NewListEventSeatsUseCase(testPool, newRepo())

	seats, total, err := uc.Execute(ctx, created.ID, mustPagination(t, 20, 0))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if total != 6 || len(seats) != 6 {
		t.Fatalf("seats = %d / total %d, want 6 / 6", len(seats), total)
	}
	for _, s := range seats {
		if s.EventID != created.ID {
			t.Errorf("seat %s belongs to event %s, want %s", s.ID, s.EventID, created.ID)
		}
		if s.Status != domain.SeatAvailable {
			t.Errorf("seat %s status = %q, want %q", s.ID, s.Status, domain.SeatAvailable)
		}
	}

	if _, _, err := uc.Execute(ctx, uuid.New(), mustPagination(t, 20, 0)); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Execute(random event id) error = %v, want domain.ErrNotFound", err)
	}
}
