//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/usecase"
)

func TestCreateNewEvent_HappyPath_PersistsEventAndExpandedSeats(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	starts := time.Now().UTC().Add(24 * time.Hour)
	repricedFront := int64(9999)
	in := usecase.CreateNewEventInput{
		Name:        "Layout Expansion Show",
		Description: "front row repriced, one seat removed",
		Venue:       "Test Arena",
		StartsAt:    starts,
		EndsAt:      starts.Add(3 * time.Hour),
		Layout: domain.LayoutSpec{
			Sections: []domain.SectionSpec{
				{Name: "A", Rows: 2, SeatsPerRow: 3, PriceMinor: 5000},
				{Name: "B", Rows: 1, SeatsPerRow: 2, PriceMinor: 3000},
			},
			Exceptions: []domain.SeatException{
				{Section: "A", Row: "2", Number: "2", Remove: true},
				{Section: "B", Row: "1", Number: "1", PriceMinor: &repricedFront},
			},
		},
	}

	event, seats, err := usecase.NewCreateNewEventUseCase(testPool, newRepo()).Execute(ctx, in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if event.ID == uuid.Nil {
		t.Fatalf("event.ID is the nil UUID")
	}
	if len(seats) != 7 {
		t.Fatalf("returned seats = %d, want 7 (8 generated - 1 removed)", len(seats))
	}

	if n := countRows(t, `SELECT COUNT(*) FROM events WHERE id = $1 AND name = $2 AND venue = $3`,
		event.ID, in.Name, in.Venue); n != 1 {
		t.Fatalf("events rows for created id = %d, want 1", n)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM seats WHERE event_id = $1`, event.ID); n != 7 {
		t.Errorf("persisted seats = %d, want 7", n)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM seats WHERE event_id = $1 AND status = $2`,
		event.ID, domain.SeatAvailable); n != 7 {
		t.Errorf("available seats = %d, want 7", n)
	}
	if n := countRows(t,
		`SELECT COUNT(*) FROM seats WHERE event_id = $1 AND section = 'A' AND "row" = '2' AND number = '2'`,
		event.ID); n != 0 {
		t.Errorf("removed seat A/2/2 is still present (%d rows)", n)
	}
	if n := countRows(t,
		`SELECT COUNT(*) FROM seats WHERE event_id = $1 AND section = 'B' AND "row" = '1' AND number = '1' AND price_minor = $2`,
		event.ID, repricedFront); n != 1 {
		t.Errorf("repriced seat B/1/1 not stored at %d minor units", repricedFront)
	}
	if n := countRows(t,
		`SELECT COUNT(*) FROM seats WHERE event_id = $1 AND section = 'A' AND price_minor = 5000`,
		event.ID); n != 5 {
		t.Errorf("section A seats at base price 5000 = %d, want 5", n)
	}
}
