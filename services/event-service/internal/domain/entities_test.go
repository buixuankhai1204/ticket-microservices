package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
)

func price(v int64) *int64 { return &v }

func window() (time.Time, time.Time) {
	start := time.Date(2030, 6, 1, 19, 0, 0, 0, time.UTC)
	return start, start.Add(3 * time.Hour)
}

func TestAnEventNeedsANameAVenueAndAPositiveDuration(t *testing.T) {
	start, end := window()
	tests := []struct {
		name    string
		evName  string
		venue   string
		start   time.Time
		end     time.Time
		wantErr error
	}{
		{"valid", "Gala", "Grand Hall", start, end, nil},
		{"blank name", "   ", "Grand Hall", start, end, domain.ErrInvalidEvent},
		{"blank venue", "Gala", "", start, end, domain.ErrInvalidEvent},
		{"ends before it starts", "Gala", "Grand Hall", end, start, domain.ErrInvalidEvent},
		{"ends the moment it starts", "Gala", "Grand Hall", start, start, domain.ErrInvalidEvent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			event, err := domain.NewEvent(tc.evName, "", tc.venue, tc.start, tc.end)

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil && (event.ID == uuid.Nil || event.ID.Version() != 4) {
				t.Fatalf("ID = %s, want a generated v4 UUID", event.ID)
			}
			if tc.wantErr != nil && event != nil {
				t.Fatalf("event = %+v, want nil on error", event)
			}
		})
	}
}

func TestALayoutExpandsIntoOneSeatPerPositionMinusRemovalsWithPriceOverrides(t *testing.T) {
	start, end := window()
	layout := domain.LayoutSpec{
		Sections: []domain.SectionSpec{
			{Name: "Floor", Rows: 4, SeatsPerRow: 5, PriceMinor: 8000},
			{Name: "Balcony", Rows: 2, SeatsPerRow: 3, PriceMinor: 5000},
		},
		Exceptions: []domain.SeatException{
			{Section: "Floor", Row: "1", Number: "1", Remove: true},
			{Section: "Floor", Row: "2", Number: "2", PriceMinor: price(12000)},
		},
	}

	event, seats, err := domain.NewEventWithSeats("Gala", "", "Grand Hall", start, end, layout)

	if err != nil {
		t.Fatalf("NewEventWithSeats: %v", err)
	}
	if want := 20 + 6 - 1; len(seats) != want {
		t.Fatalf("%d seats, want %d", len(seats), want)
	}
	prices := map[string]int64{}
	ids := map[uuid.UUID]bool{}
	for _, s := range seats {
		if s.EventID != event.ID || s.Status != domain.SeatAvailable {
			t.Fatalf("seat %+v must belong to the event and start available", s)
		}
		if ids[s.ID] {
			t.Fatalf("seat id %s is used twice", s.ID)
		}
		ids[s.ID] = true
		prices[s.Section+"/"+s.Row+"/"+s.Number] = s.PriceMinor
	}
	if _, present := prices["Floor/1/1"]; present {
		t.Fatalf("removed seat Floor/1/1 is still on the map")
	}
	if prices["Floor/2/2"] != 12000 || prices["Floor/1/2"] != 8000 || prices["Balcony/2/3"] != 5000 {
		t.Fatalf("prices = %v: override must apply to one seat only and sections keep their own price", prices)
	}
}

func TestLayoutsThatCannotBeBuiltAreRejectedWithTheReasonTheClientNeeds(t *testing.T) {
	start, end := window()
	a := domain.SectionSpec{Name: "A", Rows: 4, SeatsPerRow: 4, PriceMinor: 100}
	tests := []struct {
		name    string
		layout  domain.LayoutSpec
		wantErr error
	}{
		{"no sections", domain.LayoutSpec{}, domain.ErrEventRequiresSeats},
		{"every seat removed", domain.LayoutSpec{
			Sections:   []domain.SectionSpec{{Name: "A", Rows: 1, SeatsPerRow: 1, PriceMinor: 1}},
			Exceptions: []domain.SeatException{{Section: "A", Row: "1", Number: "1", Remove: true}},
		}, domain.ErrEventRequiresSeats},
		{"zero rows", domain.LayoutSpec{Sections: []domain.SectionSpec{{Name: "A", Rows: 0, SeatsPerRow: 1}}}, domain.ErrInvalidLayout},
		{"negative price", domain.LayoutSpec{Sections: []domain.SectionSpec{{Name: "A", Rows: 1, SeatsPerRow: 1, PriceMinor: -1}}}, domain.ErrInvalidLayout},
		{"duplicate section once trimmed", domain.LayoutSpec{Sections: []domain.SectionSpec{
			{Name: "A", Rows: 1, SeatsPerRow: 1}, {Name: " A ", Rows: 1, SeatsPerRow: 1},
		}}, domain.ErrInvalidLayout},
		{"exception for a section that does not exist", domain.LayoutSpec{
			Sections: []domain.SectionSpec{a}, Exceptions: []domain.SeatException{{Section: "B", Row: "1", Number: "1", Remove: true}},
		}, domain.ErrInvalidLayout},
		{"exception outside the grid", domain.LayoutSpec{
			Sections: []domain.SectionSpec{a}, Exceptions: []domain.SeatException{{Section: "A", Row: "9", Number: "1", Remove: true}},
		}, domain.ErrInvalidLayout},
		{"exception that neither removes nor reprices", domain.LayoutSpec{
			Sections: []domain.SectionSpec{a}, Exceptions: []domain.SeatException{{Section: "A", Row: "1", Number: "1"}},
		}, domain.ErrInvalidLayout},
		{"exception with a negative price", domain.LayoutSpec{
			Sections: []domain.SectionSpec{a}, Exceptions: []domain.SeatException{{Section: "A", Row: "1", Number: "1", PriceMinor: price(-1)}},
		}, domain.ErrInvalidLayout},
		{"two exceptions for one seat", domain.LayoutSpec{
			Sections: []domain.SectionSpec{a}, Exceptions: []domain.SeatException{
				{Section: "A", Row: "1", Number: "1", Remove: true}, {Section: "A", Row: "1", Number: "1", PriceMinor: price(5)},
			},
		}, domain.ErrInvalidLayout},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			event, seats, err := domain.NewEventWithSeats("Gala", "", "Hall", start, end, tc.layout)

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if event != nil || seats != nil {
				t.Fatalf("a rejected layout must produce nothing, got %+v / %d seats", event, len(seats))
			}
		})
	}
}

func TestTheSeatCapIsEnforcedEvenWhenRowsTimesSeatsOverflowAnInteger(t *testing.T) {
	start, end := window()
	tests := []struct {
		name     string
		sections []domain.SectionSpec
	}{
		{"one section over the cap", []domain.SectionSpec{{Name: "A", Rows: domain.MaxSeatsPerEvent + 1, SeatsPerRow: 1}}},
		{"two sections that only exceed it together", []domain.SectionSpec{
			{Name: "A", Rows: domain.MaxSeatsPerEvent, SeatsPerRow: 1}, {Name: "B", Rows: 1, SeatsPerRow: 1},
		}},
		{"product that wraps around to zero", []domain.SectionSpec{{Name: "A", Rows: 1 << 32, SeatsPerRow: 1 << 32}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := domain.NewEventWithSeats("Gala", "", "Hall", start, end, domain.LayoutSpec{Sections: tc.sections})

			if !errors.Is(err, domain.ErrLayoutTooLarge) {
				t.Fatalf("err = %v, want ErrLayoutTooLarge", err)
			}
		})
	}
}

func TestASeatCanOnlyBeReservedFromAvailableAndReleasedFromReserved(t *testing.T) {
	steps := []struct {
		name       string
		act        func(*domain.Seat) error
		wantErr    error
		wantStatus string
	}{
		{"release before anything was reserved", (*domain.Seat).Release, domain.ErrSeatUnavailable, domain.SeatAvailable},
		{"reserve", (*domain.Seat).Reserve, nil, domain.SeatReserved},
		{"reserve twice", (*domain.Seat).Reserve, domain.ErrSeatUnavailable, domain.SeatReserved},
		{"release", (*domain.Seat).Release, nil, domain.SeatAvailable},
	}
	seat, err := domain.NewSeat(uuid.New(), "A", "1", "1", 100)
	if err != nil {
		t.Fatal(err)
	}

	for _, step := range steps {
		if err := step.act(seat); !errors.Is(err, step.wantErr) || seat.Status != step.wantStatus {
			t.Fatalf("%s: got (%v, %s), want (%v, %s)", step.name, err, seat.Status, step.wantErr, step.wantStatus)
		}
	}

	seat.Status = domain.SeatBooked
	if err := seat.Release(); !errors.Is(err, domain.ErrSeatUnavailable) || seat.Status != domain.SeatBooked {
		t.Fatalf("a booked seat must never go back on sale: got (%v, %s)", err, seat.Status)
	}
}

func TestAReservationMovesForwardOnceAndRepeatsAreHarmless(t *testing.T) {
	tests := []struct {
		name       string
		start      string
		act        func(*domain.SeatReservation) error
		wantStatus string
		wantErr    error
	}{
		{"finalize a held reservation", domain.ReservationHeld, (*domain.SeatReservation).Finalize, domain.ReservationFinalized, nil},
		{"finalize twice", domain.ReservationFinalized, (*domain.SeatReservation).Finalize, domain.ReservationFinalized, nil},
		{"finalize after release", domain.ReservationReleased, (*domain.SeatReservation).Finalize, domain.ReservationReleased, domain.ErrReservationNotHeld},
		{"release a held reservation", domain.ReservationHeld, (*domain.SeatReservation).Release, domain.ReservationReleased, nil},
		{"release twice", domain.ReservationReleased, (*domain.SeatReservation).Release, domain.ReservationReleased, nil},
		{"release after finalize", domain.ReservationFinalized, (*domain.SeatReservation).Release, domain.ReservationFinalized, domain.ErrReservationNotHeld},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &domain.SeatReservation{Status: tc.start}

			err := tc.act(r)

			if !errors.Is(err, tc.wantErr) || r.Status != tc.wantStatus {
				t.Fatalf("got (%v, %s), want (%v, %s)", err, r.Status, tc.wantErr, tc.wantStatus)
			}
		})
	}
}
