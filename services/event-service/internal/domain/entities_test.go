package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func ptrInt64(v int64) *int64 { return &v }

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

func eventWindow() (time.Time, time.Time) {
	start := time.Date(2030, 6, 1, 19, 0, 0, 0, time.UTC)
	return start, start.Add(3 * time.Hour)
}

func TestNewEventHappyPath(t *testing.T) {
	start, end := eventWindow()

	ev, err := NewEvent("Spring Festival", "outdoor stage", "Riverside Park", start, end)
	if err != nil {
		t.Fatalf("NewEvent returned error: %v", err)
	}
	assertV4UUID(t, ev.ID, "Event.ID")
	if ev.Name != "Spring Festival" || ev.Description != "outdoor stage" || ev.Venue != "Riverside Park" {
		t.Fatalf("event fields not copied: %+v", ev)
	}
	if !ev.StartsAt.Equal(start) || !ev.EndsAt.Equal(end) {
		t.Fatalf("event window not copied: %+v", ev)
	}
	if ev.CreatedAt.IsZero() {
		t.Fatalf("CreatedAt not set")
	}
}

func TestNewEventRejectsInvalidInput(t *testing.T) {
	start, end := eventWindow()

	tests := []struct {
		name      string
		eventName string
		venue     string
		startsAt  time.Time
		endsAt    time.Time
	}{
		{name: "blank name", eventName: "   ", venue: "Riverside Park", startsAt: start, endsAt: end},
		{name: "blank venue", eventName: "Spring Festival", venue: "", startsAt: start, endsAt: end},
		{name: "ends not after starts", eventName: "Spring Festival", venue: "Riverside Park", startsAt: start, endsAt: start},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev, err := NewEvent(tc.eventName, "desc", tc.venue, tc.startsAt, tc.endsAt)
			if !errors.Is(err, ErrInvalidEvent) {
				t.Fatalf("err = %v, want ErrInvalidEvent", err)
			}
			if ev != nil {
				t.Fatalf("event = %+v, want nil on error", ev)
			}
		})
	}
}

func TestNewSeatHappyPath(t *testing.T) {
	eventID := uuid.New()

	seat, err := NewSeat(eventID, "A", "12", "7", 4500)
	if err != nil {
		t.Fatalf("NewSeat returned error: %v", err)
	}
	assertV4UUID(t, seat.ID, "Seat.ID")
	if seat.EventID != eventID || seat.Section != "A" || seat.Row != "12" || seat.Number != "7" || seat.PriceMinor != 4500 {
		t.Fatalf("seat fields not copied: %+v", seat)
	}
	if seat.Status != SeatAvailable {
		t.Fatalf("Status = %q, want %q", seat.Status, SeatAvailable)
	}
}

func TestNewSeatRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		eventID uuid.UUID
		number  string
		price   int64
	}{
		{name: "nil event id", eventID: uuid.Nil, number: "7", price: 100},
		{name: "blank number", eventID: uuid.New(), number: "  ", price: 100},
		{name: "negative price", eventID: uuid.New(), number: "7", price: -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			seat, err := NewSeat(tc.eventID, "A", "1", tc.number, tc.price)
			if !errors.Is(err, ErrInvalidSeat) {
				t.Fatalf("err = %v, want ErrInvalidSeat", err)
			}
			if seat != nil {
				t.Fatalf("seat = %+v, want nil on error", seat)
			}
		})
	}
}

func TestSeatReserveThenRelease(t *testing.T) {
	seat := &Seat{Status: SeatAvailable}

	if err := seat.Reserve(); err != nil {
		t.Fatalf("Reserve returned error: %v", err)
	}
	if seat.Status != SeatReserved {
		t.Fatalf("Status = %q, want %q", seat.Status, SeatReserved)
	}
	if err := seat.Release(); err != nil {
		t.Fatalf("Release returned error: %v", err)
	}
	if seat.Status != SeatAvailable {
		t.Fatalf("Status = %q, want %q", seat.Status, SeatAvailable)
	}
}

func TestSeatReserveRejectsWhenNotAvailable(t *testing.T) {
	seat := &Seat{Status: SeatBooked}

	if err := seat.Reserve(); !errors.Is(err, ErrSeatUnavailable) {
		t.Fatalf("err = %v, want ErrSeatUnavailable", err)
	}
	if seat.Status != SeatBooked {
		t.Fatalf("Status = %q, want unchanged %q", seat.Status, SeatBooked)
	}
}

func TestSeatReleaseRejectsWhenNotReserved(t *testing.T) {
	seat := &Seat{Status: SeatAvailable}

	if err := seat.Release(); !errors.Is(err, ErrSeatUnavailable) {
		t.Fatalf("err = %v, want ErrSeatUnavailable", err)
	}
	if seat.Status != SeatAvailable {
		t.Fatalf("Status = %q, want unchanged %q", seat.Status, SeatAvailable)
	}
}

func TestSeatIsAvailable(t *testing.T) {
	if !(&Seat{Status: SeatAvailable}).IsAvailable() {
		t.Fatalf("IsAvailable() = false for an available seat")
	}
	if (&Seat{Status: SeatReserved}).IsAvailable() {
		t.Fatalf("IsAvailable() = true for a reserved seat")
	}
}

func TestSeatReservationFinalize(t *testing.T) {
	tests := []struct {
		name       string
		start      string
		wantStatus string
		wantErr    error
	}{
		{name: "held becomes finalized", start: ReservationHeld, wantStatus: ReservationFinalized},
		{name: "already finalized is a no-op", start: ReservationFinalized, wantStatus: ReservationFinalized},
		{name: "released cannot finalize", start: ReservationReleased, wantStatus: ReservationReleased, wantErr: ErrReservationNotHeld},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &SeatReservation{Status: tc.start}
			err := r.Finalize()
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("Finalize returned error: %v", err)
			}
			if r.Status != tc.wantStatus {
				t.Fatalf("Status = %q, want %q", r.Status, tc.wantStatus)
			}
		})
	}
}

func TestSeatReservationRelease(t *testing.T) {
	tests := []struct {
		name       string
		start      string
		wantStatus string
		wantErr    error
	}{
		{name: "held becomes released", start: ReservationHeld, wantStatus: ReservationReleased},
		{name: "already released is a no-op", start: ReservationReleased, wantStatus: ReservationReleased},
		{name: "finalized cannot release", start: ReservationFinalized, wantStatus: ReservationFinalized, wantErr: ErrReservationNotHeld},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &SeatReservation{Status: tc.start}
			err := r.Release()
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("Release returned error: %v", err)
			}
			if r.Status != tc.wantStatus {
				t.Fatalf("Status = %q, want %q", r.Status, tc.wantStatus)
			}
		})
	}
}

func TestNewEventWithSeatsHappyPath(t *testing.T) {
	start, end := eventWindow()
	layout := LayoutSpec{
		Sections: []SectionSpec{
			{Name: "Floor", Rows: 4, SeatsPerRow: 5, PriceMinor: 8000},
			{Name: "Balcony", Rows: 2, SeatsPerRow: 3, PriceMinor: 5000},
		},
		Exceptions: []SeatException{
			{Section: "Floor", Row: "1", Number: "1", Remove: true},
			{Section: "Floor", Row: "2", Number: "2", PriceMinor: ptrInt64(12000)},
		},
	}

	ev, seats, err := NewEventWithSeats("Gala", "desc", "Grand Hall", start, end, layout)
	if err != nil {
		t.Fatalf("NewEventWithSeats returned error: %v", err)
	}
	assertV4UUID(t, ev.ID, "Event.ID")
	if len(seats) != (20 + 6 - 1) {
		t.Fatalf("len(seats) = %d, want %d", len(seats), 20+6-1)
	}
	assertV4UUID(t, seats[0].ID, "Seat.ID")

	for _, s := range seats {
		if s.EventID != ev.ID {
			t.Fatalf("seat %+v not linked to event %v", s, ev.ID)
		}
		if s.Status != SeatAvailable {
			t.Fatalf("seat %+v Status = %q, want %q", s, s.Status, SeatAvailable)
		}
		switch {
		case s.Section == "Floor" && s.Row == "1" && s.Number == "1":
			t.Fatalf("removed seat Floor/1/1 is still present")
		case s.Section == "Floor" && s.Row == "2" && s.Number == "2":
			if s.PriceMinor != 12000 {
				t.Fatalf("price override not applied: Floor/2/2 PriceMinor = %d, want 12000", s.PriceMinor)
			}
		case s.Section == "Floor" && s.Row == "1" && s.Number == "2":
			if s.PriceMinor != 8000 {
				t.Fatalf("unadjusted seat PriceMinor = %d, want 8000", s.PriceMinor)
			}
		}
	}
}

func TestNewEventWithSeatsPropagatesConstructionErrors(t *testing.T) {
	start, end := eventWindow()
	goodSections := []SectionSpec{{Name: "Floor", Rows: 2, SeatsPerRow: 2, PriceMinor: 1000}}

	tests := []struct {
		name    string
		evName  string
		layout  LayoutSpec
		wantErr error
	}{
		{
			name:    "invalid event bubbles up from NewEvent",
			evName:  "   ",
			layout:  LayoutSpec{Sections: goodSections},
			wantErr: ErrInvalidEvent,
		},
		{
			name:    "no sections bubbles up from validateSections",
			evName:  "Gala",
			layout:  LayoutSpec{},
			wantErr: ErrEventRequiresSeats,
		},
		{
			name:   "bad exception bubbles up from indexExceptions",
			evName: "Gala",
			layout: LayoutSpec{
				Sections:   goodSections,
				Exceptions: []SeatException{{Section: "Nope", Row: "1", Number: "1", Remove: true}},
			},
			wantErr: ErrInvalidLayout,
		},
		{
			name:   "every seat removed leaves an empty layout",
			evName: "Gala",
			layout: LayoutSpec{
				Sections: []SectionSpec{{Name: "Floor", Rows: 1, SeatsPerRow: 2, PriceMinor: 1000}},
				Exceptions: []SeatException{
					{Section: "Floor", Row: "1", Number: "1", Remove: true},
					{Section: "Floor", Row: "1", Number: "2", Remove: true},
				},
			},
			wantErr: ErrEventRequiresSeats,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev, seats, err := NewEventWithSeats(tc.evName, "desc", "Grand Hall", start, end, tc.layout)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if ev != nil || seats != nil {
				t.Fatalf("ev=%v seats=%v, want nil on error", ev, seats)
			}
		})
	}
}

func TestValidateSectionsRejectsBadLayouts(t *testing.T) {
	tests := []struct {
		name    string
		specs   []SectionSpec
		wantErr error
	}{
		{
			name:    "malformed section spec",
			specs:   []SectionSpec{{Name: "A", Rows: 0, SeatsPerRow: 1, PriceMinor: 1}},
			wantErr: ErrInvalidLayout,
		},
		{
			name: "duplicate section name after trim",
			specs: []SectionSpec{
				{Name: "A", Rows: 1, SeatsPerRow: 1, PriceMinor: 1},
				{Name: " A ", Rows: 1, SeatsPerRow: 1, PriceMinor: 1},
			},
			wantErr: ErrInvalidLayout,
		},
		{
			name:    "exceeds per-event maximum",
			specs:   []SectionSpec{{Name: "A", Rows: MaxSeatsPerEvent + 1, SeatsPerRow: 1, PriceMinor: 1}},
			wantErr: ErrLayoutTooLarge,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, err := validateSections(tc.specs)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if m != nil {
				t.Fatalf("map = %v, want nil on error", m)
			}
		})
	}
}

func TestIndexExceptionsRejectsBadExceptions(t *testing.T) {
	sections, err := validateSections([]SectionSpec{{Name: "A", Rows: 4, SeatsPerRow: 4, PriceMinor: 100}})
	if err != nil {
		t.Fatalf("validateSections setup failed: %v", err)
	}

	tests := []struct {
		name string
		ex   SeatException
	}{
		{name: "seat address outside the grid", ex: SeatException{Section: "A", Row: "9", Number: "1", Remove: true}},
		{name: "neither removes nor reprices", ex: SeatException{Section: "A", Row: "1", Number: "1"}},
		{name: "negative price override", ex: SeatException{Section: "A", Row: "1", Number: "1", PriceMinor: ptrInt64(-1)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, err := indexExceptions([]SeatException{tc.ex}, sections)
			if !errors.Is(err, ErrInvalidLayout) {
				t.Fatalf("err = %v, want ErrInvalidLayout", err)
			}
			if m != nil {
				t.Fatalf("map = %v, want nil on error", m)
			}
		})
	}
}

func TestIndexExceptionsRejectsDuplicateSeatKey(t *testing.T) {
	sections, err := validateSections([]SectionSpec{{Name: "A", Rows: 4, SeatsPerRow: 4, PriceMinor: 100}})
	if err != nil {
		t.Fatalf("validateSections setup failed: %v", err)
	}
	exs := []SeatException{
		{Section: "A", Row: "2", Number: "2", Remove: true},
		{Section: "A", Row: "2", Number: "2", PriceMinor: ptrInt64(10)},
	}

	m, err := indexExceptions(exs, sections)
	if !errors.Is(err, ErrInvalidLayout) {
		t.Fatalf("err = %v, want ErrInvalidLayout", err)
	}
	if m != nil {
		t.Fatalf("map = %v, want nil on error", m)
	}
}

func TestInitSeatsRejectsNegativeAdjustedPrice(t *testing.T) {
	specs := []SectionSpec{{Name: "A", Rows: 1, SeatsPerRow: 1, PriceMinor: 100}}
	adj := map[string]seatAdjust{seatKey("A", "1", "1"): {price: ptrInt64(-5)}}

	seats, err := initSeats(uuid.New(), specs, adj)
	if !errors.Is(err, ErrInvalidSeat) {
		t.Fatalf("err = %v, want ErrInvalidSeat", err)
	}
	if seats != nil {
		t.Fatalf("seats = %v, want nil on error", seats)
	}
}
