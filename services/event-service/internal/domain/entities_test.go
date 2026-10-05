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
		{
			name:    "rows times seats per row overflows int to zero",
			specs:   []SectionSpec{{Name: "A", Rows: 1 << 32, SeatsPerRow: 1 << 32, PriceMinor: 1}},
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
