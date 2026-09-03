package domain

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
)

func ptrI64(v int64) *int64 { return &v }

func baseTimes() (time.Time, time.Time) {
	s := time.Date(2030, 1, 1, 10, 0, 0, 0, time.UTC)
	return s, s.Add(2 * time.Hour)
}

func mustSections(t *testing.T, specs ...SectionSpec) map[string]SectionSpec {
	t.Helper()
	m, err := validateSections(specs)
	if err != nil {
		t.Fatalf("validateSections(%v) unexpected error: %v", specs, err)
	}
	return m
}

func TestNewEvent(t *testing.T) {
	starts, ends := baseTimes()

	tests := []struct {
		name        string
		eventName   string
		description string
		venue       string
		starts      time.Time
		ends        time.Time
		wantErr     error
	}{
		{name: "valid", eventName: "Concert", description: "a show", venue: "Arena", starts: starts, ends: ends},
		{name: "valid empty description allowed", eventName: "Concert", description: "", venue: "Arena", starts: starts, ends: ends},
		{name: "blank name", eventName: "", description: "d", venue: "Arena", starts: starts, ends: ends, wantErr: ErrInvalidEvent},
		{name: "whitespace name", eventName: "   \t", description: "d", venue: "Arena", starts: starts, ends: ends, wantErr: ErrInvalidEvent},
		{name: "blank venue", eventName: "Concert", description: "d", venue: "", starts: starts, ends: ends, wantErr: ErrInvalidEvent},
		{name: "whitespace venue", eventName: "Concert", description: "d", venue: "  ", starts: starts, ends: ends, wantErr: ErrInvalidEvent},
		{name: "ends equal to starts", eventName: "Concert", description: "d", venue: "Arena", starts: starts, ends: starts, wantErr: ErrInvalidEvent},
		{name: "ends before starts", eventName: "Concert", description: "d", venue: "Arena", starts: starts, ends: starts.Add(-time.Second), wantErr: ErrInvalidEvent},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev, err := NewEvent(tc.eventName, tc.description, tc.venue, tc.starts, tc.ends)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want errors.Is %v", err, tc.wantErr)
				}
				if ev != nil {
					t.Fatalf("event = %+v, want nil on error", ev)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestNewSeat(t *testing.T) {
	evID := uuid.New()

	tests := []struct {
		name    string
		eventID uuid.UUID
		section string
		row     string
		number  string
		price   int64
		wantErr error
	}{
		{name: "valid", eventID: evID, section: "A", row: "1", number: "1", price: 1000},
		{name: "valid zero price", eventID: evID, section: "A", row: "1", number: "1", price: 0},
		{name: "valid blank section and row (only number checked)", eventID: evID, section: "", row: "", number: "7", price: 5},
		{name: "nil event id", eventID: uuid.Nil, section: "A", row: "1", number: "1", price: 10, wantErr: ErrInvalidSeat},
		{name: "blank number", eventID: evID, section: "A", row: "1", number: "", price: 10, wantErr: ErrInvalidSeat},
		{name: "whitespace number", eventID: evID, section: "A", row: "1", number: "  ", price: 10, wantErr: ErrInvalidSeat},
		{name: "negative price", eventID: evID, section: "A", row: "1", number: "1", price: -1, wantErr: ErrInvalidSeat},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewSeat(tc.eventID, tc.section, tc.row, tc.number, tc.price)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want errors.Is %v", err, tc.wantErr)
				}
				if s != nil {
					t.Fatalf("seat = %+v, want nil on error", s)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestSeatReserveReleaseRoundTrip(t *testing.T) {
	s := &Seat{Status: SeatAvailable}
	if err := s.Reserve(); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if s.Status != SeatReserved {
		t.Fatalf("Status = %q, want reserved", s.Status)
	}
	if err := s.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if s.Status != SeatAvailable {
		t.Fatalf("Status = %q, want available", s.Status)
	}
}

func TestValidateSections(t *testing.T) {
	tests := []struct {
		name    string
		specs   []SectionSpec
		wantErr error
	}{
		{name: "nil slice", specs: nil, wantErr: ErrEventRequiresSeats},
		{name: "empty slice", specs: []SectionSpec{}, wantErr: ErrEventRequiresSeats},
		{name: "single valid", specs: []SectionSpec{{Name: "A", Rows: 2, SeatsPerRow: 3, PriceMinor: 100}}},
		{name: "zero price allowed", specs: []SectionSpec{{Name: "A", Rows: 1, SeatsPerRow: 1, PriceMinor: 0}}},
		{name: "blank name", specs: []SectionSpec{{Name: "", Rows: 1, SeatsPerRow: 1}}, wantErr: ErrInvalidLayout},
		{name: "whitespace-only name", specs: []SectionSpec{{Name: "  \t", Rows: 1, SeatsPerRow: 1}}, wantErr: ErrInvalidLayout},
		{name: "rows zero", specs: []SectionSpec{{Name: "A", Rows: 0, SeatsPerRow: 1}}, wantErr: ErrInvalidLayout},
		{name: "rows negative", specs: []SectionSpec{{Name: "A", Rows: -1, SeatsPerRow: 1}}, wantErr: ErrInvalidLayout},
		{name: "seats per row zero", specs: []SectionSpec{{Name: "A", Rows: 1, SeatsPerRow: 0}}, wantErr: ErrInvalidLayout},
		{name: "seats per row negative", specs: []SectionSpec{{Name: "A", Rows: 1, SeatsPerRow: -1}}, wantErr: ErrInvalidLayout},
		{name: "negative price", specs: []SectionSpec{{Name: "A", Rows: 1, SeatsPerRow: 1, PriceMinor: -1}}, wantErr: ErrInvalidLayout},
		{
			name: "duplicate names",
			specs: []SectionSpec{
				{Name: "A", Rows: 1, SeatsPerRow: 1},
				{Name: "A", Rows: 1, SeatsPerRow: 1},
			},
			wantErr: ErrInvalidLayout,
		},
		{
			name: "duplicate names only after trim",
			specs: []SectionSpec{
				{Name: "A", Rows: 1, SeatsPerRow: 1},
				{Name: " A ", Rows: 1, SeatsPerRow: 1},
			},
			wantErr: ErrInvalidLayout,
		},
		{
			name:    "exactly at the cap succeeds",
			specs:   []SectionSpec{{Name: "A", Rows: MaxSeatsPerEvent, SeatsPerRow: 1, PriceMinor: 1}},
			wantErr: nil,
		},
		{
			name:    "one past the cap in a single section",
			specs:   []SectionSpec{{Name: "A", Rows: MaxSeatsPerEvent + 1, SeatsPerRow: 1, PriceMinor: 1}},
			wantErr: ErrLayoutTooLarge,
		},
		{
			name: "running total crosses the cap",
			specs: []SectionSpec{
				{Name: "A", Rows: 150_000, SeatsPerRow: 1, PriceMinor: 1},
				{Name: "B", Rows: 50_001, SeatsPerRow: 1, PriceMinor: 1},
			},
			wantErr: ErrLayoutTooLarge,
		},
		{
			name: "running total exactly at the cap across sections",
			specs: []SectionSpec{
				{Name: "A", Rows: 150_000, SeatsPerRow: 1, PriceMinor: 1},
				{Name: "B", Rows: 50_000, SeatsPerRow: 1, PriceMinor: 1},
			},
			wantErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, err := validateSections(tc.specs)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want errors.Is %v", err, tc.wantErr)
				}
				if m != nil {
					t.Fatalf("map = %v, want nil on error", m)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(m) != len(tc.specs) {
				t.Fatalf("map has %d entries, want %d", len(m), len(tc.specs))
			}
		})
	}
}

func TestIndexExceptions(t *testing.T) {
	sections := mustSections(t,
		SectionSpec{Name: "A", Rows: 5, SeatsPerRow: 5, PriceMinor: 100},
		SectionSpec{Name: " B ", Rows: 3, SeatsPerRow: 3, PriceMinor: 200},
	)

	tests := []struct {
		name    string
		exs     []SeatException
		wantErr error
	}{
		{name: "no exceptions", exs: nil},
		{name: "valid remove", exs: []SeatException{{Section: "A", Row: "1", Number: "1", Remove: true}}},
		{name: "valid price override", exs: []SeatException{{Section: "A", Row: "1", Number: "1", PriceMinor: ptrI64(0)}}},
		{name: "valid remove and override together", exs: []SeatException{{Section: "A", Row: "1", Number: "1", Remove: true, PriceMinor: ptrI64(50)}}},
		{name: "untrimmed section still matches", exs: []SeatException{{Section: " A ", Row: "2", Number: "3", Remove: true}}},
		{name: "target section B", exs: []SeatException{{Section: "B", Row: "3", Number: "3", Remove: true}}},

		{name: "section not present", exs: []SeatException{{Section: "Z", Row: "1", Number: "1", Remove: true}}, wantErr: ErrInvalidLayout},
		{name: "row not parseable", exs: []SeatException{{Section: "A", Row: "abc", Number: "1", Remove: true}}, wantErr: ErrInvalidLayout},
		{name: "row empty string", exs: []SeatException{{Section: "A", Row: "", Number: "1", Remove: true}}, wantErr: ErrInvalidLayout},
		{name: "number not parseable", exs: []SeatException{{Section: "A", Row: "1", Number: "abc", Remove: true}}, wantErr: ErrInvalidLayout},
		{name: "number empty string", exs: []SeatException{{Section: "A", Row: "1", Number: "", Remove: true}}, wantErr: ErrInvalidLayout},
		{name: "row zero", exs: []SeatException{{Section: "A", Row: "0", Number: "1", Remove: true}}, wantErr: ErrInvalidLayout},
		{name: "row one past max", exs: []SeatException{{Section: "A", Row: "6", Number: "1", Remove: true}}, wantErr: ErrInvalidLayout},
		{name: "number zero", exs: []SeatException{{Section: "A", Row: "1", Number: "0", Remove: true}}, wantErr: ErrInvalidLayout},
		{name: "number one past max", exs: []SeatException{{Section: "A", Row: "1", Number: "6", Remove: true}}, wantErr: ErrInvalidLayout},
		{name: "does nothing (no remove, nil price)", exs: []SeatException{{Section: "A", Row: "1", Number: "1"}}, wantErr: ErrInvalidLayout},
		{name: "negative price override", exs: []SeatException{{Section: "A", Row: "1", Number: "1", PriceMinor: ptrI64(-1)}}, wantErr: ErrInvalidLayout},
		{
			name: "two exceptions on the same seat",
			exs: []SeatException{
				{Section: "A", Row: "2", Number: "2", Remove: true},
				{Section: "A", Row: "2", Number: "2", PriceMinor: ptrI64(10)},
			},
			wantErr: ErrInvalidLayout,
		},
		{
			name: "same seat addressed via trimmed and untrimmed section still collides",
			exs: []SeatException{
				{Section: "A", Row: "2", Number: "2", Remove: true},
				{Section: " A ", Row: "2", Number: "2", Remove: true},
			},
			wantErr: ErrInvalidLayout,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, err := indexExceptions(tc.exs, sections)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want errors.Is %v", err, tc.wantErr)
				}
				if m != nil {
					t.Fatalf("map = %v, want nil on error", m)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestExpandSeatsNumericOrdering(t *testing.T) {
	evID := uuid.New()
	specs := []SectionSpec{
		{Name: "A", Rows: 12, SeatsPerRow: 2, PriceMinor: 100},
		{Name: "B", Rows: 2, SeatsPerRow: 2, PriceMinor: 200},
	}
	seats, err := initSeats(evID, specs, map[string]seatAdjust{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var prevSection string
	var prevRow, prevNum int
	firstRow10 := -1
	lastRow2 := -1
	for i, s := range seats {
		r, _ := strconv.Atoi(s.Row)
		n, _ := strconv.Atoi(s.Number)
		if s.Section != prevSection {
			prevSection, prevRow, prevNum = s.Section, 0, 0
		}
		if r < prevRow || (r == prevRow && n <= prevNum) {
			t.Fatalf("seat %d not in ascending numeric order: prev (%d,%d) then (%d,%d)", i, prevRow, prevNum, r, n)
		}
		prevRow, prevNum = r, n
		if s.Section == "A" && s.Row == "10" && firstRow10 == -1 {
			firstRow10 = i
		}
		if s.Section == "A" && s.Row == "2" {
			lastRow2 = i
		}
	}
	if firstRow10 == -1 || lastRow2 == -1 {
		t.Fatalf("did not find both row 2 and row 10 seats (firstRow10=%d lastRow2=%d)", firstRow10, lastRow2)
	}
	if firstRow10 < lastRow2 {
		t.Errorf("row 10 (idx %d) came before row 2 (idx %d): ordering is lexical, want numeric", firstRow10, lastRow2)
	}

	sawB := false
	for _, s := range seats {
		if s.Section == "B" {
			sawB = true
		} else if sawB {
			t.Errorf("section A seat %+v appeared after a section B seat", s)
		}
	}
}

func TestExpandSeatsRemoveException(t *testing.T) {
	evID := uuid.New()
	specs := []SectionSpec{{Name: "A", Rows: 3, SeatsPerRow: 3, PriceMinor: 100}}
	adj := map[string]seatAdjust{
		seatKey("A", "2", "2"): {remove: true},
	}
	seats, err := initSeats(evID, specs, adj)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(seats) != 8 {
		t.Fatalf("len(seats) = %d, want 8 (9 minus one removed)", len(seats))
	}
	for _, s := range seats {
		if s.Row == "2" && s.Number == "2" {
			t.Errorf("removed seat (A,2,2) still present")
		}
	}
	want := make(map[string]bool, 8)
	for r := 1; r <= 3; r++ {
		for n := 1; n <= 3; n++ {
			if r == 2 && n == 2 {
				continue
			}
			want[seatKey("A", strconv.Itoa(r), strconv.Itoa(n))] = true
		}
	}
	for _, s := range seats {
		delete(want, seatKey(s.Section, s.Row, s.Number))
	}
	if len(want) != 0 {
		t.Errorf("missing expected seats: %v", want)
	}
}

func TestExpandSeatsPriceOverrideException(t *testing.T) {
	evID := uuid.New()
	specs := []SectionSpec{{Name: "A", Rows: 2, SeatsPerRow: 2, PriceMinor: 500}}
	adj := map[string]seatAdjust{
		seatKey("A", "1", "2"): {price: ptrI64(999)},
	}
	seats, err := initSeats(evID, specs, adj)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(seats) != 4 {
		t.Fatalf("len(seats) = %d, want 4", len(seats))
	}
	for _, s := range seats {
		want := int64(500)
		if s.Row == "1" && s.Number == "2" {
			want = 999
		}
		if s.PriceMinor != want {
			t.Errorf("seat (%s,%s) price = %d, want %d", s.Row, s.Number, s.PriceMinor, want)
		}
	}
}

func TestNewEventWithSeatsCountMinusRemovals(t *testing.T) {
	starts, ends := baseTimes()
	layout := LayoutSpec{
		Sections: []SectionSpec{
			{Name: "A", Rows: 4, SeatsPerRow: 5, PriceMinor: 100},
			{Name: "B", Rows: 3, SeatsPerRow: 3, PriceMinor: 200},
		},
		Exceptions: []SeatException{
			{Section: "A", Row: "1", Number: "1", Remove: true},
			{Section: "A", Row: "4", Number: "5", Remove: true},
			{Section: "B", Row: "2", Number: "2", Remove: true},
		},
	}
	_, seats, err := NewEventWithSeats("Concert", "d", "Arena", starts, ends, layout)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := (20 + 9) - 3
	if len(seats) != want {
		t.Fatalf("len(seats) = %d, want %d (total minus 3 removals)", len(seats), want)
	}
}

func TestNewEventWithSeatsRemoveAllSeats(t *testing.T) {
	starts, ends := baseTimes()
	layout := LayoutSpec{
		Sections: []SectionSpec{{Name: "A", Rows: 1, SeatsPerRow: 2, PriceMinor: 100}},
		Exceptions: []SeatException{
			{Section: "A", Row: "1", Number: "1", Remove: true},
			{Section: "A", Row: "1", Number: "2", Remove: true},
		},
	}
	ev, seats, err := NewEventWithSeats("Concert", "d", "Arena", starts, ends, layout)
	if !errors.Is(err, ErrEventRequiresSeats) {
		t.Fatalf("err = %v, want errors.Is %v", err, ErrEventRequiresSeats)
	}
	if ev != nil || seats != nil {
		t.Errorf("event/seats = %+v/%v, want nil on error", ev, seats)
	}
}

func TestNewEventWithSeatsErrorPropagation(t *testing.T) {
	starts, ends := baseTimes()
	goodSections := []SectionSpec{{Name: "A", Rows: 2, SeatsPerRow: 2, PriceMinor: 100}}

	tests := []struct {
		name    string
		evName  string
		venue   string
		starts  time.Time
		ends    time.Time
		layout  LayoutSpec
		wantErr error
	}{
		{
			name:   "blank name -> ErrInvalidEvent",
			evName: "", venue: "Arena", starts: starts, ends: ends,
			layout: LayoutSpec{Sections: goodSections}, wantErr: ErrInvalidEvent,
		},
		{
			name:   "whitespace venue -> ErrInvalidEvent",
			evName: "Concert", venue: "  ", starts: starts, ends: ends,
			layout: LayoutSpec{Sections: goodSections}, wantErr: ErrInvalidEvent,
		},
		{
			name:   "ends equals starts -> ErrInvalidEvent",
			evName: "Concert", venue: "Arena", starts: starts, ends: starts,
			layout: LayoutSpec{Sections: goodSections}, wantErr: ErrInvalidEvent,
		},
		{
			name:   "ends before starts -> ErrInvalidEvent",
			evName: "Concert", venue: "Arena", starts: starts, ends: starts.Add(-time.Hour),
			layout: LayoutSpec{Sections: goodSections}, wantErr: ErrInvalidEvent,
		},
		{
			name:   "no sections -> ErrEventRequiresSeats",
			evName: "Concert", venue: "Arena", starts: starts, ends: ends,
			layout: LayoutSpec{}, wantErr: ErrEventRequiresSeats,
		},
		{
			name:   "bad section -> ErrInvalidLayout",
			evName: "Concert", venue: "Arena", starts: starts, ends: ends,
			layout:  LayoutSpec{Sections: []SectionSpec{{Name: "A", Rows: 0, SeatsPerRow: 2}}},
			wantErr: ErrInvalidLayout,
		},
		{
			name:   "duplicate section names after trim -> ErrInvalidLayout",
			evName: "Concert", venue: "Arena", starts: starts, ends: ends,
			layout: LayoutSpec{Sections: []SectionSpec{
				{Name: "A", Rows: 1, SeatsPerRow: 1, PriceMinor: 1},
				{Name: " A ", Rows: 1, SeatsPerRow: 1, PriceMinor: 1},
			}},
			wantErr: ErrInvalidLayout,
		},
		{
			name:   "layout too large -> ErrLayoutTooLarge",
			evName: "Concert", venue: "Arena", starts: starts, ends: ends,
			layout:  LayoutSpec{Sections: []SectionSpec{{Name: "A", Rows: MaxSeatsPerEvent + 1, SeatsPerRow: 1, PriceMinor: 1}}},
			wantErr: ErrLayoutTooLarge,
		},
		{
			name:   "exception outside grid -> ErrInvalidLayout",
			evName: "Concert", venue: "Arena", starts: starts, ends: ends,
			layout: LayoutSpec{
				Sections:   goodSections,
				Exceptions: []SeatException{{Section: "A", Row: "3", Number: "1", Remove: true}},
			},
			wantErr: ErrInvalidLayout,
		},
		{
			name:   "exception that does nothing -> ErrInvalidLayout",
			evName: "Concert", venue: "Arena", starts: starts, ends: ends,
			layout: LayoutSpec{
				Sections:   goodSections,
				Exceptions: []SeatException{{Section: "A", Row: "1", Number: "1"}},
			},
			wantErr: ErrInvalidLayout,
		},
		{
			name:   "duplicate exception -> ErrInvalidLayout",
			evName: "Concert", venue: "Arena", starts: starts, ends: ends,
			layout: LayoutSpec{
				Sections: goodSections,
				Exceptions: []SeatException{
					{Section: "A", Row: "1", Number: "1", Remove: true},
					{Section: "A", Row: "1", Number: "1", PriceMinor: ptrI64(5)},
				},
			},
			wantErr: ErrInvalidLayout,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev, seats, err := NewEventWithSeats(tc.evName, "desc", tc.venue, tc.starts, tc.ends, tc.layout)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want errors.Is %v", err, tc.wantErr)
			}
			if ev != nil || seats != nil {
				t.Errorf("event/seats = %+v/%v, want nil on error", ev, seats)
			}
		})
	}
}
