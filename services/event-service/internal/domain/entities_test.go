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
			if ev.ID == uuid.Nil {
				t.Error("ID not generated")
			}
			if ev.Name != tc.eventName || ev.Description != tc.description || ev.Venue != tc.venue {
				t.Errorf("fields not copied through: %+v", ev)
			}
			if !ev.StartsAt.Equal(tc.starts) || !ev.EndsAt.Equal(tc.ends) {
				t.Errorf("times not copied through: %+v", ev)
			}
			if ev.CreatedAt.IsZero() {
				t.Error("CreatedAt not set")
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
			if s.ID == uuid.Nil {
				t.Error("ID not generated")
			}
			if s.EventID != tc.eventID || s.Section != tc.section || s.Row != tc.row || s.Number != tc.number || s.PriceMinor != tc.price {
				t.Errorf("fields not copied through: %+v", s)
			}
			if s.Status != SeatAvailable {
				t.Errorf("Status = %q, want %q", s.Status, SeatAvailable)
			}
		})
	}
}

func TestSeatReserve(t *testing.T) {
	tests := []struct {
		name       string
		start      string
		wantErr    error
		wantStatus string
	}{
		{name: "available -> reserved", start: SeatAvailable, wantStatus: SeatReserved},
		{name: "already reserved", start: SeatReserved, wantErr: ErrSeatUnavailable, wantStatus: SeatReserved},
		{name: "already booked", start: SeatBooked, wantErr: ErrSeatUnavailable, wantStatus: SeatBooked},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Seat{Status: tc.start}
			err := s.Reserve()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want errors.Is %v", err, tc.wantErr)
			}
			if s.Status != tc.wantStatus {
				t.Errorf("Status = %q, want %q", s.Status, tc.wantStatus)
			}
		})
	}
}

func TestSeatRelease(t *testing.T) {
	tests := []struct {
		name       string
		start      string
		wantErr    error
		wantStatus string
	}{
		{name: "reserved -> available", start: SeatReserved, wantStatus: SeatAvailable},
		{name: "available cannot release", start: SeatAvailable, wantErr: ErrSeatUnavailable, wantStatus: SeatAvailable},
		{name: "booked cannot release", start: SeatBooked, wantErr: ErrSeatUnavailable, wantStatus: SeatBooked},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Seat{Status: tc.start}
			err := s.Release()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want errors.Is %v", err, tc.wantErr)
			}
			if s.Status != tc.wantStatus {
				t.Errorf("Status = %q, want %q", s.Status, tc.wantStatus)
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

func TestSeatIsAvailable(t *testing.T) {
	tests := []struct {
		status string
		want   bool
	}{
		{SeatAvailable, true},
		{SeatReserved, false},
		{SeatBooked, false},
		{"", false},
	}
	for _, tc := range tests {
		t.Run(tc.status, func(t *testing.T) {
			s := &Seat{Status: tc.status}
			if got := s.IsAvailable(); got != tc.want {
				t.Errorf("IsAvailable() = %v, want %v", got, tc.want)
			}
		})
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

func TestValidateSectionsKeyedByTrimmedName(t *testing.T) {
	m, err := validateSections([]SectionSpec{{Name: "  A  ", Rows: 2, SeatsPerRow: 2, PriceMinor: 7}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, ok := m["A"]
	if !ok {
		t.Fatalf("map not keyed by trimmed name; keys = %v", keysOf(m))
	}
	if got.Name != "A" {
		t.Errorf("stored SectionSpec.Name = %q, want trimmed %q", got.Name, "A")
	}
	if _, ok := m["  A  "]; ok {
		t.Error("map still has the untrimmed key")
	}
}

func keysOf(m map[string]SectionSpec) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
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

func TestIndexExceptionsMapContents(t *testing.T) {
	sections := mustSections(t, SectionSpec{Name: "A", Rows: 5, SeatsPerRow: 5, PriceMinor: 100})

	exs := []SeatException{
		{Section: " A ", Row: "2", Number: "3", Remove: true},
		{Section: "A", Row: "1", Number: "1", PriceMinor: ptrI64(55)},
	}
	m, err := indexExceptions(exs, sections)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	removeAdj, ok := m[seatKey("A", "2", "3")]
	if !ok {
		t.Fatalf("no adjustment keyed by trimmed section for (A,2,3)")
	}
	if !removeAdj.remove || removeAdj.price != nil {
		t.Errorf("remove adjustment = %+v, want {remove:true price:nil}", removeAdj)
	}

	priceAdj, ok := m[seatKey("A", "1", "1")]
	if !ok {
		t.Fatalf("no adjustment for (A,1,1)")
	}
	if priceAdj.remove {
		t.Errorf("price adjustment should not remove: %+v", priceAdj)
	}
	if priceAdj.price == nil || *priceAdj.price != 55 {
		t.Errorf("price adjustment price = %v, want 55", priceAdj.price)
	}
}

func TestSeatKey(t *testing.T) {
	if seatKey("A", "1", "2") != seatKey("A", "1", "2") {
		t.Error("seatKey not deterministic for the same triple")
	}

	triples := [][3]string{
		{"A", "1", "2"},
		{"A", "12", ""},
		{"A1", "2", ""},
		{"A", "", "12"},
		{"", "A1", "2"},
		{"B", "1", "2"},
		{"A", "2", "1"},
		{"A", "1", "20"},
	}
	seen := make(map[string][3]string, len(triples))
	for _, tr := range triples {
		k := seatKey(tr[0], tr[1], tr[2])
		if prev, dup := seen[k]; dup {
			t.Errorf("seatKey collision: %v and %v both -> %q", prev, tr, k)
		}
		seen[k] = tr
	}
}

func TestExpandSeatsHappyPath(t *testing.T) {
	evID := uuid.New()
	specs := []SectionSpec{
		{Name: " A ", Rows: 2, SeatsPerRow: 3, PriceMinor: 100},
		{Name: "B", Rows: 1, SeatsPerRow: 2, PriceMinor: 250},
	}
	seats, err := expandSeats(evID, specs, map[string]seatAdjust{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(seats) != 2*3+1*2 {
		t.Fatalf("len(seats) = %d, want 8", len(seats))
	}

	ids := make(map[uuid.UUID]bool, len(seats))
	for _, s := range seats {
		if s.EventID != evID {
			t.Errorf("seat %+v: EventID = %v, want %v", s, s.EventID, evID)
		}
		if s.ID == uuid.Nil {
			t.Errorf("seat %+v: nil ID", s)
		}
		if ids[s.ID] {
			t.Errorf("duplicate seat ID %v", s.ID)
		}
		ids[s.ID] = true
		if s.Status != SeatAvailable {
			t.Errorf("seat %+v: Status = %q, want available", s, s.Status)
		}
		if s.Section != "A" && s.Section != "B" {
			t.Errorf("seat %+v: Section = %q, want trimmed A or B", s, s.Section)
		}
	}

	for i, s := range seats {
		if i < 6 && s.Section != "A" {
			t.Errorf("seat %d = %+v, want section A", i, s)
		}
		if i >= 6 && s.Section != "B" {
			t.Errorf("seat %d = %+v, want section B", i, s)
		}
	}

	for _, s := range seats {
		r, _ := strconv.Atoi(s.Row)
		n, _ := strconv.Atoi(s.Number)
		switch s.Section {
		case "A":
			if r < 1 || r > 2 || n < 1 || n > 3 {
				t.Errorf("section A seat out of range: %+v", s)
			}
			if s.PriceMinor != 100 {
				t.Errorf("section A seat price = %d, want 100", s.PriceMinor)
			}
		case "B":
			if r < 1 || r > 1 || n < 1 || n > 2 {
				t.Errorf("section B seat out of range: %+v", s)
			}
			if s.PriceMinor != 250 {
				t.Errorf("section B seat price = %d, want 250", s.PriceMinor)
			}
		}
	}
}

func TestExpandSeatsNumericOrdering(t *testing.T) {
	evID := uuid.New()
	specs := []SectionSpec{
		{Name: "A", Rows: 12, SeatsPerRow: 2, PriceMinor: 100},
		{Name: "B", Rows: 2, SeatsPerRow: 2, PriceMinor: 200},
	}
	seats, err := expandSeats(evID, specs, map[string]seatAdjust{})
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
	seats, err := expandSeats(evID, specs, adj)
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
	seats, err := expandSeats(evID, specs, adj)
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

func TestExpandSeatsRemoveWinsOverPrice(t *testing.T) {
	evID := uuid.New()
	specs := []SectionSpec{{Name: "A", Rows: 2, SeatsPerRow: 2, PriceMinor: 500}}
	adj := map[string]seatAdjust{
		seatKey("A", "1", "1"): {remove: true, price: ptrI64(999)},
	}
	seats, err := expandSeats(evID, specs, adj)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(seats) != 3 {
		t.Fatalf("len(seats) = %d, want 3 (seat removed, price ignored)", len(seats))
	}
	for _, s := range seats {
		if s.Row == "1" && s.Number == "1" {
			t.Errorf("seat (A,1,1) should have been removed, got %+v", s)
		}
	}
}

func TestExpandSeatsAdjustmentKeyedByTrimmedSection(t *testing.T) {
	evID := uuid.New()
	specs := []SectionSpec{{Name: "  A  ", Rows: 2, SeatsPerRow: 2, PriceMinor: 500}}
	adj := map[string]seatAdjust{
		seatKey("A", "1", "1"): {remove: true},
	}
	seats, err := expandSeats(evID, specs, adj)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(seats) != 3 {
		t.Fatalf("len(seats) = %d, want 3 (trimmed-section adjustment should apply)", len(seats))
	}
}

func TestExpandSeatsPropagatesNewSeatError(t *testing.T) {
	specs := []SectionSpec{{Name: "A", Rows: 1, SeatsPerRow: 1, PriceMinor: 1}}
	seats, err := expandSeats(uuid.Nil, specs, map[string]seatAdjust{})
	if !errors.Is(err, ErrInvalidSeat) {
		t.Fatalf("err = %v, want errors.Is %v", err, ErrInvalidSeat)
	}
	if seats != nil {
		t.Errorf("seats = %v, want nil on error", seats)
	}
}

func TestNewEventWithSeatsHappyPath(t *testing.T) {
	starts, ends := baseTimes()
	layout := LayoutSpec{
		Sections: []SectionSpec{
			{Name: " A ", Rows: 2, SeatsPerRow: 3, PriceMinor: 1000},
			{Name: "B", Rows: 1, SeatsPerRow: 2, PriceMinor: 2000},
		},
	}
	ev, seats, err := NewEventWithSeats("Concert", "desc", "Arena", starts, ends, layout)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev == nil || ev.ID == uuid.Nil {
		t.Fatalf("event not constructed: %+v", ev)
	}
	if ev.Name != "Concert" || ev.Venue != "Arena" {
		t.Errorf("event fields wrong: %+v", ev)
	}
	if len(seats) != 2*3+1*2 {
		t.Fatalf("len(seats) = %d, want 8", len(seats))
	}

	ids := make(map[uuid.UUID]bool, len(seats))
	for _, s := range seats {
		if s.EventID != ev.ID {
			t.Errorf("seat %+v: EventID != event.ID (%v)", s, ev.ID)
		}
		if s.ID == uuid.Nil || ids[s.ID] {
			t.Errorf("seat %+v: nil or duplicate ID", s)
		}
		ids[s.ID] = true
		if s.Status != SeatAvailable {
			t.Errorf("seat %+v: Status = %q", s, s.Status)
		}
		r, errR := strconv.Atoi(s.Row)
		n, errN := strconv.Atoi(s.Number)
		if errR != nil || errN != nil {
			t.Fatalf("seat %+v: non-numeric row/number", s)
		}
		switch s.Section {
		case "A":
			if r < 1 || r > 2 || n < 1 || n > 3 || s.PriceMinor != 1000 {
				t.Errorf("section A seat wrong: %+v", s)
			}
		case "B":
			if r < 1 || r > 1 || n < 1 || n > 2 || s.PriceMinor != 2000 {
				t.Errorf("section B seat wrong: %+v", s)
			}
		default:
			t.Errorf("unexpected section %q (want trimmed A/B)", s.Section)
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
			{Section: "B", Row: "3", Number: "3", PriceMinor: ptrI64(999)},
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
	var found bool
	for _, s := range seats {
		if s.Section == "B" && s.Row == "3" && s.Number == "3" {
			found = true
			if s.PriceMinor != 999 {
				t.Errorf("repriced seat price = %d, want 999", s.PriceMinor)
			}
		} else if s.Section == "B" && s.PriceMinor != 200 {
			t.Errorf("other section B seat %+v repriced unexpectedly", s)
		}
	}
	if !found {
		t.Error("repriced seat (B,3,3) missing from expansion")
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

func TestNewEventWithSeatsExactlyOneSeatSurvives(t *testing.T) {
	starts, ends := baseTimes()
	layout := LayoutSpec{
		Sections: []SectionSpec{{Name: "A", Rows: 1, SeatsPerRow: 2, PriceMinor: 100}},
		Exceptions: []SeatException{
			{Section: "A", Row: "1", Number: "1", Remove: true},
		},
	}
	_, seats, err := NewEventWithSeats("Concert", "d", "Arena", starts, ends, layout)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(seats) != 1 {
		t.Fatalf("len(seats) = %d, want 1", len(seats))
	}
	if seats[0].Row != "1" || seats[0].Number != "2" {
		t.Errorf("surviving seat = %+v, want (1,2)", seats[0])
	}
}
