package domain

import (
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Seat lifecycle. A seat starts SeatAvailable; the seat-reservation saga step
// (wired later via a /new-go-api-endpoint consume: step, when booking-service
// publishes BookingRequested) moves it to SeatReserved and then SeatBooked, or
// back to SeatAvailable on a compensation. The HTTP surface this skill scaffolds
// is read-only — it never transitions a seat.
const (
	SeatAvailable = "available"
	SeatReserved  = "reserved"
	SeatBooked    = "booked"
)

// Event is a happening people can book seats for — large or small, a stadium
// concert or a 20-seat workshop. The size difference is just the number of
// Seat rows that belong to it; there is no separate "large event" type.
type Event struct {
	ID          uuid.UUID
	Name        string
	Description string
	Venue       string
	StartsAt    time.Time
	EndsAt      time.Time
	CreatedAt   time.Time
}

// NewEvent constructs an Event, enforcing its invariants at the point of
// creation rather than leaving callers to remember to validate. The ID is
// generated here (UUID, never a sequential int — same type as a saga event's
// aggregate_id) so the record has an identity before its first insert.
func NewEvent(name, description, venue string, startsAt, endsAt time.Time) (*Event, error) {
	if strings.TrimSpace(name) == "" {
		return nil, ErrInvalidEvent
	}
	if strings.TrimSpace(venue) == "" {
		return nil, ErrInvalidEvent
	}
	if !endsAt.After(startsAt) {
		return nil, ErrInvalidEvent
	}
	return &Event{
		ID:          uuid.New(),
		Name:        name,
		Description: description,
		Venue:       venue,
		StartsAt:    startsAt,
		EndsAt:      endsAt,
		CreatedAt:   time.Now().UTC(),
	}, nil
}

// MaxSeatsPerEvent caps how many seats one create request may expand to, so a
// tiny layout body can't blow up into an unbounded number of rows. Comfortably
// above the largest real venues (~110k seats).
const MaxSeatsPerEvent = 200_000

// SectionSpec describes one rectangular block of seats to generate: rows
// numbered "1".."Rows", each with SeatsPerRow seats numbered "1".."SeatsPerRow",
// all at PriceMinor unless a SeatException reprices a specific seat.
type SectionSpec struct {
	Name        string
	Rows        int
	SeatsPerRow int
	PriceMinor  int64
}

// SeatException adjusts one generated seat, addressed by Section/Row/Number with
// the same string values the grid produces. It must do something: Remove the
// seat, or override its price (PriceMinor non-nil), or both. This is where the
// irregular parts of a real venue live (seats physically removed, one-off
// prices) without turning the whole layout into a hand-written list.
type SeatException struct {
	Section    string
	Row        string
	Number     string
	Remove     bool
	PriceMinor *int64
}

// LayoutSpec is the entire seat map as a compact description — a handful of
// rectangular sections plus per-seat exceptions — that the domain expands into
// individual Seat rows. A 100k-seat venue is a ~1KB request.
type LayoutSpec struct {
	Sections   []SectionSpec
	Exceptions []SeatException
}

// seatAdjust is the effect one SeatException has on the seat it addresses.
type seatAdjust struct {
	remove bool
	price  *int64
}

// seatKey joins a section/row/number into the string used to address one seat
// within a layout. The NUL separator can't occur in any part, so distinct
// triples never collide.
func seatKey(section, row, number string) string {
	return section + "\x00" + row + "\x00" + number
}

// NewEventWithSeats builds a new Event together with its seat map in one step,
// so the rule "an event is always created with its seat map" lives in the
// domain rather than in the usecase. It runs three steps, each its own function
// so the failure modes can be unit-tested in isolation: validate the sections
// (validateSections), index the exceptions against them (indexExceptions),
// expand the grid (expandSeats). Between them they enforce:
//   - every Event invariant (via NewEvent),
//   - a well-formed layout: >=1 section, unique section names, Rows and
//     SeatsPerRow >= 1, PriceMinor >= 0 (ErrInvalidLayout),
//   - the expanded map fits under MaxSeatsPerEvent (ErrLayoutTooLarge),
//   - every exception targets a seat the grid actually produces, no two
//     exceptions target the same seat, and each exception removes or reprices
//     (ErrInvalidLayout),
//   - at least one seat survives removals (ErrEventRequiresSeats).
//
// Uniqueness of section/row/number is guaranteed by construction here — the
// grid enumerates distinct triples and section names are checked unique — so
// there is no per-seat dedup pass and the DB unique constraint is only a
// backstop.
func NewEventWithSeats(name, description, venue string, startsAt, endsAt time.Time, layout LayoutSpec) (*Event, []Seat, error) {
	event, err := NewEvent(name, description, venue, startsAt, endsAt)
	if err != nil {
		return nil, nil, err
	}

	sections, err := validateSections(layout.Sections)
	if err != nil {
		return nil, nil, err
	}

	adjustments, err := indexExceptions(layout.Exceptions, sections)
	if err != nil {
		return nil, nil, err
	}

	seats, err := expandSeats(event.ID, layout.Sections, adjustments)
	if err != nil {
		return nil, nil, err
	}
	if len(seats) == 0 {
		return nil, nil, ErrEventRequiresSeats
	}
	return event, seats, nil
}

// validateSections checks each section, rejects a duplicate (trimmed) name, and
// enforces MaxSeatsPerEvent on the running seat total before anything is
// allocated. It returns the sections indexed by trimmed name for the exception
// step. An empty slice is ErrEventRequiresSeats.
func validateSections(specs []SectionSpec) (map[string]SectionSpec, error) {
	if len(specs) == 0 {
		return nil, ErrEventRequiresSeats
	}

	byName := make(map[string]SectionSpec, len(specs))
	total := 0
	for _, s := range specs {
		s.Name = strings.TrimSpace(s.Name)
		if s.Name == "" || s.Rows < 1 || s.SeatsPerRow < 1 || s.PriceMinor < 0 {
			return nil, ErrInvalidLayout
		}
		if _, dup := byName[s.Name]; dup {
			return nil, ErrInvalidLayout
		}
		byName[s.Name] = s

		total += s.Rows * s.SeatsPerRow
		if total > MaxSeatsPerEvent {
			return nil, ErrLayoutTooLarge
		}
	}
	return byName, nil
}

// indexExceptions validates every SeatException against the already-validated
// sections and returns them keyed by the seat they address. An exception that
// falls outside the grid, duplicates another, or would do nothing (neither
// removes nor reprices) is an ErrInvalidLayout.
func indexExceptions(exceptions []SeatException, sections map[string]SectionSpec) (map[string]seatAdjust, error) {
	byKey := make(map[string]seatAdjust, len(exceptions))
	for _, ex := range exceptions {
		sec, ok := sections[strings.TrimSpace(ex.Section)]
		if !ok {
			return nil, ErrInvalidLayout
		}
		row, err1 := strconv.Atoi(ex.Row)
		num, err2 := strconv.Atoi(ex.Number)
		if err1 != nil || err2 != nil || row < 1 || row > sec.Rows || num < 1 || num > sec.SeatsPerRow {
			return nil, ErrInvalidLayout
		}
		if !ex.Remove && ex.PriceMinor == nil {
			return nil, ErrInvalidLayout
		}
		if ex.PriceMinor != nil && *ex.PriceMinor < 0 {
			return nil, ErrInvalidLayout
		}
		key := seatKey(sec.Name, ex.Row, ex.Number)
		if _, dup := byKey[key]; dup {
			return nil, ErrInvalidLayout
		}
		byKey[key] = seatAdjust{remove: ex.Remove, price: ex.PriceMinor}
	}
	return byKey, nil
}

// expandSeats walks each section's grid in row/number order, applies any
// adjustment for a seat (skipping one marked remove), and builds the Seat
// entities against eventID.
func expandSeats(eventID uuid.UUID, specs []SectionSpec, adjustments map[string]seatAdjust) ([]Seat, error) {
	total := 0
	for _, s := range specs {
		total += s.Rows * s.SeatsPerRow
	}

	seats := make([]Seat, 0, total)
	for _, s := range specs {
		secName := strings.TrimSpace(s.Name)
		for r := 1; r <= s.Rows; r++ {
			rs := strconv.Itoa(r)
			for n := 1; n <= s.SeatsPerRow; n++ {
				ns := strconv.Itoa(n)

				price := s.PriceMinor
				if a, ok := adjustments[seatKey(secName, rs, ns)]; ok {
					if a.remove {
						continue
					}
					if a.price != nil {
						price = *a.price
					}
				}

				seat, err := NewSeat(eventID, secName, rs, ns, price)
				if err != nil {
					return nil, err
				}
				seats = append(seats, *seat)
			}
		}
	}
	return seats, nil
}

// Seat is one bookable position within an Event. The invariant that a seat can
// only be reserved when it is free lives on the entity itself (Reserve), not in
// the caller — see CLAUDE.md's domain-layer rule.
type Seat struct {
	ID         uuid.UUID
	EventID    uuid.UUID
	Section    string
	Row        string
	Number     string
	Status     string
	PriceMinor int64 // seat price in minor currency units (e.g. cents)
}

// NewSeat constructs an available Seat for an event. ID is generated here for
// the same reason NewEvent generates its own.
func NewSeat(eventID uuid.UUID, section, row, number string, priceMinor int64) (*Seat, error) {
	if eventID == uuid.Nil {
		return nil, ErrInvalidSeat
	}
	if strings.TrimSpace(number) == "" {
		return nil, ErrInvalidSeat
	}
	if priceMinor < 0 {
		return nil, ErrInvalidSeat
	}
	return &Seat{
		ID:         uuid.New(),
		EventID:    eventID,
		Section:    section,
		Row:        row,
		Number:     number,
		Status:     SeatAvailable,
		PriceMinor: priceMinor,
	}, nil
}

// Reserve moves a free seat to reserved. It returns ErrSeatUnavailable if the
// seat is already reserved or booked — the entity enforces the rule, not the
// caller. Kept here now (unused by the read-only HTTP surface) so the
// seat-reservation saga step added later has the invariant to call.
func (s *Seat) Reserve() error {
	if s.Status != SeatAvailable {
		return ErrSeatUnavailable
	}
	s.Status = SeatReserved
	return nil
}

// Release moves a reserved seat back to available — the compensating action for
// Reserve when a downstream saga step fails.
func (s *Seat) Release() error {
	if s.Status != SeatReserved {
		return ErrSeatUnavailable
	}
	s.Status = SeatAvailable
	return nil
}

// IsAvailable reports whether the seat can currently be reserved.
func (s *Seat) IsAvailable() bool { return s.Status == SeatAvailable }
