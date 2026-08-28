package domain

import (
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Seat lifecycle. A seat starts SeatAvailable; the seat-reservation saga step
// (wired later with /add-go-saga-step, when booking-service publishes
// BookingRequested) moves it to SeatReserved and then SeatBooked, or back to
// SeatAvailable on a compensation. The HTTP surface this skill scaffolds is
// read-only — it never transitions a seat.
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

// NewEventWithSeats builds a new Event together with its seat map in one step,
// so the rule "an event is always created with its seat map" lives in the
// domain rather than in the usecase. It expands layout into individual seats
// and enforces:
//   - every Event invariant (via NewEvent),
//   - a well-formed layout: >=1 section, unique section names, Rows and
//     SeatsPerRow >= 1, PriceMinor >= 0 (ErrInvalidLayout),
//   - every exception targets a seat the grid actually produces, no two
//     exceptions target the same seat, and each exception removes or reprices
//     (ErrInvalidLayout),
//   - the expanded map fits under MaxSeatsPerEvent (ErrLayoutTooLarge),
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
	if len(layout.Sections) == 0 {
		return nil, nil, ErrEventRequiresSeats
	}

	// Validate sections and size the expansion up front, so an abusive layout
	// is rejected before anything is allocated.
	sections := make(map[string]SectionSpec, len(layout.Sections))
	total := 0
	for _, s := range layout.Sections {
		s.Name = strings.TrimSpace(s.Name)
		if s.Name == "" || s.Rows < 1 || s.SeatsPerRow < 1 || s.PriceMinor < 0 {
			return nil, nil, ErrInvalidLayout
		}
		if _, dup := sections[s.Name]; dup {
			return nil, nil, ErrInvalidLayout
		}
		sections[s.Name] = s
		total += s.Rows * s.SeatsPerRow
		if total > MaxSeatsPerEvent {
			return nil, nil, ErrLayoutTooLarge
		}
	}

	// Index exceptions by the seat they address; reject any that fall outside
	// the grid, collide with another exception, or would do nothing.
	type adjust struct {
		remove bool
		price  *int64
	}
	byKey := make(map[string]adjust, len(layout.Exceptions))
	for _, ex := range layout.Exceptions {
		sec, ok := sections[strings.TrimSpace(ex.Section)]
		if !ok {
			return nil, nil, ErrInvalidLayout
		}
		row, err1 := strconv.Atoi(ex.Row)
		num, err2 := strconv.Atoi(ex.Number)
		if err1 != nil || err2 != nil || row < 1 || row > sec.Rows || num < 1 || num > sec.SeatsPerRow {
			return nil, nil, ErrInvalidLayout
		}
		if !ex.Remove && ex.PriceMinor == nil {
			return nil, nil, ErrInvalidLayout
		}
		if ex.PriceMinor != nil && *ex.PriceMinor < 0 {
			return nil, nil, ErrInvalidLayout
		}
		key := sec.Name + "\x00" + ex.Row + "\x00" + ex.Number
		if _, dup := byKey[key]; dup {
			return nil, nil, ErrInvalidLayout
		}

		byKey[key] = adjust{remove: ex.Remove, price: ex.PriceMinor}
	}

	seats := make([]Seat, 0, total)
	for _, s := range layout.Sections {
		secName := strings.TrimSpace(s.Name)
		for r := 1; r <= s.Rows; r++ {
			rs := strconv.Itoa(r)
			for n := 1; n <= s.SeatsPerRow; n++ {
				ns := strconv.Itoa(n)
				price := s.PriceMinor
				if a, ok := byKey[secName+"\x00"+rs+"\x00"+ns]; ok {
					if a.remove {
						continue
					}
					if a.price != nil {
						price = *a.price
					}
				}
				seat, err := NewSeat(event.ID, secName, rs, ns, price)
				if err != nil {
					return nil, nil, err
				}
				seats = append(seats, *seat)
			}
		}
	}

	if len(seats) == 0 {
		return nil, nil, ErrEventRequiresSeats
	}
	return event, seats, nil
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
