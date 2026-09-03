package domain

import (
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	SeatAvailable = "available"
	SeatReserved  = "reserved"
	SeatBooked    = "booked"
)

type Event struct {
	ID          uuid.UUID
	Name        string
	Description string
	Venue       string
	StartsAt    time.Time
	EndsAt      time.Time
	CreatedAt   time.Time
}

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

const MaxSeatsPerEvent = 200_000

type SectionSpec struct {
	Name        string
	Rows        int
	SeatsPerRow int
	PriceMinor  int64
}

type SeatException struct {
	Section    string
	Row        string
	Number     string
	Remove     bool
	PriceMinor *int64
}

type LayoutSpec struct {
	Sections   []SectionSpec
	Exceptions []SeatException
}

type seatAdjust struct {
	remove bool
	price  *int64
}

func seatKey(section, row, number string) string {
	return section + "\x00" + row + "\x00" + number
}

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

	seats, err := initSeats(event.ID, layout.Sections, adjustments)
	if err != nil {
		return nil, nil, err
	}
	if len(seats) == 0 {
		return nil, nil, ErrEventRequiresSeats
	}
	return event, seats, nil
}

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

func initSeats(eventID uuid.UUID, specs []SectionSpec, adjustments map[string]seatAdjust) ([]Seat, error) {
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

type Seat struct {
	ID         uuid.UUID
	EventID    uuid.UUID
	Section    string
	Row        string
	Number     string
	Status     string
	PriceMinor int64
}

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

func (s *Seat) Reserve() error {
	if s.Status != SeatAvailable {
		return ErrSeatUnavailable
	}
	s.Status = SeatReserved
	return nil
}

func (s *Seat) Release() error {
	if s.Status != SeatReserved {
		return ErrSeatUnavailable
	}
	s.Status = SeatAvailable
	return nil
}

func (s *Seat) IsAvailable() bool { return s.Status == SeatAvailable }
