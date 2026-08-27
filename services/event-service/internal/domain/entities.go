package domain

import (
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
