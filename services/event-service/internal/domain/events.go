package domain

import (
	"time"

	"github.com/google/uuid"
)

const (
	ReasonSeatUnavailable = "seat_unavailable"
	ReasonSeatNotFound    = "seat_not_found"
	ReasonEventNotFound   = "event_not_found"
)

type OutboxEvent interface {
	EventID() uuid.UUID
	AggregateID() uuid.UUID
	EventType() string
	AggregateType() string
}

type BookingRequested struct {
	ID              uuid.UUID
	BookingID       uuid.UUID
	UserID          uuid.UUID
	TicketedEventID uuid.UUID
	SeatIDs         []uuid.UUID
	RequestedAt     time.Time
}

type SeatReservedEvent struct {
	ID              uuid.UUID   `json:"event_id"`
	BookingID       uuid.UUID   `json:"booking_id"`
	TicketedEventID uuid.UUID   `json:"ticketed_event_id"`
	SeatIDs         []uuid.UUID `json:"seat_ids"`
	ReservedAt      time.Time   `json:"reserved_at"`
}

func NewSeatReservedEvent(bookingID, ticketedEventID uuid.UUID, seatIDs []uuid.UUID, reservedAt time.Time) SeatReservedEvent {
	return SeatReservedEvent{
		ID:              uuid.New(),
		BookingID:       bookingID,
		TicketedEventID: ticketedEventID,
		SeatIDs:         seatIDs,
		ReservedAt:      reservedAt,
	}
}

func (e SeatReservedEvent) EventID() uuid.UUID     { return e.ID }
func (e SeatReservedEvent) AggregateID() uuid.UUID { return e.BookingID }
func (e SeatReservedEvent) EventType() string      { return "SeatReserved" }
func (e SeatReservedEvent) AggregateType() string  { return "seat_reservation" }

type SeatReservationFailedEvent struct {
	ID              uuid.UUID   `json:"event_id"`
	BookingID       uuid.UUID   `json:"booking_id"`
	TicketedEventID uuid.UUID   `json:"ticketed_event_id"`
	SeatIDs         []uuid.UUID `json:"seat_ids"`
	Reason          string      `json:"reason"`
	FailedAt        time.Time   `json:"failed_at"`
}

func NewSeatReservationFailedEvent(bookingID, ticketedEventID uuid.UUID, seatIDs []uuid.UUID, reason string, failedAt time.Time) SeatReservationFailedEvent {
	return SeatReservationFailedEvent{
		ID:              uuid.New(),
		BookingID:       bookingID,
		TicketedEventID: ticketedEventID,
		SeatIDs:         seatIDs,
		Reason:          reason,
		FailedAt:        failedAt,
	}
}

func (e SeatReservationFailedEvent) EventID() uuid.UUID     { return e.ID }
func (e SeatReservationFailedEvent) AggregateID() uuid.UUID { return e.BookingID }
func (e SeatReservationFailedEvent) EventType() string      { return "SeatReservationFailed" }
func (e SeatReservationFailedEvent) AggregateType() string  { return "seat_reservation" }
