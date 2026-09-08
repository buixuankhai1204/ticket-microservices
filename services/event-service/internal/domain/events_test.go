package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func assertV4(t *testing.T, id uuid.UUID) {
	t.Helper()
	if id == uuid.Nil {
		t.Fatalf("id is the nil UUID")
	}
	if id.Version() != 4 {
		t.Fatalf("id version = %d, want 4", id.Version())
	}
	if id.Variant() != uuid.RFC4122 {
		t.Fatalf("id variant = %v, want RFC4122", id.Variant())
	}
}

func TestNewSeatReservedEvent(t *testing.T) {
	bookingID := uuid.New()
	ticketedEventID := uuid.New()
	seatIDs := []uuid.UUID{uuid.New(), uuid.New()}
	reservedAt := time.Date(2030, 5, 1, 12, 0, 0, 0, time.UTC)

	e := NewSeatReservedEvent(bookingID, ticketedEventID, seatIDs, reservedAt)

	assertV4(t, e.ID)
	if e.BookingID != bookingID {
		t.Fatalf("BookingID = %v, want %v", e.BookingID, bookingID)
	}
	if e.TicketedEventID != ticketedEventID {
		t.Fatalf("TicketedEventID = %v, want %v", e.TicketedEventID, ticketedEventID)
	}
	if len(e.SeatIDs) != len(seatIDs) || e.SeatIDs[0] != seatIDs[0] || e.SeatIDs[1] != seatIDs[1] {
		t.Fatalf("SeatIDs = %v, want %v", e.SeatIDs, seatIDs)
	}
	if !e.ReservedAt.Equal(reservedAt) {
		t.Fatalf("ReservedAt = %v, want %v", e.ReservedAt, reservedAt)
	}
	if e.EventID() != e.ID {
		t.Fatalf("EventID() = %v, want %v", e.EventID(), e.ID)
	}
	if e.AggregateID() != bookingID {
		t.Fatalf("AggregateID() = %v, want %v", e.AggregateID(), bookingID)
	}
	if e.EventType() != "SeatReserved" {
		t.Fatalf("EventType() = %q, want %q", e.EventType(), "SeatReserved")
	}
	if e.AggregateType() != "seat_reservation" {
		t.Fatalf("AggregateType() = %q, want %q", e.AggregateType(), "seat_reservation")
	}
}

func TestNewSeatReservationFailedEvent(t *testing.T) {
	bookingID := uuid.New()
	ticketedEventID := uuid.New()
	seatIDs := []uuid.UUID{uuid.New()}
	failedAt := time.Date(2030, 5, 1, 12, 30, 0, 0, time.UTC)

	e := NewSeatReservationFailedEvent(bookingID, ticketedEventID, seatIDs, ReasonSeatUnavailable, failedAt)

	assertV4(t, e.ID)
	if e.BookingID != bookingID {
		t.Fatalf("BookingID = %v, want %v", e.BookingID, bookingID)
	}
	if e.TicketedEventID != ticketedEventID {
		t.Fatalf("TicketedEventID = %v, want %v", e.TicketedEventID, ticketedEventID)
	}
	if len(e.SeatIDs) != 1 || e.SeatIDs[0] != seatIDs[0] {
		t.Fatalf("SeatIDs = %v, want %v", e.SeatIDs, seatIDs)
	}
	if e.Reason != ReasonSeatUnavailable {
		t.Fatalf("Reason = %q, want %q", e.Reason, ReasonSeatUnavailable)
	}
	if !e.FailedAt.Equal(failedAt) {
		t.Fatalf("FailedAt = %v, want %v", e.FailedAt, failedAt)
	}
	if e.EventID() != e.ID {
		t.Fatalf("EventID() = %v, want %v", e.EventID(), e.ID)
	}
	if e.AggregateID() != bookingID {
		t.Fatalf("AggregateID() = %v, want %v", e.AggregateID(), bookingID)
	}
	if e.EventType() != "SeatReservationFailed" {
		t.Fatalf("EventType() = %q, want %q", e.EventType(), "SeatReservationFailed")
	}
	if e.AggregateType() != "seat_reservation" {
		t.Fatalf("AggregateType() = %q, want %q", e.AggregateType(), "seat_reservation")
	}
}
