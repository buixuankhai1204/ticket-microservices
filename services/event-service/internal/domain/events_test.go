package domain

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
)

func jsonKeys(t *testing.T, v any) []string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func TestNewSeatReservedEvent(t *testing.T) {
	bookingID, ticketedEventID := uuid.New(), uuid.New()
	seatIDs := []uuid.UUID{uuid.New(), uuid.New()}
	reservedAt := time.Date(2030, 5, 1, 12, 0, 0, 0, time.UTC)

	e := NewSeatReservedEvent(bookingID, ticketedEventID, seatIDs, reservedAt)

	assertV4UUID(t, e.ID, "SeatReservedEvent.ID")
	if e.BookingID != bookingID || e.TicketedEventID != ticketedEventID {
		t.Fatalf("ids not copied: %+v", e)
	}
	if !reflect.DeepEqual(e.SeatIDs, seatIDs) {
		t.Fatalf("SeatIDs = %v, want %v", e.SeatIDs, seatIDs)
	}
	if !e.ReservedAt.Equal(reservedAt) {
		t.Fatalf("ReservedAt = %v, want %v", e.ReservedAt, reservedAt)
	}

	if e.EventID() != e.ID {
		t.Fatalf("EventID() = %v, want %v", e.EventID(), e.ID)
	}
	if e.AggregateID() != bookingID {
		t.Fatalf("AggregateID() = %v, want booking id %v", e.AggregateID(), bookingID)
	}
	if e.EventType() != "SeatReserved" {
		t.Fatalf("EventType() = %q, want %q", e.EventType(), "SeatReserved")
	}
	if e.AggregateType() != "seat_reservation" {
		t.Fatalf("AggregateType() = %q, want %q", e.AggregateType(), "seat_reservation")
	}

	want := []string{"booking_id", "event_id", "reserved_at", "seat_ids", "ticketed_event_id"}
	if got := jsonKeys(t, e); !reflect.DeepEqual(got, want) {
		t.Fatalf("payload keys = %v, want %v", got, want)
	}
}

func TestNewSeatReservationFailedEvent(t *testing.T) {
	bookingID, ticketedEventID := uuid.New(), uuid.New()
	seatIDs := []uuid.UUID{uuid.New()}
	failedAt := time.Date(2030, 5, 1, 12, 30, 0, 0, time.UTC)

	e := NewSeatReservationFailedEvent(bookingID, ticketedEventID, seatIDs, ReasonSeatUnavailable, failedAt)

	assertV4UUID(t, e.ID, "SeatReservationFailedEvent.ID")
	if e.BookingID != bookingID || e.TicketedEventID != ticketedEventID {
		t.Fatalf("ids not copied: %+v", e)
	}
	if !reflect.DeepEqual(e.SeatIDs, seatIDs) {
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
		t.Fatalf("AggregateID() = %v, want booking id %v", e.AggregateID(), bookingID)
	}
	if e.EventType() != "SeatReservationFailed" {
		t.Fatalf("EventType() = %q, want %q", e.EventType(), "SeatReservationFailed")
	}
	if e.AggregateType() != "seat_reservation" {
		t.Fatalf("AggregateType() = %q, want %q", e.AggregateType(), "seat_reservation")
	}

	want := []string{"booking_id", "event_id", "failed_at", "reason", "seat_ids", "ticketed_event_id"}
	if got := jsonKeys(t, e); !reflect.DeepEqual(got, want) {
		t.Fatalf("payload keys = %v, want %v", got, want)
	}
}
