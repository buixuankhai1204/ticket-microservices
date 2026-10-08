package kafka_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"

	kafka "github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/adapter/messaging/kafka"
)

func bookingPayload(eventID, bookingID, userID, ticketedEventID uuid.UUID, seatIDs ...uuid.UUID) []byte {
	quoted := make([]string, len(seatIDs))
	for i, id := range seatIDs {
		quoted[i] = fmt.Sprintf("%q", id.String())
	}
	return []byte(fmt.Sprintf(
		`{"event_id":%q,"booking_id":%q,"user_id":%q,"ticketed_event_id":%q,"seat_ids":[%s],"requested_at":"2026-03-04T08:30:00Z","occurred_at":"2026-03-04T08:30:00Z","reason":"user_cancelled"}`,
		eventID, bookingID, userID, ticketedEventID, strings.Join(quoted, ",")))
}

func TestBookingEventsAreReadFromTheWireFormatBookingServicePublishes(t *testing.T) {
	eventID, bookingID, userID, ticketedEventID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	seatA, seatB := uuid.New(), uuid.New()
	payload := bookingPayload(eventID, bookingID, userID, ticketedEventID, seatA, seatB)

	requested, err := kafka.BookingRequestedSpec(nil).Parse(payload)
	if err != nil {
		t.Fatalf("BookingRequested: %v", err)
	}
	confirmed, err := kafka.BookingConfirmedSpec(nil).Parse(payload)
	if err != nil {
		t.Fatalf("BookingConfirmed: %v", err)
	}
	cancelled, err := kafka.BookingCancelledSpec(nil).Parse(payload)
	if err != nil {
		t.Fatalf("BookingCancelled: %v", err)
	}

	if requested.ID != eventID || requested.BookingID != bookingID || requested.TicketedEventID != ticketedEventID ||
		len(requested.SeatIDs) != 2 || requested.SeatIDs[0] != seatA || requested.SeatIDs[1] != seatB {
		t.Fatalf("requested = %+v", requested)
	}
	if confirmed.BookingID != bookingID || len(confirmed.SeatIDs) != 2 {
		t.Fatalf("confirmed = %+v", confirmed)
	}
	if cancelled.BookingID != bookingID || cancelled.Reason != "user_cancelled" || len(cancelled.SeatIDs) != 2 {
		t.Fatalf("cancelled = %+v", cancelled)
	}
}

func TestEverySpecRejectsPayloadsItCannotTrust(t *testing.T) {
	parsers := map[string]func([]byte) error{
		"BookingRequested": func(b []byte) error { _, err := kafka.BookingRequestedSpec(nil).Parse(b); return err },
		"BookingConfirmed": func(b []byte) error { _, err := kafka.BookingConfirmedSpec(nil).Parse(b); return err },
		"BookingCancelled": func(b []byte) error { _, err := kafka.BookingCancelledSpec(nil).Parse(b); return err },
	}
	id := uuid.New()
	valid := string(bookingPayload(id, id, id, id, id))
	payloads := map[string]string{
		"not json":                 `{{ nope`,
		"empty":                    ``,
		"empty object":             `{}`,
		"no seats":                 string(bookingPayload(id, id, id, id)),
		"seat id that is not uuid": strings.Replace(valid, `"seat_ids":["`+id.String()+`"]`, `"seat_ids":["seat-1"]`, 1),
		"booking id not a uuid":    strings.Replace(valid, `"booking_id":"`+id.String()+`"`, `"booking_id":"42"`, 1),
		"timestamp not rfc3339":    strings.ReplaceAll(valid, "2026-03-04T08:30:00Z", "yesterday"),
	}
	for name, parse := range parsers {
		for what, payload := range payloads {
			t.Run(name+" "+what, func(t *testing.T) {
				if err := parse([]byte(payload)); err == nil {
					t.Fatalf("payload %q was accepted", payload)
				}
			})
		}
	}
}
