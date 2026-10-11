package kafka_test

import (
	"testing"

	"github.com/google/uuid"

	kafka "github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/messaging/kafka"
)

func TestUserCreatedIsReadFromTheWireFormatUserServicePublishes(t *testing.T) {
	eventID, userID := uuid.New(), uuid.New()
	payload := []byte(`{"event_id":"` + eventID.String() + `","user_id":"` + userID.String() +
		`","email":"ada@example.com","created_at":"2026-03-04T08:30:00Z"}`)

	ev, err := kafka.UserCreatedSpec(nil).Parse(payload)

	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if ev.EventID != eventID || ev.UserID != userID || ev.Email != "ada@example.com" || ev.CreatedAt.Year() != 2026 {
		t.Fatalf("event = %+v", ev)
	}
}

func TestBookingOutcomesAreReadWithTheTicketedEventAndOccurrenceTime(t *testing.T) {
	eventID, bookingID, ticketedEventID := uuid.New(), uuid.New(), uuid.New()
	payload := []byte(`{"event_id":"` + eventID.String() + `","booking_id":"` + bookingID.String() +
		`","ticketed_event_id":"` + ticketedEventID.String() + `","occurred_at":"2026-03-04T08:30:00Z"}`)

	confirmed, err := kafka.BookingConfirmedSpec(nil).Parse(payload)
	if err != nil {
		t.Fatalf("BookingConfirmed Parse: %v", err)
	}
	cancelled, err := kafka.BookingCancelledSpec(nil).Parse(payload)
	if err != nil {
		t.Fatalf("BookingCancelled Parse: %v", err)
	}

	if confirmed.BookingID != bookingID || confirmed.TicketedEventID != ticketedEventID || confirmed.EventID != eventID {
		t.Fatalf("confirmed = %+v", confirmed)
	}
	if cancelled.BookingID != bookingID || cancelled.TicketedEventID != ticketedEventID {
		t.Fatalf("cancelled = %+v", cancelled)
	}
}

func TestProductOrderEventsAreReadWithTheOrderIdentityAndTotals(t *testing.T) {
	eventID, orderID, bookingID, userID, ticketedEventID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	identity := `"event_id":"` + eventID.String() + `","order_id":"` + orderID.String() + `","booking_id":"` + bookingID.String() +
		`","user_id":"` + userID.String() + `","ticketed_event_id":"` + ticketedEventID.String() +
		`","currency":"USD","total_minor":1200,"item_count":3`

	placed, err := kafka.ProductOrderPlacedSpec(nil).Parse([]byte(`{` + identity + `,"items":[],"placed_at":"2026-03-04T08:30:00Z"}`))
	if err != nil {
		t.Fatalf("ProductOrderPlaced Parse: %v", err)
	}
	cancelled, err := kafka.ProductOrderCancelledSpec(nil).Parse([]byte(`{` + identity + `,"reason":"booking_cancelled","cancelled_at":"2026-03-05T09:00:00Z"}`))
	if err != nil {
		t.Fatalf("ProductOrderCancelled Parse: %v", err)
	}

	if placed.OrderID != orderID || placed.BookingID != bookingID || placed.TotalMinor != 1200 || placed.ItemCount != 3 || placed.Currency != "USD" || placed.PlacedAt.Day() != 4 {
		t.Fatalf("placed = %+v", placed)
	}
	if cancelled.OrderID != orderID || cancelled.Reason != "booking_cancelled" || cancelled.TotalMinor != 1200 || cancelled.CancelledAt.Day() != 5 {
		t.Fatalf("cancelled = %+v", cancelled)
	}
}

func TestEverySpecRejectsPayloadsItCannotTrust(t *testing.T) {
	parsers := map[string]func([]byte) error{
		"UserCreated":        func(b []byte) error { _, err := kafka.UserCreatedSpec(nil).Parse(b); return err },
		"UserLoggedIn":       func(b []byte) error { _, err := kafka.UserLoggedInSpec(nil).Parse(b); return err },
		"BookingConfirmed":   func(b []byte) error { _, err := kafka.BookingConfirmedSpec(nil).Parse(b); return err },
		"BookingCancelled":   func(b []byte) error { _, err := kafka.BookingCancelledSpec(nil).Parse(b); return err },
		"ProductOrderPlaced": func(b []byte) error { _, err := kafka.ProductOrderPlacedSpec(nil).Parse(b); return err },
		"ProductOrderCancelled": func(b []byte) error {
			_, err := kafka.ProductOrderCancelledSpec(nil).Parse(b)
			return err
		},
	}
	payloads := map[string]string{
		"not json":              `{{ nope`,
		"empty":                 ``,
		"empty object":          `{}`,
		"event id not a uuid":   `{"event_id":"abc","user_id":"` + uuid.NewString() + `","booking_id":"` + uuid.NewString() + `","ticketed_event_id":"` + uuid.NewString() + `","created_at":"2026-03-04T08:30:00Z","logged_in_at":"2026-03-04T08:30:00Z","occurred_at":"2026-03-04T08:30:00Z"}`,
		"timestamp not rfc3339": `{"event_id":"` + uuid.NewString() + `","user_id":"` + uuid.NewString() + `","booking_id":"` + uuid.NewString() + `","ticketed_event_id":"` + uuid.NewString() + `","created_at":"yesterday","logged_in_at":"yesterday","occurred_at":"yesterday"}`,
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
