//go:build contract

package tests

import (
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/pact-foundation/pact-go/v2/matchers"
	v4 "github.com/pact-foundation/pact-go/v2/message/v4"

	kafka "github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/adapter/messaging/kafka"
)

var pactDir = filepath.Join("..", "pacts")

func bookingBody(extra map[string]any) map[string]any {
	body := map[string]any{
		"event_id":          matchers.UUID(),
		"booking_id":        matchers.UUID(),
		"user_id":           matchers.UUID(),
		"ticketed_event_id": matchers.UUID(),
		"seat_ids":          matchers.EachLike(matchers.UUID(), 1),
	}
	for k, v := range extra {
		body[k] = v
	}
	return body
}

func expect[E any](t *testing.T, pact *v4.AsynchronousPact, state, description, eventType string, body map[string]any, spec kafka.EventSpec[E]) E {
	t.Helper()
	var got E
	err := pact.AddAsynchronousMessage().
		Given(state).
		ExpectsToReceive(description).
		WithMetadata(map[string]string{"event_type": eventType, "topic": "booking.events"}).
		WithJSONContent(body).
		AsType(&map[string]any{}).
		ConsumedBy(func(mc v4.AsynchronousMessage) error {
			ev, err := spec.Parse(mc.Contents)
			if err != nil {
				return err
			}
			got = ev
			return nil
		}).
		Verify(t)
	if err != nil {
		t.Fatalf("%s: %v", description, err)
	}
	return got
}

func TestBookingServiceEventsAreUnderstood(t *testing.T) {
	pact, err := v4.NewAsynchronousPact(v4.Config{Consumer: "event-service", Provider: "booking-service", PactDir: pactDir})
	if err != nil {
		t.Fatal(err)
	}

	requested := expect(t, pact, "a customer has just asked for two seats", "a BookingRequested event", "BookingRequested",
		bookingBody(map[string]any{"requested_at": matchers.Timestamp()}), kafka.BookingRequestedSpec(nil))
	if requested.BookingID == uuid.Nil || requested.TicketedEventID == uuid.Nil || len(requested.SeatIDs) == 0 {
		t.Fatalf("BookingRequested understood as %+v", requested)
	}

	confirmed := expect(t, pact, "a booking has just been confirmed", "a BookingConfirmed event", "BookingConfirmed",
		bookingBody(map[string]any{"occurred_at": matchers.Timestamp()}), kafka.BookingConfirmedSpec(nil))
	if confirmed.BookingID == uuid.Nil || len(confirmed.SeatIDs) == 0 {
		t.Fatalf("BookingConfirmed understood as %+v", confirmed)
	}

	cancelled := expect(t, pact, "a booking has just been cancelled", "a BookingCancelled event", "BookingCancelled",
		bookingBody(map[string]any{"reason": matchers.Like("seat_unavailable"), "occurred_at": matchers.Timestamp()}), kafka.BookingCancelledSpec(nil))
	if cancelled.BookingID == uuid.Nil || cancelled.Reason != "seat_unavailable" {
		t.Fatalf("BookingCancelled understood as %+v", cancelled)
	}
}
