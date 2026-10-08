//go:build contract

package tests

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/pact-foundation/pact-go/v2/matchers"
	v4 "github.com/pact-foundation/pact-go/v2/message/v4"
	segkafka "github.com/segmentio/kafka-go"

	kafka "github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/adapter/messaging/kafka"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/testsupport"
)

var pactDir = filepath.Join("..", "pacts")

type captured[E any] struct {
	events []E
}

func (c *captured[E]) Execute(_ context.Context, ev E) (bool, error) {
	c.events = append(c.events, ev)
	return false, nil
}

type nothingMayBeParked struct {
	t *testing.T
}

func (n nothingMayBeParked) Park(_ context.Context, _ segkafka.Message, reason string) error {
	n.t.Errorf("the example message was parked: %s", reason)
	return nil
}

func consume[E any](t *testing.T, spec kafka.EventSpec[E], eventType string, mc v4.AsynchronousMessage) {
	t.Helper()
	p := kafka.NewProcessor(spec, nothingMayBeParked{t}, kafka.DefaultRetryPolicy(1), testsupport.SilentLogger{})

	outcome, err := p.Process(context.Background(), segkafka.Message{
		Value:   mc.Contents,
		Headers: []segkafka.Header{testsupport.EventType(eventType)},
	})

	if err != nil || outcome != kafka.Handled {
		t.Fatalf("the example message was not handled: (%v, %v)", outcome, err)
	}
}

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

func expect[E any](t *testing.T, pact *v4.AsynchronousPact, state, description, eventType string, body map[string]any, spec kafka.EventSpec[E]) {
	t.Helper()
	err := pact.AddAsynchronousMessage().
		Given(state).
		ExpectsToReceive(description).
		WithMetadata(map[string]string{"event_type": eventType, "topic": "booking.events"}).
		WithJSONContent(body).
		AsType(&map[string]any{}).
		ConsumedBy(func(mc v4.AsynchronousMessage) error {
			consume(t, spec, eventType, mc)
			return nil
		}).
		Verify(t)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBookingServiceEventsAreUnderstood(t *testing.T) {
	pact, err := v4.NewAsynchronousPact(v4.Config{Consumer: "event-service", Provider: "booking-service", PactDir: pactDir})
	if err != nil {
		t.Fatal(err)
	}

	requested := &captured[domain.BookingRequested]{}
	expect(t, pact, "a customer has just asked for two seats", "a BookingRequested event", "BookingRequested",
		bookingBody(map[string]any{"requested_at": matchers.Timestamp()}), kafka.BookingRequestedSpec(requested))
	if len(requested.events) != 1 || requested.events[0].BookingID == uuid.Nil || requested.events[0].TicketedEventID == uuid.Nil || len(requested.events[0].SeatIDs) == 0 {
		t.Fatalf("BookingRequested understood as %+v", requested.events)
	}

	confirmed := &captured[domain.BookingConfirmed]{}
	expect(t, pact, "a booking has just been confirmed", "a BookingConfirmed event", "BookingConfirmed",
		bookingBody(map[string]any{"occurred_at": matchers.Timestamp()}), kafka.BookingConfirmedSpec(confirmed))
	if len(confirmed.events) != 1 || confirmed.events[0].BookingID == uuid.Nil || len(confirmed.events[0].SeatIDs) == 0 {
		t.Fatalf("BookingConfirmed understood as %+v", confirmed.events)
	}

	cancelled := &captured[domain.BookingCancelled]{}
	expect(t, pact, "a booking has just been cancelled", "a BookingCancelled event", "BookingCancelled",
		bookingBody(map[string]any{"reason": matchers.Like("seat_unavailable"), "occurred_at": matchers.Timestamp()}), kafka.BookingCancelledSpec(cancelled))
	if len(cancelled.events) != 1 || cancelled.events[0].BookingID == uuid.Nil || cancelled.events[0].Reason != "seat_unavailable" {
		t.Fatalf("BookingCancelled understood as %+v", cancelled.events)
	}
}
