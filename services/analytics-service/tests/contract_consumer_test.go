//go:build contract

package tests

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pact-foundation/pact-go/v2/matchers"
	v4 "github.com/pact-foundation/pact-go/v2/message/v4"
	segkafka "github.com/segmentio/kafka-go"

	kafka "github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/messaging/kafka"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/testsupport"
)

var pactDir = filepath.Join("..", "pacts")

const (
	exampleUUID = "3f2b8c1e-5a47-4d0e-9a52-1c6f0e7d2b90"
	exampleTime = "2026-03-04T08:30:00Z"
)

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

func newPact(t *testing.T, provider string) *v4.AsynchronousPact {
	t.Helper()
	p, err := v4.NewAsynchronousPact(v4.Config{Consumer: "analytics-service", Provider: provider, PactDir: pactDir})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestUserServiceEventsAreUnderstood(t *testing.T) {
	pact := newPact(t, "user-service")

	created := &captured[domain.UserCreated]{}
	err := pact.AddAsynchronousMessage().
		Given("a user has just registered").
		ExpectsToReceive("a UserCreated event").
		WithMetadata(map[string]string{"event_type": "UserCreated", "topic": "user.events"}).
		WithJSONContent(map[string]any{
			"event_id":   matchers.UUID(),
			"user_id":    matchers.UUID(),
			"email":      matchers.Like("ada@example.com"),
			"created_at": matchers.Timestamp(),
		}).
		AsType(&map[string]any{}).
		ConsumedBy(func(mc v4.AsynchronousMessage) error {
			consume(t, kafka.UserCreatedSpec(created), "UserCreated", mc)
			return nil
		}).
		Verify(t)
	if err != nil {
		t.Fatal(err)
	}
	if len(created.events) != 1 || created.events[0].Email != "ada@example.com" || created.events[0].UserID == uuid.Nil {
		t.Fatalf("UserCreated understood as %+v", created.events)
	}

	loggedIn := &captured[domain.UserLoggedIn]{}
	err = pact.AddAsynchronousMessage().
		Given("a user has just logged in").
		ExpectsToReceive("a UserLoggedIn event").
		WithMetadata(map[string]string{"event_type": "UserLoggedIn", "topic": "user.events"}).
		WithJSONContent(map[string]any{
			"event_id":     matchers.UUID(),
			"user_id":      matchers.UUID(),
			"email":        matchers.Like("ada@example.com"),
			"logged_in_at": matchers.Timestamp(),
		}).
		AsType(&map[string]any{}).
		ConsumedBy(func(mc v4.AsynchronousMessage) error {
			consume(t, kafka.UserLoggedInSpec(loggedIn), "UserLoggedIn", mc)
			return nil
		}).
		Verify(t)
	if err != nil {
		t.Fatal(err)
	}
	if len(loggedIn.events) != 1 || loggedIn.events[0].Email != "ada@example.com" || loggedIn.events[0].LoggedInAt.IsZero() {
		t.Fatalf("UserLoggedIn understood as %+v", loggedIn.events)
	}
}

func TestBookingServiceOutcomesAreUnderstood(t *testing.T) {
	pact := newPact(t, "booking-service")
	body := map[string]any{
		"event_id":          matchers.UUID(),
		"booking_id":        matchers.UUID(),
		"ticketed_event_id": matchers.UUID(),
		"occurred_at":       matchers.Timestamp(),
	}

	confirmed := &captured[domain.BookingConfirmed]{}
	err := pact.AddAsynchronousMessage().
		Given("a booking has just been confirmed").
		ExpectsToReceive("a BookingConfirmed event").
		WithMetadata(map[string]string{"event_type": "BookingConfirmed", "topic": "booking.events"}).
		WithJSONContent(body).
		AsType(&map[string]any{}).
		ConsumedBy(func(mc v4.AsynchronousMessage) error {
			consume(t, kafka.BookingConfirmedSpec(confirmed), "BookingConfirmed", mc)
			return nil
		}).
		Verify(t)
	if err != nil {
		t.Fatal(err)
	}
	if len(confirmed.events) != 1 || confirmed.events[0].BookingID == uuid.Nil || confirmed.events[0].TicketedEventID == uuid.Nil {
		t.Fatalf("BookingConfirmed understood as %+v", confirmed.events)
	}

	cancelled := &captured[domain.BookingCancelled]{}
	err = pact.AddAsynchronousMessage().
		Given("a booking has just been cancelled").
		ExpectsToReceive("a BookingCancelled event").
		WithMetadata(map[string]string{"event_type": "BookingCancelled", "topic": "booking.events"}).
		WithJSONContent(body).
		AsType(&map[string]any{}).
		ConsumedBy(func(mc v4.AsynchronousMessage) error {
			consume(t, kafka.BookingCancelledSpec(cancelled), "BookingCancelled", mc)
			return nil
		}).
		Verify(t)
	if err != nil {
		t.Fatal(err)
	}
	if len(cancelled.events) != 1 || cancelled.events[0].BookingID == uuid.Nil || cancelled.events[0].OccurredAt.After(time.Now().Add(24*time.Hour)) {
		t.Fatalf("BookingCancelled understood as %+v", cancelled.events)
	}
}
