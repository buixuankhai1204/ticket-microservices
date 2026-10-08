//go:build contract

package tests

import (
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/pact-foundation/pact-go/v2/matchers"
	v4 "github.com/pact-foundation/pact-go/v2/message/v4"

	kafka "github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/messaging/kafka"
)

var pactDir = filepath.Join("..", "pacts")

func expect[E any](t *testing.T, pact *v4.AsynchronousPact, state, description, eventType, topic string, body map[string]any, spec kafka.EventSpec[E]) E {
	t.Helper()
	var got E
	err := pact.AddAsynchronousMessage().
		Given(state).
		ExpectsToReceive(description).
		WithMetadata(map[string]string{"event_type": eventType, "topic": topic}).
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

	created := expect(t, pact, "a user has just registered", "a UserCreated event", "UserCreated", "user.events",
		map[string]any{
			"event_id":   matchers.UUID(),
			"user_id":    matchers.UUID(),
			"email":      matchers.Like("ada@example.com"),
			"created_at": matchers.Timestamp(),
		}, kafka.UserCreatedSpec(nil))
	if created.Email != "ada@example.com" || created.UserID == uuid.Nil || created.EventID == uuid.Nil {
		t.Fatalf("UserCreated understood as %+v", created)
	}

	loggedIn := expect(t, pact, "a user has just logged in", "a UserLoggedIn event", "UserLoggedIn", "user.events",
		map[string]any{
			"event_id":     matchers.UUID(),
			"user_id":      matchers.UUID(),
			"email":        matchers.Like("ada@example.com"),
			"logged_in_at": matchers.Timestamp(),
		}, kafka.UserLoggedInSpec(nil))
	if loggedIn.Email != "ada@example.com" || loggedIn.LoggedInAt.IsZero() {
		t.Fatalf("UserLoggedIn understood as %+v", loggedIn)
	}
}

func TestBookingServiceOutcomesAreUnderstood(t *testing.T) {
	pact := newPact(t, "booking-service")
	body := func() map[string]any {
		return map[string]any{
			"event_id":          matchers.UUID(),
			"booking_id":        matchers.UUID(),
			"ticketed_event_id": matchers.UUID(),
			"occurred_at":       matchers.Timestamp(),
		}
	}

	confirmed := expect(t, pact, "a booking has just been confirmed", "a BookingConfirmed event", "BookingConfirmed", "booking.events",
		body(), kafka.BookingConfirmedSpec(nil))
	if confirmed.BookingID == uuid.Nil || confirmed.TicketedEventID == uuid.Nil {
		t.Fatalf("BookingConfirmed understood as %+v", confirmed)
	}

	cancelled := expect(t, pact, "a booking has just been cancelled", "a BookingCancelled event", "BookingCancelled", "booking.events",
		body(), kafka.BookingCancelledSpec(nil))
	if cancelled.BookingID == uuid.Nil || cancelled.TicketedEventID == uuid.Nil || cancelled.OccurredAt.IsZero() {
		t.Fatalf("BookingCancelled understood as %+v", cancelled)
	}
}
