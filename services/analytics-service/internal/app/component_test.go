//go:build component

package app_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	segkafka "github.com/segmentio/kafka-go"

	kafka "github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/messaging/kafka"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/app"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/config"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/testsupport"
)

func TestMain(m *testing.M) {
	os.Exit(testsupport.Run(m))
}

const (
	userTopic    = "user.events"
	bookingTopic = "booking.events"
)

type stubDeadLetters struct {
	mu      sync.Mutex
	parked  []string
	payload [][]byte
}

func (s *stubDeadLetters) Park(_ context.Context, m segkafka.Message, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.parked = append(s.parked, reason)
	s.payload = append(s.payload, m.Value)
	return nil
}

func (s *stubDeadLetters) reasons() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.parked...)
}

type service struct {
	t     *testing.T
	url   string
	inbox kafka.Inbox
	dead  *stubDeadLetters
	close func()
}

func newService(t *testing.T) *service {
	t.Helper()
	pool := testsupport.NewDatabase(t)
	cfg := config.Config{KafkaUserEventsTopic: userTopic, KafkaBookingEventsTopic: bookingTopic}
	application := app.New(pool, cfg, testsupport.SilentLogger{})
	server := httptest.NewServer(application.Router)
	t.Cleanup(server.Close)

	dead := &stubDeadLetters{}
	policy := kafka.RetryPolicy{MaxAttempts: 3, FirstBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}
	return &service{t: t, url: server.URL, inbox: application.NewInbox(dead, policy), dead: dead, close: pool.Close}
}

func (s *service) get(path string, headers ...string) (int, http.Header, []byte) {
	s.t.Helper()
	req, err := http.NewRequest(http.MethodGet, s.url+path, nil)
	if err != nil {
		s.t.Fatal(err)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	return s.do(req)
}

func (s *service) do(req *http.Request) (int, http.Header, []byte) {
	s.t.Helper()
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	return res.StatusCode, res.Header, body
}

func (s *service) deliver(topic, eventType string, payload any) kafka.Outcome {
	s.t.Helper()
	raw, ok := payload.([]byte)
	if !ok {
		var err error
		if raw, err = json.Marshal(payload); err != nil {
			s.t.Fatal(err)
		}
	}
	outcome, err := s.inbox.Deliver(context.Background(), segkafka.Message{
		Topic:   topic,
		Key:     []byte(uuid.NewString()),
		Value:   raw,
		Headers: []segkafka.Header{testsupport.EventType(eventType)},
	})
	if err != nil {
		s.t.Fatalf("deliver %s: %v", eventType, err)
	}
	return outcome
}

func userCreated(eventID, userID uuid.UUID, email string) map[string]any {
	return map[string]any{
		"event_id":   eventID.String(),
		"user_id":    userID.String(),
		"email":      email,
		"created_at": "2026-03-04T08:30:00Z",
	}
}

func bookingOutcome(eventID, bookingID, ticketedEventID uuid.UUID) map[string]any {
	return map[string]any{
		"event_id":          eventID.String(),
		"booking_id":        bookingID.String(),
		"ticketed_event_id": ticketedEventID.String(),
		"occurred_at":       "2026-03-04T08:30:00Z",
	}
}

type stats struct {
	EventID   uuid.UUID `json:"event_id"`
	Confirmed int64     `json:"confirmed"`
	Cancelled int64     `json:"cancelled"`
}

func (s *service) stats(eventID uuid.UUID) stats {
	s.t.Helper()
	status, _, body := s.get("/api/v1/analytics/events/" + eventID.String())
	if status != http.StatusOK {
		s.t.Fatalf("GET stats = %d %s", status, body)
	}
	var out stats
	if err := json.Unmarshal(body, &out); err != nil {
		s.t.Fatal(err)
	}
	return out
}

func TestARegisteredUserCanBeQueriedOnceTheEventHasBeenDelivered(t *testing.T) {
	t.Parallel()
	s := newService(t)
	userID := uuid.New()

	outcome := s.deliver(userTopic, "UserCreated", userCreated(uuid.New(), userID, "ada@example.com"))

	if outcome != kafka.Handled {
		t.Fatalf("outcome = %v, want handled", outcome)
	}
	status, _, body := s.get("/api/v1/analytics/users/" + userID.String())
	var got struct {
		UserID       uuid.UUID `json:"user_id"`
		Email        string    `json:"email"`
		RegisteredAt time.Time `json:"registered_at"`
	}
	if err := json.Unmarshal(body, &got); err != nil || status != http.StatusOK {
		t.Fatalf("GET user = %d %s", status, body)
	}
	if got.UserID != userID || got.Email != "ada@example.com" || !got.RegisteredAt.Equal(time.Date(2026, 3, 4, 8, 30, 0, 0, time.UTC)) {
		t.Fatalf("user = %+v", got)
	}
}

func TestRedeliveringARegistrationEventChangesNothing(t *testing.T) {
	t.Parallel()
	s := newService(t)
	eventID, userID := uuid.New(), uuid.New()
	s.deliver(userTopic, "UserCreated", userCreated(eventID, userID, "ada@example.com"))
	_, _, before := s.get("/api/v1/analytics/users/" + userID.String())

	outcome := s.deliver(userTopic, "UserCreated", userCreated(eventID, userID, "changed@example.com"))

	if outcome != kafka.Handled {
		t.Fatalf("outcome = %v, want handled", outcome)
	}
	_, _, after := s.get("/api/v1/analytics/users/" + userID.String())
	if string(after) != string(before) {
		t.Fatalf("a replayed event changed the projection:\nbefore %s\nafter  %s", before, after)
	}
	if reasons := s.dead.reasons(); len(reasons) != 0 {
		t.Fatalf("a duplicate must not be dead-lettered: %v", reasons)
	}
}

func TestBookingOutcomesAreCountedOncePerBookingEvenWhenEventsAreReplayedOrDisagree(t *testing.T) {
	t.Parallel()
	s := newService(t)
	ticketedEvent := uuid.New()
	first, second, third := uuid.New(), uuid.New(), uuid.New()
	firstConfirmedEvent := uuid.New()

	s.deliver(bookingTopic, "BookingConfirmed", bookingOutcome(firstConfirmedEvent, first, ticketedEvent))
	s.deliver(bookingTopic, "BookingConfirmed", bookingOutcome(uuid.New(), second, ticketedEvent))
	s.deliver(bookingTopic, "BookingCancelled", bookingOutcome(uuid.New(), third, ticketedEvent))
	if got := s.stats(ticketedEvent); got.Confirmed != 2 || got.Cancelled != 1 {
		t.Fatalf("stats = %+v, want 2 confirmed 1 cancelled", got)
	}

	s.deliver(bookingTopic, "BookingConfirmed", bookingOutcome(firstConfirmedEvent, first, ticketedEvent))
	s.deliver(bookingTopic, "BookingCancelled", bookingOutcome(uuid.New(), first, ticketedEvent))

	if got := s.stats(ticketedEvent); got.Confirmed != 2 || got.Cancelled != 1 {
		t.Fatalf("stats = %+v after a replay and a conflicting outcome, want unchanged 2/1", got)
	}
}

func TestAnEventNobodyHasBookedYetHasZeroStatsRatherThanBeingMissing(t *testing.T) {
	t.Parallel()
	s := newService(t)
	eventID := uuid.New()

	got := s.stats(eventID)

	if got.EventID != eventID || got.Confirmed != 0 || got.Cancelled != 0 {
		t.Fatalf("stats = %+v, want zeros for %s", got, eventID)
	}
}

func TestMessagesTheServiceCannotUseNeverReachTheReadModel(t *testing.T) {
	t.Parallel()
	s := newService(t)
	userID := uuid.New()

	tests := []struct {
		name        string
		eventType   string
		payload     any
		wantOutcome kafka.Outcome
		wantReason  string
	}{
		{"another event type on the same topic", "UserDeleted", userCreated(uuid.New(), userID, "ada@example.com"), kafka.Ignored, ""},
		{"payload that is not json", "UserCreated", []byte("{{ nope"), kafka.Parked, "parse:"},
		{"user id that is not a uuid", "UserCreated", map[string]any{"event_id": uuid.NewString(), "user_id": "42", "email": "ada@example.com", "created_at": "2026-03-04T08:30:00Z"}, kafka.Parked, "parse:"},
		{"email the domain rejects", "UserCreated", userCreated(uuid.New(), userID, "not-an-email"), kafka.Parked, "permanent:"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := len(s.dead.reasons())

			outcome := s.deliver(userTopic, tc.eventType, tc.payload)

			if outcome != tc.wantOutcome {
				t.Fatalf("outcome = %v, want %v", outcome, tc.wantOutcome)
			}
			reasons := s.dead.reasons()
			if tc.wantReason == "" {
				if len(reasons) != before {
					t.Fatalf("an ignored message was dead-lettered: %v", reasons)
				}
				return
			}
			if len(reasons) != before+1 || !strings.HasPrefix(reasons[before], tc.wantReason) {
				t.Fatalf("reasons = %v, want a new one starting %q", reasons, tc.wantReason)
			}
		})
	}

	if status, _, _ := s.get("/api/v1/analytics/users/" + userID.String()); status != http.StatusNotFound {
		t.Fatalf("GET user = %d, want 404: none of those messages may be recorded", status)
	}
}

func TestMalformedRequestsAreAnsweredWithTheRightStatus(t *testing.T) {
	t.Parallel()
	s := newService(t)

	tests := []struct {
		name   string
		method string
		path   string
		want   int
	}{
		{"user id that is not a uuid", http.MethodGet, "/api/v1/analytics/users/42", http.StatusBadRequest},
		{"event id that is not a uuid", http.MethodGet, "/api/v1/analytics/events/42", http.StatusBadRequest},
		{"user nobody registered", http.MethodGet, "/api/v1/analytics/users/" + uuid.NewString(), http.StatusNotFound},
		{"writing to a read-only projection", http.MethodPost, "/api/v1/analytics/users/" + uuid.NewString(), http.StatusMethodNotAllowed},
		{"route that does not exist", http.MethodGet, "/api/v1/analytics/nothing", http.StatusNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(tc.method, s.url+tc.path, nil)
			if status, _, body := s.do(req); status != tc.want {
				t.Fatalf("%s %s = %d %s, want %d", tc.method, tc.path, status, body, tc.want)
			}
		})
	}
}

func TestEveryResponseCarriesARequestIdAndTheServiceReportsItselfHealthy(t *testing.T) {
	t.Parallel()
	s := newService(t)

	_, headers, _ := s.get("/healthz", "X-Request-Id", "req-123")
	if got := headers.Get("X-Request-Id"); got != "req-123" {
		t.Fatalf("X-Request-Id = %q, want the inbound id echoed", got)
	}
	_, headers, _ = s.get("/healthz")
	if _, err := uuid.Parse(headers.Get("X-Request-Id")); err != nil {
		t.Fatalf("X-Request-Id = %q, want a generated uuid", headers.Get("X-Request-Id"))
	}
	if status, _, _ := s.get("/readyz"); status != http.StatusOK {
		t.Fatalf("/readyz = %d, want 200 with the database up", status)
	}
	s.get("/api/v1/analytics/users/" + uuid.NewString())
	status, _, body := s.get("/metrics")
	if status != http.StatusOK || !strings.Contains(string(body), "http_requests_total") {
		t.Fatalf("/metrics = %d, want the RED counters exposed", status)
	}
}

func TestWhenTheDatabaseIsGoneEventsAreRetriedThenParkedAndReadinessFails(t *testing.T) {
	t.Parallel()
	s := newService(t)
	s.close()

	outcome := s.deliver(userTopic, "UserCreated", userCreated(uuid.New(), uuid.New(), "ada@example.com"))

	if outcome != kafka.Parked {
		t.Fatalf("outcome = %v, want parked after the retries ran out", outcome)
	}
	if reasons := s.dead.reasons(); len(reasons) != 1 || !strings.HasPrefix(reasons[0], "max-retries after 3 attempts") {
		t.Fatalf("reasons = %v, want one max-retries after 3 attempts", reasons)
	}
	if status, _, _ := s.get("/readyz"); status != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503", status)
	}
	if status, _, _ := s.get("/api/v1/analytics/users/" + uuid.NewString()); status != http.StatusInternalServerError {
		t.Fatalf("GET user = %d, want 500", status)
	}
}
