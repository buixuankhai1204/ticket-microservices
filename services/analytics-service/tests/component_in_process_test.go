//go:build component

package tests

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	httpadapter "github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/http"
	kafka "github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/messaging/kafka"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/repository/postgres"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/usecase"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/tests/common"
)

type service struct {
	t         *testing.T
	url       string
	closePool func()
	userReg   *usecase.RecordUserRegistrationUseCase
	userLogin *usecase.RecordUserLoginUseCase
	confirmed *usecase.RecordBookingConfirmedUseCase
	cancelled *usecase.RecordBookingCancelledUseCase
}

func newService(t *testing.T) *service {
	t.Helper()
	pool := common.NewDatabase(t)
	repo := postgres.New()
	handler := httpadapter.NewHandler(
		usecase.NewGetEventStatsUseCase(pool, repo),
		usecase.NewGetUserRegistrationUseCase(pool, repo),
	)
	router := httpadapter.NewRouter(handler, httpadapter.NewHealthHandler(pool),
		httpadapter.RequestID(),
		httpadapter.AccessLog(common.SilentLogger{}),
		httpadapter.Metrics(),
	)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	return &service{
		t: t, url: server.URL, closePool: pool.Close,
		userReg:   usecase.NewRecordUserRegistrationUseCase(pool, repo),
		userLogin: usecase.NewRecordUserLoginUseCase(pool, repo),
		confirmed: usecase.NewRecordBookingConfirmedUseCase(pool, repo),
		cancelled: usecase.NewRecordBookingCancelledUseCase(pool, repo),
	}
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

func (s *service) deliver(eventType string, payload any) (alreadyProcessed bool, err error) {
	s.t.Helper()
	raw, ok := payload.([]byte)
	if !ok {
		var marshalErr error
		if raw, marshalErr = json.Marshal(payload); marshalErr != nil {
			s.t.Fatal(marshalErr)
		}
	}
	ctx := context.Background()
	switch eventType {
	case "UserCreated":
		ev, err := kafka.UserCreatedSpec(nil).Parse(raw)
		if err != nil {
			return false, err
		}
		return s.userReg.Execute(ctx, ev)
	case "UserLoggedIn":
		ev, err := kafka.UserLoggedInSpec(nil).Parse(raw)
		if err != nil {
			return false, err
		}
		return s.userLogin.Execute(ctx, ev)
	case "BookingConfirmed":
		ev, err := kafka.BookingConfirmedSpec(nil).Parse(raw)
		if err != nil {
			return false, err
		}
		return s.confirmed.Execute(ctx, ev)
	case "BookingCancelled":
		ev, err := kafka.BookingCancelledSpec(nil).Parse(raw)
		if err != nil {
			return false, err
		}
		return s.cancelled.Execute(ctx, ev)
	}
	s.t.Fatalf("unknown event type %s", eventType)
	return false, nil
}

func (s *service) mustDeliver(eventType string, payload any) bool {
	s.t.Helper()
	already, err := s.deliver(eventType, payload)
	if err != nil {
		s.t.Fatalf("deliver %s: %v", eventType, err)
	}
	return already
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

type eventStats struct {
	EventID   uuid.UUID `json:"event_id"`
	Confirmed int64     `json:"confirmed"`
	Cancelled int64     `json:"cancelled"`
}

func (s *service) stats(eventID uuid.UUID) eventStats {
	s.t.Helper()
	status, _, body := s.get("/api/v1/analytics/events/" + eventID.String())
	if status != http.StatusOK {
		s.t.Fatalf("GET stats = %d %s", status, body)
	}
	var out eventStats
	if err := json.Unmarshal(body, &out); err != nil {
		s.t.Fatal(err)
	}
	return out
}

func TestARegisteredUserCanBeQueriedOnceTheEventHasBeenDelivered(t *testing.T) {
	t.Parallel()
	s := newService(t)
	userID := uuid.New()

	s.mustDeliver("UserCreated", userCreated(uuid.New(), userID, "ada@example.com"))

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
	s.mustDeliver("UserCreated", userCreated(eventID, userID, "ada@example.com"))
	_, _, before := s.get("/api/v1/analytics/users/" + userID.String())

	already := s.mustDeliver("UserCreated", userCreated(eventID, userID, "changed@example.com"))

	if !already {
		t.Fatalf("a replayed event must be recognised as already processed")
	}
	_, _, after := s.get("/api/v1/analytics/users/" + userID.String())
	if string(after) != string(before) {
		t.Fatalf("a replayed event changed the projection:\nbefore %s\nafter  %s", before, after)
	}
}

func TestBookingOutcomesAreCountedOncePerBookingEvenWhenEventsAreReplayedOrDisagree(t *testing.T) {
	t.Parallel()
	s := newService(t)
	ticketedEvent := uuid.New()
	first, second, third := uuid.New(), uuid.New(), uuid.New()
	firstConfirmedEvent := uuid.New()

	s.mustDeliver("BookingConfirmed", bookingOutcome(firstConfirmedEvent, first, ticketedEvent))
	s.mustDeliver("BookingConfirmed", bookingOutcome(uuid.New(), second, ticketedEvent))
	s.mustDeliver("BookingCancelled", bookingOutcome(uuid.New(), third, ticketedEvent))
	if got := s.stats(ticketedEvent); got.Confirmed != 2 || got.Cancelled != 1 {
		t.Fatalf("stats = %+v, want 2 confirmed 1 cancelled", got)
	}

	s.mustDeliver("BookingConfirmed", bookingOutcome(firstConfirmedEvent, first, ticketedEvent))
	s.mustDeliver("BookingCancelled", bookingOutcome(uuid.New(), first, ticketedEvent))

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
		name    string
		payload any
		wantErr error
	}{
		{"payload that is not json", []byte("{{ nope"), nil},
		{"user id that is not a uuid", map[string]any{"event_id": uuid.NewString(), "user_id": "42", "email": "ada@example.com", "created_at": "2026-03-04T08:30:00Z"}, nil},
		{"email the domain rejects", userCreated(uuid.New(), userID, "not-an-email"), domain.ErrInvalidUserRegistration},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.deliver("UserCreated", tc.payload)

			if err == nil || (tc.wantErr != nil && !errors.Is(err, tc.wantErr)) {
				t.Fatalf("err = %v, want a rejection (%v)", err, tc.wantErr)
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

func TestWhenTheDatabaseIsGoneEventsFailRetryablyAndReadinessFails(t *testing.T) {
	t.Parallel()
	s := newService(t)
	s.closePool()

	_, err := s.deliver("UserCreated", userCreated(uuid.New(), uuid.New(), "ada@example.com"))

	var repoErr *domain.RepositoryError
	if !errors.As(err, &repoErr) {
		t.Fatalf("err = %v, want a RepositoryError so the consumer retries", err)
	}
	if status, _, _ := s.get("/readyz"); status != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503", status)
	}
	if status, _, _ := s.get("/api/v1/analytics/users/" + uuid.NewString()); status != http.StatusInternalServerError {
		t.Fatalf("GET user = %d, want 500", status)
	}
}
