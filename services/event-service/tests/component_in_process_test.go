//go:build component

package tests

import (
	"bytes"
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
	"github.com/jackc/pgx/v5/pgxpool"

	kafka "github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/adapter/messaging/kafka"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/adapter/repository/postgres"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/app"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/config"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/usecase"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/tests/common"
)

type service struct {
	t         *testing.T
	url       string
	app       *app.App
	pool      *pgxpool.Pool
	tap       *common.OutboxTap
	requested *usecase.ReserveSeatUseCase
	confirmed *usecase.FinalizeSeatUseCase
	cancelled *usecase.ReleaseSeatUseCase
}

func newService(t *testing.T) *service {
	t.Helper()
	pool := common.NewDatabase(t)
	cfg := config.Config{KafkaBookingEventsTopic: "booking.events", SeatHoldTimeoutSecs: 3600}
	application := app.New(pool, cfg, common.SilentLogger{})
	server := httptest.NewServer(application.Router)
	t.Cleanup(server.Close)
	repo := postgres.New()
	return &service{
		t: t, url: server.URL, app: application, pool: pool, tap: common.TapOutbox(t, pool),
		requested: usecase.NewReserveSeatUseCase(pool, repo),
		confirmed: usecase.NewFinalizeSeatUseCase(pool, repo),
		cancelled: usecase.NewReleaseSeatUseCase(pool, repo),
	}
}

func (s *service) call(method, path string, body any, headers ...string) (int, http.Header, []byte) {
	s.t.Helper()
	var reader io.Reader
	if body != nil {
		raw, ok := body.([]byte)
		if !ok {
			var err error
			if raw, err = json.Marshal(body); err != nil {
				s.t.Fatal(err)
			}
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, s.url+path, reader)
	if err != nil {
		s.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer res.Body.Close()
	out, _ := io.ReadAll(res.Body)
	return res.StatusCode, res.Header, out
}

func decode[T any](t *testing.T, raw []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return v
}

func layout(rows, seatsPerRow int) map[string]any {
	return map[string]any{
		"name":      "Fest " + uuid.NewString()[:8],
		"venue":     "Park",
		"starts_at": "2030-06-01T19:00:00Z",
		"ends_at":   "2030-06-01T22:00:00Z",
		"layout": map[string]any{"sections": []map[string]any{
			{"name": "A", "rows": rows, "seats_per_row": seatsPerRow, "price_minor": 5000},
		}},
	}
}

type seat struct {
	ID     uuid.UUID `json:"id"`
	Status string    `json:"status"`
}

type seatPage struct {
	Data       []seat `json:"data"`
	Pagination struct {
		Limit   int  `json:"limit"`
		Offset  int  `json:"offset"`
		Total   int  `json:"total"`
		HasMore bool `json:"has_more"`
	} `json:"pagination"`
}

func (s *service) createEvent(rows, seatsPerRow int) uuid.UUID {
	s.t.Helper()
	status, _, body := s.call(http.MethodPost, "/api/v1/events", layout(rows, seatsPerRow))
	if status != http.StatusCreated {
		s.t.Fatalf("create event = %d %s, want 201", status, body)
	}
	return decode[struct {
		Event struct {
			ID uuid.UUID `json:"id"`
		} `json:"event"`
	}](s.t, body).Event.ID
}

func (s *service) seats(eventID uuid.UUID) []seat {
	s.t.Helper()
	status, _, body := s.call(http.MethodGet, "/api/v1/events/"+eventID.String()+"/seats?limit=100", nil)
	if status != http.StatusOK {
		s.t.Fatalf("list seats = %d %s", status, body)
	}
	return decode[seatPage](s.t, body).Data
}

func (s *service) statusOf(eventID, seatID uuid.UUID) string {
	s.t.Helper()
	for _, st := range s.seats(eventID) {
		if st.ID == seatID {
			return st.Status
		}
	}
	s.t.Fatalf("seat %s not found", seatID)
	return ""
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
	case "BookingRequested":
		ev, err := kafka.BookingRequestedSpec(nil).Parse(raw)
		if err != nil {
			return false, err
		}
		return s.requested.Execute(ctx, ev)
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

type booking struct {
	eventID, bookingID, ticketedEventID uuid.UUID
	seatIDs                             []uuid.UUID
}

func newBooking(ticketedEventID uuid.UUID, seatIDs ...uuid.UUID) booking {
	return booking{eventID: uuid.New(), bookingID: uuid.New(), ticketedEventID: ticketedEventID, seatIDs: seatIDs}
}

func (b booking) payload() map[string]any {
	ids := make([]string, len(b.seatIDs))
	for i, id := range b.seatIDs {
		ids[i] = id.String()
	}
	now := time.Now().UTC().Format(time.RFC3339)
	return map[string]any{
		"event_id": b.eventID.String(), "booking_id": b.bookingID.String(), "user_id": uuid.NewString(),
		"ticketed_event_id": b.ticketedEventID.String(), "seat_ids": ids,
		"reason": "user_cancelled", "requested_at": now, "occurred_at": now,
	}
}

func (b booking) withNewEventID() booking {
	b.eventID = uuid.New()
	return b
}

func TestACreatedEventCanBeFetchedWithItsSeatsPaginated(t *testing.T) {
	t.Parallel()
	s := newService(t)

	status, _, body := s.call(http.MethodPost, "/api/v1/events", layout(2, 3))
	if status != http.StatusCreated {
		t.Fatalf("create = %d %s, want 201", status, body)
	}
	created := decode[struct {
		Event struct {
			ID   uuid.UUID `json:"id"`
			Name string    `json:"name"`
		} `json:"event"`
		SeatCount int `json:"seat_count"`
	}](t, body)
	if created.SeatCount != 6 {
		t.Fatalf("seat_count = %d, want 6", created.SeatCount)
	}

	status, _, body = s.call(http.MethodGet, "/api/v1/events/"+created.Event.ID.String(), nil)
	if status != http.StatusOK || decode[struct{ Name string }](t, body).Name != created.Event.Name {
		t.Fatalf("get event = %d %s", status, body)
	}

	_, _, body = s.call(http.MethodGet, "/api/v1/events/"+created.Event.ID.String()+"/seats?limit=4", nil)
	first := decode[seatPage](t, body)
	if len(first.Data) != 4 || first.Pagination.Total != 6 || !first.Pagination.HasMore || first.Pagination.Limit != 4 {
		t.Fatalf("first page = %+v, want 4 rows of 6 with more to come", first.Pagination)
	}
	_, _, body = s.call(http.MethodGet, "/api/v1/events/"+created.Event.ID.String()+"/seats?limit=4&offset=4", nil)
	last := decode[seatPage](t, body)
	if len(last.Data) != 2 || last.Pagination.HasMore {
		t.Fatalf("last page = %d rows has_more %v, want 2 / false", len(last.Data), last.Pagination.HasMore)
	}
}

func TestEventsComeBackNewestFirstInAPaginatedEnvelope(t *testing.T) {
	t.Parallel()
	s := newService(t)
	for range 3 {
		s.createEvent(1, 1)
		time.Sleep(5 * time.Millisecond)
	}

	status, _, body := s.call(http.MethodGet, "/api/v1/events?limit=2", nil)

	page := decode[struct {
		Data []struct {
			CreatedAt time.Time `json:"created_at"`
		} `json:"data"`
		Pagination struct {
			Limit, Offset, Total int
			HasMore              bool `json:"has_more"`
		} `json:"pagination"`
	}](t, body)
	if status != http.StatusOK || len(page.Data) != 2 || page.Pagination.Total != 3 || !page.Pagination.HasMore {
		t.Fatalf("page = %d %s", status, body)
	}
	if !page.Data[0].CreatedAt.After(page.Data[1].CreatedAt) {
		t.Fatalf("events are not newest first: %v then %v", page.Data[0].CreatedAt, page.Data[1].CreatedAt)
	}
}

func TestBadRequestsAreRejectedWithTheRightStatusAndPersistNothing(t *testing.T) {
	t.Parallel()
	s := newService(t)

	tests := []struct {
		name   string
		method string
		path   string
		body   any
		want   int
	}{
		{"malformed json", http.MethodPost, "/api/v1/events", []byte("{not json"), http.StatusBadRequest},
		{"empty layout", http.MethodPost, "/api/v1/events", layout(0, 0), http.StatusBadRequest},
		{"layout whose seat count overflows", http.MethodPost, "/api/v1/events", layout(1<<32, 1<<32), http.StatusBadRequest},
		{"event that ends before it starts", http.MethodPost, "/api/v1/events", map[string]any{
			"name": "x", "venue": "y", "starts_at": "2030-06-01T22:00:00Z", "ends_at": "2030-06-01T19:00:00Z",
			"layout": map[string]any{"sections": []map[string]any{{"name": "A", "rows": 1, "seats_per_row": 1}}},
		}, http.StatusBadRequest},
		{"unknown event", http.MethodGet, "/api/v1/events/" + uuid.NewString(), nil, http.StatusNotFound},
		{"seats of an unknown event", http.MethodGet, "/api/v1/events/" + uuid.NewString() + "/seats", nil, http.StatusNotFound},
		{"event id that is not a uuid", http.MethodGet, "/api/v1/events/not-a-uuid", nil, http.StatusBadRequest},
		{"page size below one", http.MethodGet, "/api/v1/events?limit=0", nil, http.StatusBadRequest},
		{"page size that is not a number", http.MethodGet, "/api/v1/events?limit=ten", nil, http.StatusBadRequest},
		{"negative offset", http.MethodGet, "/api/v1/events?offset=-1", nil, http.StatusBadRequest},
		{"deleting an event", http.MethodDelete, "/api/v1/events/" + uuid.NewString(), nil, http.StatusMethodNotAllowed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if status, _, body := s.call(tc.method, tc.path, tc.body); status != tc.want {
				t.Fatalf("%s %s = %d %s, want %d", tc.method, tc.path, status, body, tc.want)
			}
		})
	}

	_, _, body := s.call(http.MethodGet, "/api/v1/events", nil)
	if total := decode[struct {
		Pagination struct{ Total int } `json:"pagination"`
	}](t, body).Pagination.Total; total != 0 {
		t.Fatalf("%d events were persisted by rejected requests, want 0", total)
	}
}

func TestABookingRequestHoldsItsSeatsAndAnnouncesSeatReserved(t *testing.T) {
	t.Parallel()
	s := newService(t)
	eventID := s.createEvent(1, 3)
	seats := s.seats(eventID)
	req := newBooking(eventID, seats[0].ID, seats[1].ID)

	s.mustDeliver("BookingRequested", req.payload())

	if s.statusOf(eventID, seats[0].ID) != "reserved" || s.statusOf(eventID, seats[1].ID) != "reserved" || s.statusOf(eventID, seats[2].ID) != "available" {
		t.Fatalf("seat statuses are wrong after the reservation")
	}
	published := s.tap.Events()
	if len(published) != 1 || published[0].EventType != "SeatReserved" || published[0].AggregateID != req.bookingID || published[0].AggregateType != "seat_reservation" {
		t.Fatalf("published = %+v, want one SeatReserved for the booking", published)
	}
	if got := published[0].Payload["seat_ids"].([]any); len(got) != 2 || got[0] != seats[0].ID.String() {
		t.Fatalf("payload seat_ids = %v", got)
	}
}

func TestASeatCannotBeBookedTwiceAndTheLoserIsToldWhyWithoutHoldingAnythingElse(t *testing.T) {
	t.Parallel()
	s := newService(t)
	eventID := s.createEvent(1, 3)
	seats := s.seats(eventID)
	winner := newBooking(eventID, seats[0].ID)
	loser := newBooking(eventID, seats[0].ID, seats[1].ID)

	s.mustDeliver("BookingRequested", winner.payload())
	s.mustDeliver("BookingRequested", loser.payload())

	published := s.tap.Events()
	if len(published) != 2 || published[1].EventType != "SeatReservationFailed" || published[1].AggregateID != loser.bookingID || published[1].Payload["reason"] != "seat_unavailable" {
		t.Fatalf("published = %+v, want SeatReserved then SeatReservationFailed(seat_unavailable)", published)
	}
	if got := s.statusOf(eventID, seats[1].ID); got != "available" {
		t.Fatalf("seat %s = %s: a booking that conflicted on another seat must hold nothing", seats[1].ID, got)
	}

	s.mustDeliver("BookingCancelled", loser.withNewEventID().payload())
	if got := s.statusOf(eventID, seats[0].ID); got != "reserved" {
		t.Fatalf("seat = %s: cancelling a booking that never held the seat must not release it", got)
	}
}

func TestRequestsThatCannotBeMatchedToRealSeatsAreAnsweredWithTheRightReason(t *testing.T) {
	t.Parallel()
	s := newService(t)
	eventID := s.createEvent(1, 1)
	otherEvent := s.createEvent(1, 1)
	foreignSeat := s.seats(otherEvent)[0].ID

	s.mustDeliver("BookingRequested", newBooking(uuid.New(), uuid.New()).payload())
	s.mustDeliver("BookingRequested", newBooking(eventID, foreignSeat).payload())

	var reasons []any
	for _, p := range s.tap.Events() {
		if p.EventType != "SeatReservationFailed" {
			t.Fatalf("published %s, want only failures", p.EventType)
		}
		reasons = append(reasons, p.Payload["reason"])
	}
	if len(reasons) != 2 || reasons[0] != "event_not_found" || reasons[1] != "seat_not_found" {
		t.Fatalf("reasons = %v, want [event_not_found seat_not_found]", reasons)
	}
}

func TestACancellationReleasesTheSeatsAndAnOlderRequestRedeliveredAfterwardsIsIgnored(t *testing.T) {
	t.Parallel()
	s := newService(t)
	eventID := s.createEvent(1, 2)
	seats := s.seats(eventID)
	req := newBooking(eventID, seats[0].ID, seats[1].ID)

	s.mustDeliver("BookingRequested", req.payload())
	s.mustDeliver("BookingCancelled", req.withNewEventID().payload())
	if got := s.statusOf(eventID, seats[0].ID); got != "available" {
		t.Fatalf("seat = %s after the cancellation, want available", got)
	}
	publishedBefore := len(s.tap.Events())

	already := s.mustDeliver("BookingRequested", req.payload())

	if !already {
		t.Fatalf("a redelivered request must be recognised as already processed")
	}
	if got := s.statusOf(eventID, seats[0].ID); got != "available" {
		t.Fatalf("seat = %s: a redelivered request must not take the seat back", got)
	}
	if len(s.tap.Events()) != publishedBefore {
		t.Fatalf("a redelivered request published something new")
	}
}

func TestAPaidSeatIsFinalAndALateCancellationIsRefusedInsteadOfReleasingIt(t *testing.T) {
	t.Parallel()
	s := newService(t)
	eventID := s.createEvent(1, 1)
	seatID := s.seats(eventID)[0].ID
	paid := newBooking(eventID, seatID)

	s.mustDeliver("BookingRequested", paid.payload())
	s.mustDeliver("BookingConfirmed", paid.withNewEventID().payload())
	if got := s.statusOf(eventID, seatID); got != "booked" {
		t.Fatalf("seat = %s after confirmation, want booked", got)
	}

	_, err := s.deliver("BookingCancelled", paid.withNewEventID().payload())

	if !errors.Is(err, domain.ErrReservationNotHeld) {
		t.Fatalf("err = %v, want ErrReservationNotHeld: a permanent rejection the consumer dead-letters", err)
	}
	if got := s.statusOf(eventID, seatID); got != "booked" {
		t.Fatalf("seat = %s: a paid seat must survive a late cancellation", got)
	}
}

func TestAConfirmationThatArrivesBeforeItsReservationFailsCleanlyAndSucceedsWhenRedelivered(t *testing.T) {
	t.Parallel()
	s := newService(t)
	eventID := s.createEvent(1, 1)
	seatID := s.seats(eventID)[0].ID
	req := newBooking(eventID, seatID)
	confirmation := req.withNewEventID()

	_, err := s.deliver("BookingConfirmed", confirmation.payload())

	var repoErr *domain.RepositoryError
	if !errors.As(err, &repoErr) {
		t.Fatalf("err = %v, want a RepositoryError so the consumer retries", err)
	}
	if got := s.statusOf(eventID, seatID); got != "available" {
		t.Fatalf("seat = %s after an early confirmation, want untouched", got)
	}

	s.mustDeliver("BookingRequested", req.payload())
	already := s.mustDeliver("BookingConfirmed", confirmation.payload())

	if already {
		t.Fatalf("the failed attempt must not have been remembered as processed")
	}
	if got := s.statusOf(eventID, seatID); got != "booked" {
		t.Fatalf("seat = %s, want booked once the confirmation is redelivered", got)
	}
}

func TestMessagesTheServiceCannotParseNeverTouchSeats(t *testing.T) {
	t.Parallel()
	s := newService(t)
	eventID := s.createEvent(1, 1)
	seatID := s.seats(eventID)[0].ID

	tests := []struct {
		name    string
		payload any
	}{
		{"payload that is not json", []byte("{{ nope")},
		{"request without any seat", newBooking(eventID).payload()},
		{"seat id that is not a uuid", map[string]any{
			"event_id": uuid.NewString(), "booking_id": uuid.NewString(), "user_id": uuid.NewString(),
			"ticketed_event_id": eventID.String(), "seat_ids": []string{"seat-1"}, "requested_at": time.Now().Format(time.RFC3339),
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.deliver("BookingRequested", tc.payload); err == nil {
				t.Fatalf("the message was accepted")
			}
		})
	}

	if got := s.statusOf(eventID, seatID); got != "available" || len(s.tap.Events()) != 0 {
		t.Fatalf("seat = %s with %d events published: none of those messages may have an effect", got, len(s.tap.Events()))
	}
}

func TestTheReaperReleasesHoldsThatOutlivedTheTimeoutAndLeavesFreshOnesAlone(t *testing.T) {
	t.Parallel()
	s := newService(t)
	eventID := s.createEvent(1, 2)
	seats := s.seats(eventID)
	stale := newBooking(eventID, seats[0].ID)
	fresh := newBooking(eventID, seats[1].ID)
	s.mustDeliver("BookingRequested", stale.payload())
	s.mustDeliver("BookingRequested", fresh.payload())
	if _, err := s.pool.Exec(context.Background(),
		`UPDATE seat_reservations SET created_at = now() - interval '2 hours' WHERE booking_id = $1`, stale.bookingID); err != nil {
		t.Fatal(err)
	}

	released, err := s.app.ReapHeldReservations.Execute(context.Background())

	if err != nil || released != 1 {
		t.Fatalf("reaper released %d (err %v), want 1", released, err)
	}
	if s.statusOf(eventID, seats[0].ID) != "available" || s.statusOf(eventID, seats[1].ID) != "reserved" {
		t.Fatalf("the stale hold must be released and the fresh one kept")
	}
	if again, _ := s.app.ReapHeldReservations.Execute(context.Background()); again != 0 {
		t.Fatalf("a second sweep released %d, want 0", again)
	}
}

func TestEveryResponseCarriesARequestIdAndTheServiceReportsItselfHealthy(t *testing.T) {
	t.Parallel()
	s := newService(t)

	_, headers, _ := s.call(http.MethodGet, "/healthz", nil, "X-Request-Id", "req-123")
	if got := headers.Get("X-Request-Id"); got != "req-123" {
		t.Fatalf("X-Request-Id = %q, want the inbound id echoed", got)
	}
	_, headers, _ = s.call(http.MethodGet, "/healthz", nil)
	if _, err := uuid.Parse(headers.Get("X-Request-Id")); err != nil {
		t.Fatalf("X-Request-Id = %q, want a generated uuid", headers.Get("X-Request-Id"))
	}
	if status, _, _ := s.call(http.MethodGet, "/readyz", nil); status != http.StatusOK {
		t.Fatalf("/readyz = %d, want 200 with the database up", status)
	}
	s.call(http.MethodGet, "/api/v1/events", nil)
	status, _, body := s.call(http.MethodGet, "/metrics", nil)
	if status != http.StatusOK || !strings.Contains(string(body), "http_requests_total") {
		t.Fatalf("/metrics = %d, want the RED counters exposed", status)
	}
}

func TestWhenTheDatabaseIsGoneRequestsFailWithA500EventsFailRetryablyAndReadinessFails(t *testing.T) {
	t.Parallel()
	s := newService(t)
	eventID := s.createEvent(1, 1)
	seatID := s.seats(eventID)[0].ID
	s.pool.Close()

	_, err := s.deliver("BookingRequested", newBooking(eventID, seatID).payload())

	var repoErr *domain.RepositoryError
	if !errors.As(err, &repoErr) {
		t.Fatalf("err = %v, want a RepositoryError so the consumer retries", err)
	}
	if status, _, _ := s.call(http.MethodGet, "/readyz", nil); status != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503", status)
	}
	if status, _, _ := s.call(http.MethodGet, "/api/v1/events", nil); status != http.StatusInternalServerError {
		t.Fatalf("GET events = %d, want 500", status)
	}
}
