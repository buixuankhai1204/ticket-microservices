//go:build component

package app_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/app"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/config"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/testsupport"
)

type service struct {
	url       string
	app       *app.App
	topic     string
	consumers []app.Consumer
}

func newService(t *testing.T, withKafka bool) *service {
	t.Helper()
	pool := testsupport.NewDatabase(t)
	cfg := config.Config{
		KafkaBrokers:             testsupport.Brokers(),
		KafkaConsumerMaxAttempts: 3,
		SeatHoldTimeoutSecs:      1,
		SeatReaperIntervalSecs:   1,
	}
	if withKafka {
		cfg.KafkaBookingEventsTopic = testsupport.NewTopic(t)
		cfg.KafkaGroupSuffix = "-" + cfg.KafkaBookingEventsTopic
	}
	a := app.New(pool, cfg, testsupport.SilentLogger{})
	srv := httptest.NewServer(a.Router)
	t.Cleanup(srv.Close)

	var consumers []app.Consumer
	if withKafka {
		consumers = a.NewConsumers()
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		for _, c := range consumers {
			wg.Add(1)
			go func(c app.Consumer) {
				defer wg.Done()
				_ = c.Run(ctx)
			}(c)
		}
		t.Cleanup(func() {
			cancel()
			wg.Wait()
			for _, c := range consumers {
				_ = c.Close()
			}
		})
	}
	return &service{url: srv.URL, app: a, topic: cfg.KafkaBookingEventsTopic, consumers: consumers}
}

func (s *service) do(t *testing.T, method, path string, body any) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, ok := body.([]byte)
		if !ok {
			var err error
			if raw, err = json.Marshal(body); err != nil {
				t.Fatal(err)
			}
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, s.url+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	out, _ := io.ReadAll(res.Body)
	return res.StatusCode, out
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
		Total   int  `json:"total"`
		HasMore bool `json:"has_more"`
	} `json:"pagination"`
}

func (s *service) createEvent(t *testing.T, rows, seatsPerRow int) uuid.UUID {
	t.Helper()
	status, body := s.do(t, http.MethodPost, "/api/v1/events", layout(rows, seatsPerRow))
	if status != http.StatusCreated {
		t.Fatalf("create event = %d %s, want 201", status, body)
	}
	return decode[struct {
		Event struct {
			ID uuid.UUID `json:"id"`
		} `json:"event"`
	}](t, body).Event.ID
}

func (s *service) seats(t *testing.T, eventID uuid.UUID) []seat {
	t.Helper()
	status, body := s.do(t, http.MethodGet, "/api/v1/events/"+eventID.String()+"/seats?limit=100", nil)
	if status != http.StatusOK {
		t.Fatalf("list seats = %d %s", status, body)
	}
	return decode[seatPage](t, body).Data
}

func (s *service) statusOf(t *testing.T, eventID, seatID uuid.UUID) string {
	t.Helper()
	for _, st := range s.seats(t, eventID) {
		if st.ID == seatID {
			return st.Status
		}
	}
	t.Fatalf("seat %s not found", seatID)
	return ""
}

func (s *service) waitStatus(t *testing.T, eventID, seatID uuid.UUID, want string) {
	t.Helper()
	testsupport.Eventually(t, 60*time.Second, "seat "+seatID.String()+" to be "+want, func() bool {
		return s.statusOf(t, eventID, seatID) == want
	})
}

func (s *service) publish(t *testing.T, eventType string, eventID, bookingID, ticketedEventID uuid.UUID, seatIDs ...uuid.UUID) {
	t.Helper()
	ids := make([]string, len(seatIDs))
	for i, id := range seatIDs {
		ids[i] = id.String()
	}
	payload := map[string]any{
		"event_id":          eventID.String(),
		"booking_id":        bookingID.String(),
		"user_id":           uuid.NewString(),
		"ticketed_event_id": ticketedEventID.String(),
		"seat_ids":          ids,
		"reason":            "user_cancelled",
		"requested_at":      time.Now().UTC().Format(time.RFC3339),
		"occurred_at":       time.Now().UTC().Format(time.RFC3339),
	}
	raw, _ := json.Marshal(payload)
	testsupport.Produce(t, s.topic, bookingID.String(), string(raw), eventType)
}

func TestACreatedEventCanBeFetchedWithItsSeatsPaginated(t *testing.T) {
	t.Parallel()
	s := newService(t, false)

	status, body := s.do(t, http.MethodPost, "/api/v1/events", layout(2, 3))
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

	status, body = s.do(t, http.MethodGet, "/api/v1/events/"+created.Event.ID.String(), nil)
	if status != http.StatusOK || decode[struct{ Name string }](t, body).Name != created.Event.Name {
		t.Fatalf("get event = %d %s", status, body)
	}

	_, body = s.do(t, http.MethodGet, "/api/v1/events/"+created.Event.ID.String()+"/seats?limit=4", nil)
	first := decode[seatPage](t, body)
	if len(first.Data) != 4 || first.Pagination.Total != 6 || !first.Pagination.HasMore {
		t.Fatalf("first page = %d rows total %d has_more %v, want 4 / 6 / true", len(first.Data), first.Pagination.Total, first.Pagination.HasMore)
	}
	_, body = s.do(t, http.MethodGet, "/api/v1/events/"+created.Event.ID.String()+"/seats?limit=4&offset=4", nil)
	last := decode[seatPage](t, body)
	if len(last.Data) != 2 || last.Pagination.HasMore {
		t.Fatalf("last page = %d rows has_more %v, want 2 / false", len(last.Data), last.Pagination.HasMore)
	}
}

func TestBadRequestsAreRejectedWithTheRightStatusAndPersistNothing(t *testing.T) {
	t.Parallel()
	s := newService(t, false)

	tests := []struct {
		name   string
		method string
		path   string
		body   any
		want   int
	}{
		{"malformed json", http.MethodPost, "/api/v1/events", []byte("{not json"), http.StatusBadRequest},
		{"empty layout", http.MethodPost, "/api/v1/events", layout(0, 0), http.StatusBadRequest},
		{"absurd layout that overflows the seat count", http.MethodPost, "/api/v1/events", layout(1<<32, 1<<32), http.StatusBadRequest},
		{"unknown event", http.MethodGet, "/api/v1/events/" + uuid.NewString(), nil, http.StatusNotFound},
		{"event id that is not a uuid", http.MethodGet, "/api/v1/events/not-a-uuid", nil, http.StatusBadRequest},
		{"page size below one", http.MethodGet, "/api/v1/events?limit=0", nil, http.StatusBadRequest},
	}
	for _, tc := range tests {
		if status, body := s.do(t, tc.method, tc.path, tc.body); status != tc.want {
			t.Fatalf("%s = %d %s, want %d", tc.name, status, body, tc.want)
		}
	}

	_, body := s.do(t, http.MethodGet, "/api/v1/events", nil)
	if total := decode[struct {
		Pagination struct{ Total int } `json:"pagination"`
	}](t, body).Pagination.Total; total != 0 {
		t.Fatalf("%d events were persisted by rejected requests, want 0", total)
	}
}

func TestAnOlderBookingRequestRedeliveredAfterACancellationIsIgnored(t *testing.T) {
	t.Parallel()
	s := newService(t, true)
	eventID := s.createEvent(t, 1, 3)
	seats := s.seats(t, eventID)
	s1, s2, s3 := seats[0].ID, seats[1].ID, seats[2].ID
	booking, request := uuid.New(), uuid.New()

	s.publish(t, "BookingRequested", request, booking, eventID, s1, s2)
	s.waitStatus(t, eventID, s1, "reserved")
	s.publish(t, "BookingCancelled", uuid.New(), booking, eventID, s1, s2)
	s.waitStatus(t, eventID, s1, "available")

	s.publish(t, "BookingRequested", request, booking, eventID, s1, s2)
	s.publish(t, "BookingRequested", uuid.New(), uuid.New(), eventID, s3)
	s.waitStatus(t, eventID, s3, "reserved")

	if got := s.statusOf(t, eventID, s1); got != "available" {
		t.Fatalf("seat %s = %s after a redelivered request, want available", s1, got)
	}
}

func TestASeatCannotBeBookedTwiceAndAFailedBookingCannotReleaseAnothersSeat(t *testing.T) {
	t.Parallel()
	s := newService(t, true)
	eventID := s.createEvent(t, 1, 3)
	seats := s.seats(t, eventID)
	s1, s2, s3 := seats[0].ID, seats[1].ID, seats[2].ID
	first, second, sentinel := uuid.New(), uuid.New(), uuid.New()

	s.publish(t, "BookingRequested", uuid.New(), first, eventID, s1)
	s.waitStatus(t, eventID, s1, "reserved")
	s.publish(t, "BookingRequested", uuid.New(), second, eventID, s1, s2)
	s.publish(t, "BookingRequested", uuid.New(), sentinel, eventID, s3)
	s.waitStatus(t, eventID, s3, "reserved")

	if got := s.statusOf(t, eventID, s2); got != "available" {
		t.Fatalf("seat %s = %s: a booking that conflicted on another seat must reserve nothing", s2, got)
	}

	s.publish(t, "BookingCancelled", uuid.New(), second, eventID, s1, s2)
	s.publish(t, "BookingCancelled", uuid.New(), sentinel, eventID, s3)
	s.waitStatus(t, eventID, s3, "available")

	if got := s.statusOf(t, eventID, s1); got != "reserved" {
		t.Fatalf("seat %s = %s: cancelling a booking that never held it must not release it", s1, got)
	}
}

func TestABookedSeatIsFinalAndACancellationCannotReleaseIt(t *testing.T) {
	t.Parallel()
	s := newService(t, true)
	eventID := s.createEvent(t, 1, 2)
	seats := s.seats(t, eventID)
	s1, s2 := seats[0].ID, seats[1].ID
	paid, sentinel := uuid.New(), uuid.New()

	s.publish(t, "BookingRequested", uuid.New(), paid, eventID, s1)
	s.waitStatus(t, eventID, s1, "reserved")
	s.publish(t, "BookingConfirmed", uuid.New(), paid, eventID, s1)
	s.waitStatus(t, eventID, s1, "booked")

	s.publish(t, "BookingRequested", uuid.New(), sentinel, eventID, s2)
	s.waitStatus(t, eventID, s2, "reserved")
	s.publish(t, "BookingCancelled", uuid.New(), paid, eventID, s1)
	s.publish(t, "BookingCancelled", uuid.New(), sentinel, eventID, s2)
	s.waitStatus(t, eventID, s2, "available")

	if got := s.statusOf(t, eventID, s1); got != "booked" {
		t.Fatalf("seat %s = %s, want booked: a paid seat must survive a late cancellation", s1, got)
	}
}

func TestTheReaperReleasesStaleHeldReservations(t *testing.T) {
	t.Parallel()
	s := newService(t, true)
	eventID := s.createEvent(t, 1, 1)
	seatID := s.seats(t, eventID)[0].ID

	s.publish(t, "BookingRequested", uuid.New(), uuid.New(), eventID, seatID)
	s.waitStatus(t, eventID, seatID, "reserved")
	time.Sleep(1500 * time.Millisecond)

	released, err := s.app.ReapHeldReservations.Execute(context.Background())
	if err != nil || released != 1 {
		t.Fatalf("reaper released %d (err %v), want 1", released, err)
	}
	if got := s.statusOf(t, eventID, seatID); got != "available" {
		t.Fatalf("seat = %s after the reaper ran, want available", got)
	}
}
