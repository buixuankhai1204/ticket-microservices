//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	httpadapter "github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/adapter/http"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/adapter/repository/postgres"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/usecase"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	repo := postgres.New()
	h := httpadapter.NewHandler(
		usecase.NewListEventsUseCase(testPool, repo),
		usecase.NewGetEventUseCase(testPool, repo),
		usecase.NewListEventSeatsUseCase(testPool, repo),
		usecase.NewCreateNewEventUseCase(testPool, repo),
	)
	health := httpadapter.NewHealthHandler(testPool)
	router := httpadapter.NewRouter(h, health,
		httpadapter.RequestID(),
		httpadapter.AccessLog(noopLogger{}),
	)
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)
	return srv
}

func getJSON(t *testing.T, url string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, body
}

func postEvent(t *testing.T, srv *httptest.Server, req httpadapter.CreateEventRequest) (int, []byte) {
	t.Helper()
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal create request: %v", err)
	}
	resp, err := http.Post(srv.URL+"/api/v1/events", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST /api/v1/events: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, body
}

func int64p(v int64) *int64 { return &v }

func baseEventReq(name string) httpadapter.CreateEventRequest {
	start := time.Now().UTC().Add(24 * time.Hour)
	return httpadapter.CreateEventRequest{
		Name:        name,
		Description: "desc for " + name,
		Venue:       "Test Arena",
		StartsAt:    start,
		EndsAt:      start.Add(3 * time.Hour),
		Layout: httpadapter.LayoutRequest{
			Sections: []httpadapter.SectionRequest{
				{Name: "S", Rows: 1, SeatsPerRow: 1, PriceMinor: 1000},
			},
		},
	}
}

func assertExactKeys(t *testing.T, m map[string]json.RawMessage, want ...string) {
	t.Helper()
	if len(m) != len(want) {
		t.Fatalf("object has keys %v, want exactly %v", keysOf(m), want)
	}
	for _, k := range want {
		if _, ok := m[k]; !ok {
			t.Fatalf("object missing key %q; has %v", k, keysOf(m))
		}
	}
}

func keysOf(m map[string]json.RawMessage) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

type dbSeat struct {
	Section    string
	Row        string
	Number     string
	Status     string
	PriceMinor int64
}

func seatsForEvent(t *testing.T, eventID uuid.UUID) []dbSeat {
	t.Helper()
	rows, err := testPool.Query(context.Background(),
		`SELECT section, "row", number, status, price_minor
		   FROM seats WHERE event_id = $1
		   ORDER BY section, "row", number`, eventID)
	if err != nil {
		t.Fatalf("query seats: %v", err)
	}
	defer rows.Close()
	var out []dbSeat
	for rows.Next() {
		var s dbSeat
		if err := rows.Scan(&s.Section, &s.Row, &s.Number, &s.Status, &s.PriceMinor); err != nil {
			t.Fatalf("scan seat: %v", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate seats: %v", err)
	}
	return out
}

func countRows(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := testPool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

func TestCreateEvent_LayoutExpansion_PersistedToSeatsTable(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)

	req := baseEventReq("Expansion Show")
	req.Layout = httpadapter.LayoutRequest{
		Sections: []httpadapter.SectionRequest{
			{Name: "A", Rows: 3, SeatsPerRow: 3, PriceMinor: 5000},
			{Name: "B", Rows: 2, SeatsPerRow: 4, PriceMinor: 3000},
		},
		Exceptions: []httpadapter.ExceptionRequest{
			{Section: "A", Row: "2", Number: "2", Remove: true},
			{Section: "B", Row: "1", Number: "1", PriceMinor: int64p(9999)},
		},
	}

	status, body := postEvent(t, srv, req)
	if status != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body = %s", status, body)
	}

	var created httpadapter.CreateEventResponse
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("unmarshal CreateEventResponse: %v", err)
	}
	if created.SeatCount != 16 {
		t.Errorf("seat_count = %d, want 16 (17 generated - 1 removed)", created.SeatCount)
	}
	if created.Event.ID == uuid.Nil {
		t.Fatalf("response event.id is the nil UUID")
	}

	if n := countRows(t, `SELECT COUNT(*) FROM events WHERE id = $1`, created.Event.ID); n != 1 {
		t.Fatalf("events rows for returned id = %d, want 1", n)
	}

	seats := seatsForEvent(t, created.Event.ID)
	if len(seats) != 16 {
		t.Fatalf("persisted seats = %d, want 16", len(seats))
	}

	for _, s := range seats {
		if s.Status != domain.SeatAvailable {
			t.Errorf("seat %s/%s/%s status = %q, want %q", s.Section, s.Row, s.Number, s.Status, domain.SeatAvailable)
		}
		if s.Section == "A" && s.Row == "2" && s.Number == "2" {
			t.Errorf("removed seat A/2/2 is still present")
		}
		switch {
		case s.Section == "B" && s.Row == "1" && s.Number == "1":
			if s.PriceMinor != 9999 {
				t.Errorf("repriced seat B/1/1 price_minor = %d, want 9999", s.PriceMinor)
			}
		case s.Section == "A":
			if s.PriceMinor != 5000 {
				t.Errorf("seat %s/%s/%s price_minor = %d, want section price 5000", s.Section, s.Row, s.Number, s.PriceMinor)
			}
		case s.Section == "B":
			if s.PriceMinor != 3000 {
				t.Errorf("seat %s/%s/%s price_minor = %d, want section price 3000", s.Section, s.Row, s.Number, s.PriceMinor)
			}
		default:
			t.Errorf("unexpected section %q", s.Section)
		}
	}

	if n := countRows(t, `SELECT COUNT(*) FROM seats WHERE event_id = $1 AND section = 'A'`, created.Event.ID); n != 8 {
		t.Errorf("section A seats = %d, want 8", n)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM seats WHERE event_id = $1 AND section = 'B'`, created.Event.ID); n != 8 {
		t.Errorf("section B seats = %d, want 8", n)
	}
}

func TestCreateEvent_ResponseEnvelopeShape(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)

	req := baseEventReq("Envelope Show")
	status, body := postEvent(t, srv, req)
	if status != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body = %s", status, body)
	}

	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		t.Fatalf("unmarshal top-level: %v", err)
	}
	assertExactKeys(t, top, "event", "seat_count")

	var seatCount int
	if err := json.Unmarshal(top["seat_count"], &seatCount); err != nil {
		t.Fatalf("seat_count not a number: %v", err)
	}
	if seatCount != 1 {
		t.Errorf("seat_count = %d, want 1", seatCount)
	}

	var ev map[string]json.RawMessage
	if err := json.Unmarshal(top["event"], &ev); err != nil {
		t.Fatalf("unmarshal event object: %v", err)
	}
	assertExactKeys(t, ev, "id", "name", "description", "venue", "starts_at", "ends_at", "created_at")

	var got httpadapter.EventResponse
	if err := json.Unmarshal(top["event"], &got); err != nil {
		t.Fatalf("unmarshal EventResponse: %v", err)
	}
	if got.Name != req.Name || got.Venue != req.Venue || got.Description != req.Description {
		t.Errorf("event fields = %+v, want name/venue/description from request %+v", got, req)
	}
	if !got.StartsAt.Equal(req.StartsAt) || !got.EndsAt.Equal(req.EndsAt) {
		t.Errorf("starts_at/ends_at = %s/%s, want %s/%s", got.StartsAt, got.EndsAt, req.StartsAt, req.EndsAt)
	}
	if got.CreatedAt.IsZero() {
		t.Errorf("created_at is zero")
	}
}

func TestCreateEventWithSeats_DuplicateSeatPosition_RollsBackWholeTx(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()
	repo := postgres.New()

	ev, err := domain.NewEvent("Atomic Dup", "", "Test Arena",
		time.Now().UTC().Add(24*time.Hour), time.Now().UTC().Add(27*time.Hour))
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}

	seats := []domain.Seat{
		{ID: uuid.New(), EventID: ev.ID, Section: "A", Row: "1", Number: "1", Status: domain.SeatAvailable, PriceMinor: 1000},
		{ID: uuid.New(), EventID: ev.ID, Section: "A", Row: "1", Number: "1", Status: domain.SeatAvailable, PriceMinor: 2000},
	}

	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	err = repo.CreateEventWithSeats(ctx, tx, *ev, seats)
	_ = tx.Rollback(ctx)
	if err == nil {
		t.Fatalf("CreateEventWithSeats returned nil, want a constraint error")
	}
	var repoErr *domain.RepositoryError
	if !errors.As(err, &repoErr) {
		t.Errorf("error = %v, want it to wrap *domain.RepositoryError", err)
	}

	assertNothingPersisted(t, ev.ID)
}

func TestCreateEventWithSeats_NegativePrice_RollsBackWholeTx(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()
	repo := postgres.New()

	ev, err := domain.NewEvent("Atomic Neg", "", "Test Arena",
		time.Now().UTC().Add(24*time.Hour), time.Now().UTC().Add(27*time.Hour))
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}

	seats := []domain.Seat{
		{ID: uuid.New(), EventID: ev.ID, Section: "A", Row: "1", Number: "1", Status: domain.SeatAvailable, PriceMinor: 1000},
		{ID: uuid.New(), EventID: ev.ID, Section: "A", Row: "1", Number: "2", Status: domain.SeatAvailable, PriceMinor: -1},
	}

	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	err = repo.CreateEventWithSeats(ctx, tx, *ev, seats)
	_ = tx.Rollback(ctx)
	if err == nil {
		t.Fatalf("CreateEventWithSeats returned nil, want a CHECK violation")
	}

	assertNothingPersisted(t, ev.ID)
}

func assertNothingPersisted(t *testing.T, eventID uuid.UUID) {
	t.Helper()
	if n := countRows(t, `SELECT COUNT(*) FROM events WHERE id = $1`, eventID); n != 0 {
		t.Errorf("events rows after failed create = %d, want 0 (orphan event left behind)", n)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM seats WHERE event_id = $1`, eventID); n != 0 {
		t.Errorf("seats rows after failed create = %d, want 0", n)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM events`); n != 0 {
		t.Errorf("total events rows = %d, want 0", n)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM seats`); n != 0 {
		t.Errorf("total seats rows = %d, want 0", n)
	}
}

func TestCreateEvent_ConcurrentRequests_NoLostOrCrossedRows(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)

	const n = 20
	const seatsPerEvent = 6

	type result struct {
		status int
		id     uuid.UUID
		body   []byte
	}
	results := make([]result, n)

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			req := baseEventReq(fmt.Sprintf("Concurrent Event %02d", i))
			req.Layout = httpadapter.LayoutRequest{
				Sections: []httpadapter.SectionRequest{
					{Name: "X", Rows: 2, SeatsPerRow: 3, PriceMinor: int64(1000 + i)},
				},
			}
			status, body := postEvent(t, srv, req)
			r := result{status: status, body: body}
			if status == http.StatusCreated {
				var created httpadapter.CreateEventResponse
				if err := json.Unmarshal(body, &created); err != nil {
					t.Errorf("goroutine %d: unmarshal: %v", i, err)
				}
				r.id = created.Event.ID
			}
			results[i] = r
		}(i)
	}
	wg.Wait()

	seen := make(map[uuid.UUID]bool)
	for i, r := range results {
		if r.status != http.StatusCreated {
			t.Errorf("request %d: status = %d, want 201; body = %s", i, r.status, r.body)
			continue
		}
		if r.id == uuid.Nil {
			t.Errorf("request %d: nil event id", i)
			continue
		}
		if seen[r.id] {
			t.Errorf("request %d: duplicate event id %s across responses", i, r.id)
		}
		seen[r.id] = true
	}
	if len(seen) != n {
		t.Fatalf("distinct created event ids = %d, want %d", len(seen), n)
	}

	if got := countRows(t, `SELECT COUNT(*) FROM events`); got != n {
		t.Errorf("total events rows = %d, want %d", got, n)
	}
	if got := countRows(t, `SELECT COUNT(*) FROM seats`); got != n*seatsPerEvent {
		t.Errorf("total seats rows = %d, want %d", got, n*seatsPerEvent)
	}

	for i, r := range results {
		if r.status != http.StatusCreated {
			continue
		}
		seats := seatsForEvent(t, r.id)
		if len(seats) != seatsPerEvent {
			t.Errorf("event %d (%s): %d seats, want %d", i, r.id, len(seats), seatsPerEvent)
			continue
		}
		wantPrice := int64(1000 + i)
		positions := make(map[string]bool)
		for _, s := range seats {
			if s.Section != "X" {
				t.Errorf("event %d: seat in section %q, want X", i, s.Section)
			}
			if s.PriceMinor != wantPrice {
				t.Errorf("event %d: seat %s/%s price_minor = %d, want %d (cross-contaminated)", i, s.Row, s.Number, s.PriceMinor, wantPrice)
			}
			if s.Status != domain.SeatAvailable {
				t.Errorf("event %d: seat status = %q, want available", i, s.Status)
			}
			positions[s.Row+"/"+s.Number] = true
		}
		for row := 1; row <= 2; row++ {
			for num := 1; num <= 3; num++ {
				key := fmt.Sprintf("%d/%d", row, num)
				if !positions[key] {
					t.Errorf("event %d: missing seat X/%s", i, key)
				}
			}
		}
	}
}

type createdEvent struct {
	ID        uuid.UUID
	CreatedAt time.Time
}

func seedEvents(t *testing.T, srv *httptest.Server, n int) []createdEvent {
	t.Helper()
	out := make([]createdEvent, 0, n)
	for i := 0; i < n; i++ {
		status, body := postEvent(t, srv, baseEventReq(fmt.Sprintf("Paged Event %02d", i)))
		if status != http.StatusCreated {
			t.Fatalf("seed event %d: status %d; body %s", i, status, body)
		}
		var created httpadapter.CreateEventResponse
		if err := json.Unmarshal(body, &created); err != nil {
			t.Fatalf("seed event %d: unmarshal: %v", i, err)
		}
		out = append(out, createdEvent{ID: created.Event.ID, CreatedAt: created.Event.CreatedAt})
		time.Sleep(3 * time.Millisecond)
	}
	return out
}

func decodeEvents(t *testing.T, body []byte) httpadapter.PaginatedEventsResponse {
	t.Helper()
	var page httpadapter.PaginatedEventsResponse
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatalf("unmarshal PaginatedEventsResponse from %s: %v", body, err)
	}
	return page
}

func TestListEvents_PageThrough_TotalStableOrderNewestFirstNoGaps(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)

	const total = 7
	seeded := seedEvents(t, srv, total)

	want := append([]createdEvent(nil), seeded...)
	sort.Slice(want, func(i, j int) bool {
		if !want[i].CreatedAt.Equal(want[j].CreatedAt) {
			return want[i].CreatedAt.After(want[j].CreatedAt)
		}
		return bytes.Compare(want[i].ID[:], want[j].ID[:]) > 0
	})

	var gotOrder []uuid.UUID
	seen := make(map[uuid.UUID]bool)

	offsets := []int{0, 3, 6}
	wantPageLen := []int{3, 3, 1}
	wantMore := []bool{true, true, false}
	for i, off := range offsets {
		status, body := getJSON(t, fmt.Sprintf("%s/api/v1/events?limit=3&offset=%d", srv.URL, off))
		if status != http.StatusOK {
			t.Fatalf("page offset=%d: status %d; body %s", off, status, body)
		}
		page := decodeEvents(t, body)

		if page.Pagination.Total != total {
			t.Errorf("page offset=%d: pagination.total = %d, want %d", off, page.Pagination.Total, total)
		}
		if page.Pagination.Limit != 3 || page.Pagination.Offset != off {
			t.Errorf("page offset=%d: pagination limit/offset = %d/%d, want 3/%d", off, page.Pagination.Limit, page.Pagination.Offset, off)
		}
		if len(page.Data) != wantPageLen[i] {
			t.Errorf("page offset=%d: len(data) = %d, want %d", off, len(page.Data), wantPageLen[i])
		}
		if page.Pagination.HasMore != wantMore[i] {
			t.Errorf("page offset=%d: has_more = %v, want %v", off, page.Pagination.HasMore, wantMore[i])
		}
		for _, e := range page.Data {
			if seen[e.ID] {
				t.Errorf("event id %s appeared on more than one page", e.ID)
			}
			seen[e.ID] = true
			gotOrder = append(gotOrder, e.ID)
		}
	}

	if len(seen) != total {
		t.Fatalf("union of page data has %d distinct ids, want %d", len(seen), total)
	}
	for _, ce := range seeded {
		if !seen[ce.ID] {
			t.Errorf("seeded event %s never appeared in any page", ce.ID)
		}
	}

	if len(gotOrder) != len(want) {
		t.Fatalf("concatenated pages have %d ids, want %d", len(gotOrder), len(want))
	}
	for i := range want {
		if gotOrder[i] != want[i].ID {
			t.Errorf("position %d: got %s, want %s (order is not created_at DESC, id DESC)", i, gotOrder[i], want[i].ID)
		}
	}
}

func TestListEvents_LimitAbsent_DefaultsTo20(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)

	status, body := getJSON(t, srv.URL+"/api/v1/events")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", status, body)
	}
	page := decodeEvents(t, body)
	if page.Pagination.Limit != 20 {
		t.Errorf("pagination.limit = %d, want default 20", page.Pagination.Limit)
	}
	if page.Pagination.Offset != 0 {
		t.Errorf("pagination.offset = %d, want default 0", page.Pagination.Offset)
	}
}

func TestListEvents_InvalidLimit_Returns400(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)

	for _, bad := range []string{"-1", "abc"} {
		status, body := getJSON(t, srv.URL+"/api/v1/events?limit="+bad)
		if status != http.StatusBadRequest {
			t.Errorf("limit=%s: status = %d, want 400; body = %s", bad, status, body)
		}
		var errResp httpadapter.ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			t.Errorf("limit=%s: unmarshal error envelope: %v", bad, err)
		} else if errResp.Error == "" {
			t.Errorf("limit=%s: expected non-empty error message", bad)
		}
	}
}

func TestListEvents_LimitAboveMax_ClampedTo100(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)

	status, body := getJSON(t, srv.URL+"/api/v1/events?limit=99999")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", status, body)
	}
	page := decodeEvents(t, body)
	if page.Pagination.Limit != 100 {
		t.Errorf("pagination.limit = %d, want clamped to 100", page.Pagination.Limit)
	}
}

func TestGetEvent_ByID_Returns200WithMatchingFields(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)

	status, body := postEvent(t, srv, baseEventReq("Fetch Me"))
	if status != http.StatusCreated {
		t.Fatalf("create: status %d; body %s", status, body)
	}
	var created httpadapter.CreateEventResponse
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}

	status, body = getJSON(t, srv.URL+"/api/v1/events/"+created.Event.ID.String())
	if status != http.StatusOK {
		t.Fatalf("get: status = %d, want 200; body = %s", status, body)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("unmarshal get body: %v", err)
	}
	assertExactKeys(t, raw, "id", "name", "description", "venue", "starts_at", "ends_at", "created_at")

	var got httpadapter.EventResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal EventResponse: %v", err)
	}
	if got.ID != created.Event.ID {
		t.Errorf("id = %s, want %s", got.ID, created.Event.ID)
	}
	if got.Name != created.Event.Name || got.Venue != created.Event.Venue || got.Description != created.Event.Description {
		t.Errorf("fields = %+v, want %+v", got, created.Event)
	}
	if !got.StartsAt.Equal(created.Event.StartsAt) || !got.EndsAt.Equal(created.Event.EndsAt) {
		t.Errorf("starts_at/ends_at = %s/%s, want %s/%s", got.StartsAt, got.EndsAt, created.Event.StartsAt, created.Event.EndsAt)
	}
	if !got.CreatedAt.Equal(created.Event.CreatedAt) {
		t.Errorf("created_at = %s, want %s", got.CreatedAt, created.Event.CreatedAt)
	}
}

func TestListEventSeats_PageThrough_TotalMatchesAndOrdered(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)

	req := baseEventReq("Seat Map Show")
	req.Layout = httpadapter.LayoutRequest{
		Sections: []httpadapter.SectionRequest{
			{Name: "A", Rows: 2, SeatsPerRow: 3, PriceMinor: 1000},
			{Name: "B", Rows: 1, SeatsPerRow: 2, PriceMinor: 500},
		},
	}
	status, body := postEvent(t, srv, req)
	if status != http.StatusCreated {
		t.Fatalf("create: status %d; body %s", status, body)
	}
	var created httpadapter.CreateEventResponse
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}
	const totalSeats = 8

	var gotTuples [][3]string
	seen := make(map[uuid.UUID]bool)
	offsets := []int{0, 5}
	wantLen := []int{5, 3}
	wantMore := []bool{true, false}
	for i, off := range offsets {
		status, body := getJSON(t, fmt.Sprintf("%s/api/v1/events/%s/seats?limit=5&offset=%d", srv.URL, created.Event.ID, off))
		if status != http.StatusOK {
			t.Fatalf("seats offset=%d: status %d; body %s", off, status, body)
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(body, &raw); err != nil {
			t.Fatalf("seats offset=%d: unmarshal envelope: %v", off, err)
		}
		assertExactKeys(t, raw, "data", "pagination")

		var page httpadapter.PaginatedSeatsResponse
		if err := json.Unmarshal(body, &page); err != nil {
			t.Fatalf("seats offset=%d: unmarshal PaginatedSeatsResponse: %v", off, err)
		}
		if page.Pagination.Total != totalSeats {
			t.Errorf("seats offset=%d: total = %d, want %d", off, page.Pagination.Total, totalSeats)
		}
		if len(page.Data) != wantLen[i] {
			t.Errorf("seats offset=%d: len(data) = %d, want %d", off, len(page.Data), wantLen[i])
		}
		if page.Pagination.HasMore != wantMore[i] {
			t.Errorf("seats offset=%d: has_more = %v, want %v", off, page.Pagination.HasMore, wantMore[i])
		}
		for _, s := range page.Data {
			if s.EventID != created.Event.ID {
				t.Errorf("seat %s belongs to event %s, want %s", s.ID, s.EventID, created.Event.ID)
			}
			if seen[s.ID] {
				t.Errorf("seat id %s appeared on more than one page", s.ID)
			}
			seen[s.ID] = true
			gotTuples = append(gotTuples, [3]string{s.Section, s.Row, s.Number})
		}
	}

	if len(seen) != totalSeats {
		t.Fatalf("union of seat pages = %d distinct ids, want %d", len(seen), totalSeats)
	}

	for i := 1; i < len(gotTuples); i++ {
		prev, cur := gotTuples[i-1], gotTuples[i]
		if tupleLess(cur, prev) {
			t.Errorf("seat order broken at %d: %v came after %v", i, cur, prev)
		}
	}
}

func tupleLess(a, b [3]string) bool {
	for i := 0; i < 3; i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

func TestGetEvent_RandomUUID_Returns404(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)

	missing := uuid.New()
	status, body := getJSON(t, srv.URL+"/api/v1/events/"+missing.String())
	if status != http.StatusNotFound {
		t.Fatalf("GET event: status = %d, want 404; body = %s", status, body)
	}
	var errResp httpadapter.ErrorResponse
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("unmarshal error envelope: %v", err)
	}
	if errResp.Error != "not found" {
		t.Errorf("error = %q, want \"not found\"", errResp.Error)
	}
}

func TestListEventSeats_RandomUUID_Returns404_NoSuchEvent(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)

	missing := uuid.New()
	status, body := getJSON(t, srv.URL+"/api/v1/events/"+missing.String()+"/seats")
	if status != http.StatusNotFound {
		t.Fatalf("GET seats: status = %d, want 404; body = %s", status, body)
	}
	var errResp httpadapter.ErrorResponse
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("unmarshal error envelope: %v", err)
	}
	if errResp.Error != "not found" {
		t.Errorf("error = %q, want \"not found\"", errResp.Error)
	}
}

func TestGetEvent_NonUUIDPathSegment_Returns400(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)

	for _, path := range []string{
		"/api/v1/events/not-a-uuid",
		"/api/v1/events/12345/seats",
	} {
		status, body := getJSON(t, srv.URL+path)
		if status != http.StatusBadRequest {
			t.Errorf("GET %s: status = %d, want 400; body = %s", path, status, body)
		}
		var errResp httpadapter.ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			t.Errorf("GET %s: unmarshal error envelope: %v", path, err)
		} else if errResp.Error == "" {
			t.Errorf("GET %s: expected non-empty error message", path)
		}
	}
}
