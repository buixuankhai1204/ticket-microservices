//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	httpadapter "github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/http"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/repository/postgres"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/usecase"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	repo := postgres.New()
	h := httpadapter.NewHandler(
		usecase.NewGetEventStatsUseCase(testPool, repo),
		usecase.NewGetUserRegistrationUseCase(testPool, repo),
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

func TestGetEventStats_SeededRows_Returns200WithExactBodyShape(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)
	ctx := context.Background()

	eventID := uuid.New()
	otherEventID := uuid.New()

	seedOutcome(t, ctx, eventID, "confirmed")
	seedOutcome(t, ctx, eventID, "confirmed")
	seedOutcome(t, ctx, eventID, "confirmed")
	seedOutcome(t, ctx, eventID, "cancelled")
	seedOutcome(t, ctx, eventID, "cancelled")
	seedOutcome(t, ctx, otherEventID, "confirmed")
	seedOutcome(t, ctx, otherEventID, "cancelled")

	status, body := getJSON(t, srv.URL+"/api/v1/analytics/events/"+eventID.String())
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", status, body)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("unmarshal body %s: %v", body, err)
	}
	assertExactKeys(t, raw, "event_id", "confirmed", "cancelled")

	var got httpadapter.EventStatsResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal into EventStatsResponse: %v", err)
	}
	if got.EventID != eventID {
		t.Errorf("event_id = %s, want %s", got.EventID, eventID)
	}
	if got.Confirmed != 3 {
		t.Errorf("confirmed = %d, want 3", got.Confirmed)
	}
	if got.Cancelled != 2 {
		t.Errorf("cancelled = %d, want 2", got.Cancelled)
	}
}

func TestGetEventStats_UnknownEventID_Returns200WithZeroCounts(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)
	ctx := context.Background()

	other := uuid.New()
	seedOutcome(t, ctx, other, "confirmed")
	seedOutcome(t, ctx, other, "cancelled")

	unknown := uuid.New()
	status, body := getJSON(t, srv.URL+"/api/v1/analytics/events/"+unknown.String())
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", status, body)
	}

	var got httpadapter.EventStatsResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.EventID != unknown || got.Confirmed != 0 || got.Cancelled != 0 {
		t.Errorf("got %+v, want {EventID:%s Confirmed:0 Cancelled:0}", got, unknown)
	}
}

func TestGetEventStats_NonUUIDPathParam_Returns400(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)

	status, body := getJSON(t, srv.URL+"/api/v1/analytics/events/not-a-uuid")
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", status, body)
	}
	var errResp httpadapter.ErrorResponse
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("unmarshal error envelope: %v", err)
	}
	if errResp.Error == "" {
		t.Errorf("expected non-empty error message, got %s", body)
	}
}

func TestGetUserRegistration_SeededRow_Returns200WithExactBodyShape(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)
	ctx := context.Background()

	userID := uuid.New()
	registeredAt := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Microsecond)
	recordedAt := time.Now().UTC().Add(-47 * time.Hour).Truncate(time.Microsecond)

	if _, err := testPool.Exec(ctx,
		`INSERT INTO user_registrations (user_id, email, registered_at, recorded_at)
		 VALUES ($1, $2, $3, $4)`,
		userID, "alice@example.com", registeredAt, recordedAt,
	); err != nil {
		t.Fatalf("seed user_registrations: %v", err)
	}

	status, body := getJSON(t, srv.URL+"/api/v1/analytics/users/"+userID.String())
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", status, body)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("unmarshal body %s: %v", body, err)
	}
	assertExactKeys(t, raw, "user_id", "email", "registered_at", "recorded_at")

	var got httpadapter.UserRegistrationResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal into UserRegistrationResponse: %v", err)
	}
	if got.UserID != userID {
		t.Errorf("user_id = %s, want %s", got.UserID, userID)
	}
	if got.Email != "alice@example.com" {
		t.Errorf("email = %q, want alice@example.com", got.Email)
	}
	if !got.RegisteredAt.Equal(registeredAt) {
		t.Errorf("registered_at = %s, want %s", got.RegisteredAt, registeredAt)
	}
	if !got.RecordedAt.Equal(recordedAt) {
		t.Errorf("recorded_at = %s, want %s", got.RecordedAt, recordedAt)
	}
}

func TestGetUserRegistration_NonUUIDPathParam_Returns400(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)

	status, body := getJSON(t, srv.URL+"/api/v1/analytics/users/12345")
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", status, body)
	}
}

func TestGetUserRegistration_WellFormedUUIDNoRow_Returns404(t *testing.T) {
	truncateAll(t)
	srv := newTestServer(t)

	missing := uuid.New()
	status, body := getJSON(t, srv.URL+"/api/v1/analytics/users/"+missing.String())
	if status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body = %s", status, body)
	}
	var errResp httpadapter.ErrorResponse
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("unmarshal error envelope: %v", err)
	}
	if errResp.Error != "not found" {
		t.Errorf("error = %q, want \"not found\"", errResp.Error)
	}
}

func seedOutcome(t *testing.T, ctx context.Context, eventID uuid.UUID, status string) {
	t.Helper()
	_, err := testPool.Exec(ctx,
		`INSERT INTO booking_outcomes (id, booking_id, event_id, status, occurred_at, recorded_at)
		 VALUES ($1, $2, $3, $4, now(), now())`,
		uuid.New(), uuid.New(), eventID, status,
	)
	if err != nil {
		t.Fatalf("seed booking_outcomes (%s): %v", status, err)
	}
}

func assertExactKeys(t *testing.T, m map[string]json.RawMessage, want ...string) {
	t.Helper()
	if len(m) != len(want) {
		t.Fatalf("body has keys %v, want exactly %v", keysOf(m), want)
	}
	for _, k := range want {
		if _, ok := m[k]; !ok {
			t.Fatalf("body missing key %q; has %v", k, keysOf(m))
		}
	}
}

func keysOf(m map[string]json.RawMessage) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}
