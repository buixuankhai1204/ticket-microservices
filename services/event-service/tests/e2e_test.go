//go:build e2e

package tests

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func gateway(t *testing.T) string {
	t.Helper()
	base := os.Getenv("E2E_BASE_URL")
	if base == "" {
		t.Fatal("set E2E_BASE_URL to the Kong gateway of a running stack, e.g. http://localhost:8000")
	}
	return strings.TrimRight(base, "/")
}

func call(t *testing.T, method, url string, body any) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer res.Body.Close()
	out, _ := io.ReadAll(res.Body)
	return res.StatusCode, out
}

func newEvent(name string, rows, seatsPerRow int) map[string]any {
	return map[string]any{
		"name":      name,
		"venue":     "E2E Arena",
		"starts_at": "2031-06-01T19:00:00Z",
		"ends_at":   "2031-06-01T22:00:00Z",
		"layout": map[string]any{"sections": []map[string]any{
			{"name": "A", "rows": rows, "seats_per_row": seatsPerRow, "price_minor": 5000},
		}},
	}
}

func TestAnEventCreatedThroughTheGatewayCanBeFoundAndItsSeatMapPaged(t *testing.T) {
	base := gateway(t)
	name := "e2e-" + uuid.NewString()[:8]

	status, body := call(t, http.MethodPost, base+"/api/v1/events", newEvent(name, 2, 5))
	if status != http.StatusCreated {
		t.Fatalf("create event = %d %s, want 201", status, body)
	}
	var created struct {
		Event struct {
			ID uuid.UUID `json:"id"`
		} `json:"event"`
		SeatCount int `json:"seat_count"`
	}
	if err := json.Unmarshal(body, &created); err != nil || created.Event.ID == uuid.Nil || created.SeatCount != 10 {
		t.Fatalf("create response %s, want an id and 10 seats", body)
	}
	eventURL := base + "/api/v1/events/" + created.Event.ID.String()

	status, body = call(t, http.MethodGet, eventURL, nil)
	var fetched struct {
		Name string `json:"name"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &fetched) != nil || fetched.Name != name {
		t.Fatalf("get event = %d %s", status, body)
	}

	status, body = call(t, http.MethodGet, base+"/api/v1/events?limit=5", nil)
	var listed struct {
		Data []struct {
			ID uuid.UUID `json:"id"`
		} `json:"data"`
		Pagination struct {
			Limit int `json:"limit"`
		} `json:"pagination"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &listed) != nil || listed.Pagination.Limit != 5 {
		t.Fatalf("list events = %d %s", status, body)
	}

	status, body = call(t, http.MethodGet, eventURL+"/seats?limit=4&offset=8", nil)
	var seats struct {
		Data       []struct{ Status string } `json:"data"`
		Pagination struct {
			Total   int  `json:"total"`
			HasMore bool `json:"has_more"`
		} `json:"pagination"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &seats) != nil {
		t.Fatalf("list seats = %d %s", status, body)
	}
	if len(seats.Data) != 2 || seats.Pagination.Total != 10 || seats.Pagination.HasMore || seats.Data[0].Status != "available" {
		t.Fatalf("last seat page = %s, want 2 available seats of 10 and nothing more", body)
	}
}

func TestTheGatewayPassesThroughTheServicesValidationErrors(t *testing.T) {
	base := gateway(t)

	if status, body := call(t, http.MethodPost, base+"/api/v1/events", newEvent("no seats", 0, 0)); status != http.StatusBadRequest {
		t.Fatalf("event without seats = %d %s, want 400", status, body)
	}
	if status, body := call(t, http.MethodGet, base+"/api/v1/events/"+uuid.NewString(), nil); status != http.StatusNotFound {
		t.Fatalf("unknown event = %d %s, want 404", status, body)
	}
	if status, body := call(t, http.MethodGet, base+"/api/v1/events?limit=0", nil); status != http.StatusBadRequest {
		t.Fatalf("limit=0 = %d %s, want 400", status, body)
	}
}
