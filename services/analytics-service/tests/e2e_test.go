//go:build e2e

package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/testsupport"
)

func gateway(t *testing.T) string {
	t.Helper()
	base := os.Getenv("E2E_BASE_URL")
	if base == "" {
		t.Fatal("set E2E_BASE_URL to the Kong gateway of a running stack, e.g. http://localhost:8000")
	}
	return strings.TrimRight(base, "/")
}

func call(t *testing.T, method, url, token string, body any) (int, []byte) {
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
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer res.Body.Close()
	out, _ := io.ReadAll(res.Body)
	return res.StatusCode, out
}

type customer struct {
	id    uuid.UUID
	email string
	token string
}

func signUpAndLogIn(t *testing.T, base string) customer {
	t.Helper()
	email := fmt.Sprintf("e2e-%s@example.com", uuid.NewString()[:8])
	credentials := map[string]string{"email": email, "password": "correct-horse-battery-staple"}

	status, body := call(t, http.MethodPost, base+"/api/v1/auth/register", "", credentials)
	if status != http.StatusCreated {
		t.Fatalf("register = %d %s, want 201", status, body)
	}
	var registered struct {
		ID uuid.UUID `json:"id"`
	}
	if err := json.Unmarshal(body, &registered); err != nil || registered.ID == uuid.Nil {
		t.Fatalf("register response %s has no id", body)
	}

	status, body = call(t, http.MethodPost, base+"/api/v1/auth/login", "", credentials)
	if status != http.StatusOK {
		t.Fatalf("login = %d %s, want 200", status, body)
	}
	var session struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(body, &session); err != nil || session.Token == "" {
		t.Fatalf("login response %s has no token", body)
	}
	return customer{id: registered.ID, email: email, token: session.Token}
}

func TestAUserWhoSignsUpAppearsInAnalyticsAfterTheEventHasTravelledThroughKafka(t *testing.T) {
	base := gateway(t)
	user := signUpAndLogIn(t, base)
	path := base + "/api/v1/analytics/users/" + user.id.String()

	if status, _ := call(t, http.MethodGet, path, "", nil); status != http.StatusUnauthorized {
		t.Fatalf("GET without a token = %d, want 401 from the gateway", status)
	}

	var projected struct {
		UserID uuid.UUID `json:"user_id"`
		Email  string    `json:"email"`
	}
	testsupport.Eventually(t, 60*time.Second, "the registration to be projected into analytics", func() bool {
		status, body := call(t, http.MethodGet, path, user.token, nil)
		return status == http.StatusOK && json.Unmarshal(body, &projected) == nil
	})
	if projected.UserID != user.id || projected.Email != user.email {
		t.Fatalf("projection = %+v, want user %s with email %s", projected, user.id, user.email)
	}
}

func TestEventStatsForAnEventNobodyBookedAreZeroThroughTheGateway(t *testing.T) {
	base := gateway(t)
	user := signUpAndLogIn(t, base)
	eventID := uuid.New()

	status, body := call(t, http.MethodGet, base+"/api/v1/analytics/events/"+eventID.String(), user.token, nil)

	var stats struct {
		EventID   uuid.UUID `json:"event_id"`
		Confirmed int       `json:"confirmed"`
		Cancelled int       `json:"cancelled"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &stats) != nil || stats.EventID != eventID || stats.Confirmed != 0 || stats.Cancelled != 0 {
		t.Fatalf("stats = %d %s, want 200 with zero counts", status, body)
	}
}
