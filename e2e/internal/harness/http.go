//go:build e2e

package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

// DoJSON performs one request against the gateway (never a service's own
// port -- see CLAUDE.md's Kong-only-fronts-client-traffic rule), optionally
// with a bearer token, marshalling body (if non-nil) and unmarshalling the
// response into out (if non-nil and the body is non-empty). It returns the
// HTTP status code and fails the test via t.Fatalf on any transport/encoding
// error -- callers assert on the status code themselves, since a non-2xx
// response is often the scenario under test.
func DoJSON(t *testing.T, ctx context.Context, method, path, bearerToken string, body, out any) int {
	t.Helper()

	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body for %s %s: %v", method, path, err)
		}
		reader = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, GatewayURL()+path, reader)
	if err != nil {
		t.Fatalf("build request %s %s: %v", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s: read response body: %v", method, path, err)
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("%s %s: unmarshal response %q: %v", method, path, string(raw), err)
		}
	}
	return resp.StatusCode
}

type registerRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type registerResponse struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

type loginResponse struct {
	Token string `json:"token"`
}

// RegisterAndLogin mints a fresh, unique user through user-service's public
// API (register then login, both through Kong) and returns its id and a
// bearer JWT. Every call produces new, never-before-seen data, so tests never
// need a shared fixture user or any cleanup.
func RegisterAndLogin(t *testing.T, ctx context.Context) (uuid.UUID, string) {
	t.Helper()

	email := fmt.Sprintf("e2e-%s-%d@example.com", sanitizeForEmail(t.Name()), time.Now().UnixNano())
	const password = "correct-horse-battery-staple"

	var reg registerResponse
	status := DoJSON(t, ctx, http.MethodPost, "/api/v1/auth/register", "", registerRequest{Email: email, Password: password}, &reg)
	if status != http.StatusCreated {
		t.Fatalf("register %s: expected 201, got %d", email, status)
	}

	var login loginResponse
	status = DoJSON(t, ctx, http.MethodPost, "/api/v1/auth/login", "", registerRequest{Email: email, Password: password}, &login)
	if status != http.StatusOK {
		t.Fatalf("login %s: expected 200, got %d", email, status)
	}

	userID, err := uuid.Parse(reg.ID)
	if err != nil {
		t.Fatalf("parse registered user id %q: %v", reg.ID, err)
	}
	return userID, login.Token
}

func sanitizeForEmail(s string) string {
	replacer := strings.NewReplacer("/", "-", " ", "-")
	return strings.ToLower(replacer.Replace(s))
}
