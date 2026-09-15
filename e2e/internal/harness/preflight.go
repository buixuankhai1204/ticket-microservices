//go:build e2e

package harness

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// Preflight checks every participant service's /healthz, the Kafka Connect
// REST API, and the gateway itself (via a route that needs no auth), exactly
// as e2e-saga-tester would before driving a saga by hand. TestMain calls this
// once; a non-nil error means every test should skip, not fail.
func Preflight(ctx context.Context) error {
	checks := []struct{ name, url string }{
		{"user-service", UserServiceHealthzURL()},
		{"event-service", EventServiceHealthzURL()},
		{"booking-service", BookingServiceHealthzURL()},
		{"analytics-service", AnalyticsServiceHealthzURL()},
		{"kafka-connect", KafkaConnectURL()},
		{"kong-gateway", GatewayURL() + "/api/v1/events"},
	}

	client := &http.Client{Timeout: 5 * time.Second}
	for _, c := range checks {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
		if err != nil {
			return fmt.Errorf("%s: build request: %w", c.name, err)
		}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("%s unreachable at %s: %w", c.name, c.url, err)
		}
		resp.Body.Close()
		if resp.StatusCode >= 500 {
			return fmt.Errorf("%s at %s returned %d", c.name, c.url, resp.StatusCode)
		}
	}
	return nil
}
