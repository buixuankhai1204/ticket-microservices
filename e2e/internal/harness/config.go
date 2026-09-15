//go:build e2e

// Package harness holds transport-agnostic helpers shared by every saga's e2e
// test file: Kong-facing HTTP, one *pgxpool.Pool per participant DB, and
// segmentio/kafka-go helpers for the DLQ/idempotency/poison-message checks
// that can only be done by talking to Kafka directly. All application traffic
// (booking/event/user actions) must still go through Kong -- only assertions
// go straight to Postgres/Kafka, per docs/development-workflow.md and
// CLAUDE.md's choreography-saga section.
//
// Every default below points at the dedicated "ticket-e2e" docker-compose
// project (.env.e2e / scripts/run-e2e.sh), NOT the normal dev stack -- on
// purpose. This suite writes real data through real code paths, and the dev
// stack's Postgres is not a disposable test database. Every port here is the
// dev stack's port + 10000, matching .env.e2e, so `go test -C e2e -tags=e2e
// -count=1 ./...` is safe to run directly, without scripts/run-e2e.sh, and
// still never touches dev data -- the safety doesn't depend on remembering to
// use the wrapper.
package harness

import (
	"os"
	"strings"
)

func envOrDefault(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// GatewayURL is Kong's proxy listener -- every saga action goes through here,
// never a service's own host-mapped port. Defaults to the dedicated e2e
// stack's Kong (.env.e2e: KONG_PROXY_HOST_PORT=18000), not the dev stack's
// :8000.
func GatewayURL() string {
	return strings.TrimRight(envOrDefault("GATEWAY_URL", "http://localhost:18000"), "/")
}

// KafkaBrokers returns the broker list the e2e module should dial directly
// (segmentio/kafka-go, for DLQ/idempotency/poison checks only -- never for
// application traffic). Defaults to the dedicated e2e stack's EXTERNAL
// listener (.env.e2e: KAFKA_EXTERNAL_HOST_PORT=19094) -- the dev stack's
// kafka has no such listener by design, so pointing here by default is also
// what keeps this reachable out of the box instead of silently skipping.
func KafkaBrokers() []string {
	raw := envOrDefault("KAFKA_BROKERS", "localhost:19094")
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// BookingDatabaseURL, EventDatabaseURL, AnalyticsDatabaseURL and
// UserDatabaseURL default to the dedicated e2e stack's host port mappings
// (.env.e2e: postgres-booking:15436, postgres-event:15435,
// postgres-analytics:15434, postgres-user:15433 -- each dev port + 10000)
// with the default docker-compose credentials, which are the same in both
// stacks.
func BookingDatabaseURL() string {
	return envOrDefault("BOOKING_DATABASE_URL", "postgres://booking_service:booking_service@localhost:15436/booking_service?sslmode=disable")
}

func EventDatabaseURL() string {
	return envOrDefault("EVENT_DATABASE_URL", "postgres://event_service:event_service@localhost:15435/event_service?sslmode=disable")
}

func AnalyticsDatabaseURL() string {
	return envOrDefault("ANALYTICS_DATABASE_URL", "postgres://analytics_service:analytics_service@localhost:15434/analytics_service?sslmode=disable")
}

func UserDatabaseURL() string {
	return envOrDefault("USER_DATABASE_URL", "postgres://user_service:user_service@localhost:15433/user_service?sslmode=disable")
}

// Service healthz URLs are hit directly (Kong has no route for /healthz --
// kong/kong.yml only fronts /api/v1/*), matching the dedicated e2e stack's
// host-mapped ports in .env.e2e (each dev port + 10000).
func UserServiceHealthzURL() string {
	return envOrDefault("USER_SERVICE_HEALTHZ_URL", "http://localhost:18081/healthz")
}

func EventServiceHealthzURL() string {
	return envOrDefault("EVENT_SERVICE_HEALTHZ_URL", "http://localhost:18082/healthz")
}

func AnalyticsServiceHealthzURL() string {
	return envOrDefault("ANALYTICS_SERVICE_HEALTHZ_URL", "http://localhost:18084/healthz")
}

func BookingServiceHealthzURL() string {
	return envOrDefault("BOOKING_SERVICE_HEALTHZ_URL", "http://localhost:18085/healthz")
}

// KafkaConnectURL is the Kafka Connect REST API -- unlike the broker port,
// this one is host-mapped on the dev stack too, but defaults here to the
// dedicated e2e stack's port (.env.e2e: KAFKA_CONNECT_HOST_PORT=18083) for
// the same reason as everything else in this file.
func KafkaConnectURL() string {
	return envOrDefault("KAFKA_CONNECT_URL", "http://localhost:18083/connectors")
}

// BookingEventsTopic and SeatReservationEventsTopic mirror the
// KAFKA_BOOKING_EVENTS_TOPIC / KAFKA_SEAT_RESERVATION_EVENTS_TOPIC env vars
// every participant service reads (docs/sagas/seat-reservation.md §9).
func BookingEventsTopic() string {
	return envOrDefault("KAFKA_BOOKING_EVENTS_TOPIC", "booking.events")
}

func SeatReservationEventsTopic() string {
	return envOrDefault("KAFKA_SEAT_RESERVATION_EVENTS_TOPIC", "seat_reservation.events")
}

// DLQTopic is the dead-letter topic naming convention used everywhere in this
// repo: <topic>.dlq.
func DLQTopic(topic string) string {
	return topic + ".dlq"
}
