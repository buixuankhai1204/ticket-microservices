//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/repository/postgres"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
)

func newRepo() *postgres.Repository {
	return postgres.New()
}

func countRows(t *testing.T, ctx context.Context, query string, args ...any) int {
	t.Helper()
	var n int
	if err := testPool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

func seedBookingOutcome(t *testing.T, ctx context.Context, eventID uuid.UUID, status string) {
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

func seedUserRegistrationRow(t *testing.T, ctx context.Context, reg domain.UserRegistration) {
	t.Helper()
	_, err := testPool.Exec(ctx,
		`INSERT INTO user_registrations (user_id, email, registered_at, recorded_at)
		 VALUES ($1, $2, $3, $4)`,
		reg.UserID, reg.Email, reg.RegisteredAt, reg.RecordedAt,
	)
	if err != nil {
		t.Fatalf("seed user_registrations: %v", err)
	}
}

func queryBookingOutcome(t *testing.T, ctx context.Context, bookingID uuid.UUID) (domain.BookingOutcome, bool) {
	t.Helper()
	var o domain.BookingOutcome
	err := testPool.QueryRow(ctx,
		`SELECT id, booking_id, event_id, status, occurred_at, recorded_at
		   FROM booking_outcomes WHERE booking_id = $1`, bookingID,
	).Scan(&o.ID, &o.BookingID, &o.EventID, &o.Status, &o.OccurredAt, &o.RecordedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.BookingOutcome{}, false
	}
	if err != nil {
		t.Fatalf("query booking_outcome %s: %v", bookingID, err)
	}
	return o, true
}

func queryUserRegistration(t *testing.T, ctx context.Context, userID uuid.UUID) (domain.UserRegistration, bool) {
	t.Helper()
	var r domain.UserRegistration
	err := testPool.QueryRow(ctx,
		`SELECT user_id, email, registered_at, recorded_at
		   FROM user_registrations WHERE user_id = $1`, userID,
	).Scan(&r.UserID, &r.Email, &r.RegisteredAt, &r.RecordedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.UserRegistration{}, false
	}
	if err != nil {
		t.Fatalf("query user_registration %s: %v", userID, err)
	}
	return r, true
}

func queryUserLogin(t *testing.T, ctx context.Context, eventID uuid.UUID) (domain.UserLogin, bool) {
	t.Helper()
	var l domain.UserLogin
	err := testPool.QueryRow(ctx,
		`SELECT user_id, email, logged_in_at, recorded_at
		   FROM user_logins WHERE event_id = $1`, eventID,
	).Scan(&l.UserID, &l.Email, &l.LoggedInAt, &l.RecordedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.UserLogin{}, false
	}
	if err != nil {
		t.Fatalf("query user_login %s: %v", eventID, err)
	}
	return l, true
}
